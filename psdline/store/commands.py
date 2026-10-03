"""门控指令队列与 v1/v2 格式并存迁移。

批量开门与回绕处理要求"同一列车的门控指令按提交顺序执行、发车后清空"。指令
队列按 ``train_id`` 分文档存放，每条指令带 ``epoch``（提交时的代际），读取时
原样返回，供上位机核对有没有被重放。

v1 文档是一个顶层数组；v2 文档是带版本号与校验信息的信封，信封内才是有序指令
数组。迁移期读取端同时接受两种文档，写入端按每列车的迁移状态选择格式。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..errors import StorageError
from .jsonfile import JsonStore
from .locks import TrainLock

COMMAND_FORMAT_V1 = 1
COMMAND_FORMAT_V2 = 2


@dataclass(frozen=True)
class DoorCommand:
    """一条待执行的门控指令。"""

    id: str
    train_id: str
    kind: str
    epoch: int
    station_id: str = ""
    schema_version: int = COMMAND_FORMAT_V1
    sequence: int = 0
    issued_at: float | None = None
    idempotency_key: str = ""
    extra: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict:
        if self.schema_version < COMMAND_FORMAT_V2:
            return {
                "id": self.id,
                "train_id": self.train_id,
                "kind": self.kind,
                "epoch": self.epoch,
                "station_id": self.station_id,
            }
        payload = {
            "id": self.id,
            "train_id": self.train_id,
            "kind": self.kind,
            "epoch": self.epoch,
            "station_id": self.station_id,
            "schema_version": COMMAND_FORMAT_V2,
            "sequence": self.sequence,
            "issued_at": self.issued_at,
            "idempotency_key": self.idempotency_key or self.id,
        }
        if self.extra:
            payload["extra"] = dict(self.extra)
        return payload

    @classmethod
    def from_dict(cls, payload: dict) -> "DoorCommand":
        schema_version = int(payload.get("schema_version", COMMAND_FORMAT_V1))
        extra_raw = payload.get("extra", {})
        issued_at = payload.get("issued_at")
        return cls(
            id=str(payload.get("id", "")),
            train_id=str(payload.get("train_id", "")),
            kind=str(payload.get("kind", "")),
            epoch=int(payload.get("epoch", 0)),
            station_id=str(payload.get("station_id", "")),
            schema_version=max(COMMAND_FORMAT_V1, schema_version),
            sequence=int(payload.get("sequence", 0)),
            issued_at=float(issued_at) if issued_at is not None else None,
            idempotency_key=str(payload.get("idempotency_key", "")),
            extra=dict(extra_raw) if isinstance(extra_raw, dict) else {},
        )


def envelope_v2(commands: list[DoorCommand], *, epoch: int) -> dict:
    """构造 v2 指令队列信封。"""

    return {
        "schema_version": COMMAND_FORMAT_V2,
        "kind": "door_commands",
        "epoch": epoch,
        "count": len(commands),
        "commands": [item.as_dict() for item in commands],
    }


def parse_command_document(raw: Any) -> tuple[list[DoorCommand], int, int]:
    """解析 v1 数组或 v2 信封，返回指令、格式版本和队列代际。"""

    if isinstance(raw, list):
        commands = [DoorCommand.from_dict(item) for item in raw if isinstance(item, dict)]
        epoch = max((item.epoch for item in commands), default=0)
        return commands, COMMAND_FORMAT_V1, epoch
    if isinstance(raw, dict) and int(raw.get("schema_version", 0)) == COMMAND_FORMAT_V2:
        items_raw = raw.get("commands", [])
        if not isinstance(items_raw, list):
            raise StorageError("v2 门控指令文档的 commands 必须是数组", component="store")
        commands = [DoorCommand.from_dict(item) for item in items_raw if isinstance(item, dict)]
        if int(raw.get("count", len(commands))) != len(commands):
            raise StorageError("v2 门控指令文档的 count 与实际条数不一致", component="store")
        seen: set[str] = set()
        for item in commands:
            key = item.idempotency_key or item.id
            if key in seen:
                raise StorageError(f"门控指令重复: {key}", component="store")
            seen.add(key)
        return commands, COMMAND_FORMAT_V2, int(raw.get("epoch", 0))
    raise StorageError("无法识别的门控指令文档格式", component="store")


class CommandLog:
    """按列车保存的门控指令队列，读取兼容 v1/v2。"""

    def __init__(self, store: JsonStore) -> None:
        self._store = store
        self._epoch = self._load_epoch()

    @property
    def epoch(self) -> int:
        """返回当前代际，每次清空队列后递增。"""

        return self._epoch

    def _doc_id(self, train_id: str) -> str:
        return f"commands/{train_id}"

    def _state_doc_id(self, train_id: str) -> str:
        return f"migration/commands/{train_id}/state"

    def _epoch_doc_id(self) -> str:
        return "commands/epoch"

    def _load_epoch(self) -> int:
        values: list[int] = []
        raw_epoch = self._store.read(self._epoch_doc_id())
        if isinstance(raw_epoch, dict):
            values.append(int(raw_epoch.get("epoch", 0)))
        for doc_id in self._store.list_ids():
            if not doc_id.startswith("commands/") or doc_id == "commands/epoch":
                continue
            raw = self._store.read(doc_id)
            try:
                _commands, _version, epoch = parse_command_document(raw)
            except StorageError:
                continue
            values.append(epoch)
        return max(values, default=0)

    def _persist_epoch(self) -> None:
        self._store.write(self._epoch_doc_id(), {"epoch": self._epoch})

    def _format_for_append(self, train_id: str) -> int:
        state = self._store.read(self._state_doc_id(train_id))
        if isinstance(state, dict):
            return int(state.get("format", COMMAND_FORMAT_V1))
        manifest = self._store.read("migration/commands/manifest")
        if isinstance(manifest, dict) and str(manifest.get("phase")) == "finished":
            return COMMAND_FORMAT_V2
        return COMMAND_FORMAT_V1

    def _upgrade_command(
        self, command: DoorCommand, format_version: int, sequence: int
    ) -> DoorCommand:
        if format_version < COMMAND_FORMAT_V2:
            return command
        return DoorCommand(
            id=command.id,
            train_id=command.train_id,
            kind=command.kind,
            epoch=command.epoch,
            station_id=command.station_id,
            schema_version=COMMAND_FORMAT_V2,
            sequence=sequence,
            issued_at=command.issued_at,
            idempotency_key=command.idempotency_key or command.id,
            extra=dict(command.extra),
        )

    def append(self, command: DoorCommand) -> None:
        """把一条指令追加到列车队列尾部。"""

        with TrainLock(self._store.root, command.train_id):
            queued, _document_version, _epoch = self.pending_with_version(command.train_id)
            target_version = self._format_for_append(command.train_id)
            if target_version >= COMMAND_FORMAT_V2:
                if not isinstance(
                    self._store.read(self._state_doc_id(command.train_id)), dict
                ):
                    self._store.write(
                        self._state_doc_id(command.train_id),
                        {
                            "train_id": command.train_id,
                            "state": "v2",
                            "format": COMMAND_FORMAT_V2,
                        },
                    )
                key = command.idempotency_key or command.id
                if any((item.idempotency_key or item.id) == key for item in queued):
                    raise StorageError(f"门控指令重复: {key}", component="store")
            queued.append(command)
            if target_version >= COMMAND_FORMAT_V2:
                normalized = [
                    self._upgrade_command(item, COMMAND_FORMAT_V2, index + 1)
                    for index, item in enumerate(queued)
                ]
                self._store.write(
                    self._doc_id(command.train_id),
                    envelope_v2(normalized, epoch=max(self._epoch, command.epoch)),
                )
                return
            self._store.write(self._doc_id(command.train_id), [item.as_dict() for item in queued])

    def pending_with_version(self, train_id: str) -> tuple[list[DoorCommand], int, int]:
        """返回队列、实际存储格式和文档队列代际。"""

        raw = self._store.read(self._doc_id(train_id))
        if raw is None:
            return [], self._format_for_append(train_id), self._epoch
        return parse_command_document(raw)

    def pending(self, train_id: str) -> list[DoorCommand]:
        """返回列车当前排队中的指令（按提交顺序）。"""

        return self.pending_with_version(train_id)[0]

    def format_version(self, train_id: str) -> int:
        """返回列车当前队列车文档的实际格式；无队列时返回目标格式。"""

        return self.pending_with_version(train_id)[1]

    def ids(self, train_id: str) -> list[str]:
        """返回排队指令的标识列表。"""

        return [command.id for command in self.pending(train_id)]

    def clear(self, train_id: str, *, format_version: int | None = None) -> int:
        """清空列车队列并推进代际，返回被清掉的指令条数。"""

        with TrainLock(self._store.root, train_id):
            removed = len(self.pending(train_id))
            self._epoch = max(self._epoch + 1, self._load_epoch() + 1)
            self._persist_epoch()
            target_version = (
                self._format_for_append(train_id) if format_version is None else format_version
            )
            if target_version >= COMMAND_FORMAT_V2:
                self._store.write(self._doc_id(train_id), envelope_v2([], epoch=self._epoch))
            else:
                self._store.remove(self._doc_id(train_id))
            return removed

    def clear_v1(self, train_id: str) -> int:
        """按 v1 语义删除活动队列，但保留迁移状态后续生成 v2 空信封。"""

        with TrainLock(self._store.root, train_id):
            removed = len(self.pending(train_id))
            self._epoch = max(self._epoch + 1, self._load_epoch() + 1)
            self._persist_epoch()
            self._store.remove(self._doc_id(train_id))
            return removed

    def total_queued(self) -> int:
        """统计全部列车排队中的指令条数。"""

        total = 0
        for doc_id in self._store.list_ids():
            if not doc_id.startswith("commands/") or doc_id == "commands/epoch":
                continue
            raw = self._store.read(doc_id)
            try:
                commands, _version, _epoch = parse_command_document(raw)
            except StorageError:
                continue
            total += len(commands)
        return total
