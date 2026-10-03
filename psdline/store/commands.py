"""门控指令队列（v1／v2 双格式）。

批量开门与回绕处理要求"同一列车的门控指令按提交顺序执行、发车后清空"。指令
队列按 ``train_id`` 分文档存放，每条指令带 ``epoch``（提交时的代际），读取时
原样返回，供上位机核对有没有被重放。

格式升级期间新旧并存：

* v1（``commands/<train>``）：裸列表，元素只有
  ``id/train_id/kind/epoch/station_id``；
* v2（``commands_v2/<train>``）：信封文档，元素是超集，额外带
  ``ver/seq/issued_at/attrs``，并对每条指令和整份队列各算一个 SHA-256 摘要。

写入路由由 :class:`~psdline.store.migration.MigrationCoordinator` 的阶段与
逐车迁移状态决定；读取始终兼容两种格式并按提交序号合并。迁移状态文档同时承载
``epoch``，修掉旧实现"重启后 epoch 内存归零"的问题。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

from .jsonfile import JsonStore

V1 = 1
V2 = 2

V1_KINDS = ("open", "close")


def canonical_json(payload: Any) -> bytes:
    """迁移摘要使用的规范化 JSON（键排序、紧凑分隔、UTF-8、原样中文）。"""

    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )


def sha256_of(payload: Any) -> str:
    """对规范化后的载荷计算 SHA-256。"""

    return hashlib.sha256(canonical_json(payload)).hexdigest()


@dataclass(frozen=True)
class DoorCommand:
    """一条门控指令。

    前五个字段是 v1 原样字段，构造签名保持不变（旧调用点零改动）；``ver``
    之后的是 v2 扩展，默认构造出 v1 指令，显式 ``ver=2`` 才是新格式。
    """

    id: str
    train_id: str
    kind: str
    epoch: int
    station_id: str = ""
    ver: int = V1
    seq: int = 0
    issued_at: float = 0.0
    attrs: dict = field(default_factory=dict)

    @property
    def is_v2(self) -> bool:
        return self.ver >= V2

    def _base(self) -> dict:
        return {
            "id": self.id,
            "train_id": self.train_id,
            "kind": self.kind,
            "epoch": self.epoch,
            "station_id": self.station_id,
        }

    def as_dict(self) -> dict:
        """按自身版本落盘；v1 指令只吐 v1 字段，历史形态不受升级影响。"""

        payload = self._base()
        if not self.is_v2:
            return payload
        payload.update(
            {
                "ver": V2,
                "seq": self.seq,
                "issued_at": self.issued_at,
                "attrs": dict(self.attrs),
                "digest": self.digest(),
            }
        )
        return payload

    def digest(self) -> str:
        """该条指令（不含 ``digest`` 自身）的规范化摘要。"""

        body = self._base()
        if self.is_v2:
            body.update(
                {
                    "ver": V2,
                    "seq": self.seq,
                    "issued_at": self.issued_at,
                    "attrs": dict(self.attrs),
                }
            )
        return sha256_of(body)

    @classmethod
    def from_dict(cls, payload: dict) -> "DoorCommand":
        """从 v1 或 v2 形态解析一条指令（旧上位机两种都得能读）。"""

        command = cls(
            id=str(payload.get("id", "")),
            train_id=str(payload.get("train_id", "")),
            kind=str(payload.get("kind", "")),
            epoch=int(payload.get("epoch", 0)),
            station_id=str(payload.get("station_id", "")),
            ver=int(payload.get("ver", V1) or V1),
            seq=int(payload.get("seq", 0) or 0),
            issued_at=float(payload.get("issued_at", 0.0) or 0.0),
            attrs=dict(payload.get("attrs")) if isinstance(payload.get("attrs"), dict) else {},
        )
        return command


@dataclass(frozen=True)
class QueueDocument:
    """一份 v2 队列信封：列车标识、格式版本、有序指令与整队摘要。"""

    train_id: str
    commands: tuple[DoorCommand, ...]
    ver: int = V2

    def as_envelope(self) -> dict:
        items = [command.as_dict() for command in self.commands]
        body = {"train_id": self.train_id, "ver": V2, "commands": items}
        return {**body, "digest": sha256_of(body)}


class CommandLog:
    """按列车保存的门控指令队列，读写都感知迁移阶段。"""

    def __init__(
        self,
        store: JsonStore,
        migration: Any | None = None,
        clock: Any | None = None,
    ) -> None:
        self._store = store
        if migration is None:
            # 延迟导入：migration 反向依赖本模块的命令模型，不能在模块顶层成环。
            from .migration import MigrationCoordinator

            migration = MigrationCoordinator(store, clock=clock)
        self._migration = migration
        self._clock = clock

    @property
    def migration(self) -> MigrationCoordinator:
        return self._migration

    @property
    def epoch(self) -> int:
        """当前代际（持久化在迁移状态文档里，每次清空队列后递增）。"""

        return self._migration.epoch

    # ---- 文档标识 -----------------------------------------------------

    def _doc_id(self, train_id: str) -> str:
        return f"commands/{train_id}"

    def _v2_doc_id(self, train_id: str) -> str:
        return f"commands_v2/{train_id}"

    def legacy_train_ids(self) -> list[str]:
        """全部仍有 v1 活动文档的列车（归档目录不在扫描范围）。"""

        prefix = "commands/"
        return [
            doc_id[len(prefix) :]
            for doc_id in self._store.list_ids()
            if doc_id.startswith(prefix)
        ]

    def v2_train_ids(self) -> list[str]:
        """全部有 v2 活动文档的列车。"""

        prefix = "commands_v2/"
        return [
            doc_id[len(prefix) :]
            for doc_id in self._store.list_ids()
            if doc_id.startswith(prefix)
        ]

    # ---- 读取（两种格式都能读）---------------------------------------

    def _read_legacy(self, train_id: str) -> list[DoorCommand]:
        raw = self._store.read(self._doc_id(train_id))
        if not isinstance(raw, list):
            return []
        return [DoorCommand.from_dict(item) for item in raw if isinstance(item, dict)]

    def _read_v2(self, train_id: str) -> list[DoorCommand]:
        raw = self._store.read(self._v2_doc_id(train_id))
        if not isinstance(raw, dict) or not isinstance(raw.get("commands"), list):
            return []
        return [DoorCommand.from_dict(item) for item in raw["commands"] if isinstance(item, dict)]

    def pending(self, train_id: str) -> list[DoorCommand]:
        """返回列车当前排队中的指令（按提交顺序），新旧格式合并去重。

        * legacy／done 阶段只看一种文档；
        * dual 阶段：已迁移车只看 v2（v1 已归档不可变），未迁移车读 v1；
          若 v1 在迁移落档后又冒出"漏网指令"（理论上只在崩溃恢复竞态时
          出现），按 id／seq 去重后追加到队尾，保证不丢指令。
        """

        with self._migration.lock:
            phase = self._migration.phase
            if phase == "legacy":
                return self._read_legacy(train_id)
            if phase == "done":
                return self._read_v2(train_id)
            state = self._migration.train_state(train_id)
            if state == "migrated":
                v2 = self._read_v2(train_id)
                legacy = self._read_legacy(train_id)
                if legacy:
                    known = {command.id for command in v2}
                    max_seq = max((command.seq for command in v2), default=0)
                    for command in legacy:
                        if command.id in known:
                            continue
                        max_seq += 1
                        v2.append(self._as_v2(command, seq=max_seq))
                v2.sort(key=lambda command: command.seq)
                return v2
            return self._read_legacy(train_id)

    def ids(self, train_id: str) -> list[str]:
        """返回排队指令的标识列表。"""

        return [command.id for command in self.pending(train_id)]

    # ---- 写入（按迁移状态路由）---------------------------------------

    def _as_v2(
        self,
        command: DoorCommand,
        *,
        seq: int,
        issued_at: float | None = None,
    ) -> DoorCommand:
        if issued_at is None:
            issued_at = command.issued_at
        if not issued_at and self._clock is not None:
            issued_at = self._clock.now()
        return DoorCommand(
            id=command.id,
            train_id=command.train_id,
            kind=command.kind,
            epoch=command.epoch,
            station_id=command.station_id,
            ver=V2,
            seq=seq,
            issued_at=issued_at,
            attrs=dict(command.attrs),
        )

    def _append_legacy(self, command: DoorCommand) -> None:
        queued = self._read_legacy(command.train_id)
        queued.append(command)
        self._store.write(self._doc_id(command.train_id), [item.as_dict() for item in queued])

    def _append_v2(self, command: DoorCommand) -> None:
        train_id = command.train_id
        queued = self._read_v2(train_id)
        if any(item.id == command.id for item in queued):
            # 崩溃恢复后的重放：同一条指令不重复入队。
            return
        seq = max((item.seq for item in queued), default=0) + 1
        if not command.is_v2:
            command = self._as_v2(command, seq=seq)
        elif command.seq <= 0:
            command = self._as_v2(command, seq=seq)
        queued.append(command)
        queued.sort(key=lambda item: item.seq)
        envelope = QueueDocument(train_id, tuple(queued)).as_envelope()
        self._store.write(self._v2_doc_id(train_id), envelope)

    def append(self, command: DoorCommand) -> None:
        """把一条指令追加到列车队列尾部；目标格式由迁移状态决定。"""

        with self._migration.lock:
            train_id = command.train_id
            if self._migration.phase == "legacy":
                self._append_legacy(command)
                return
            if self._migration.phase == "done":
                self._append_v2(command)
                return
            # dual：已迁移走 v2；未迁移的车第一次入队即把整车惰性切到 v2。
            state = self._migration.train_state(train_id)
            if state == "legacy":
                self._migration.migrate_train(train_id)
            self._append_v2(command)

    def submit(
        self,
        train_id: str,
        command_id: str,
        kind: str,
        station_id: str = "",
        *,
        at: float | None = None,
        attrs: dict | None = None,
    ) -> DoorCommand:
        """按当前阶段构造并落盘一条指令，返回落盘后的实际形态。

        legacy 阶段发 v1 形态；dual／done 阶段发 v2 形态，``seq`` 与
        ``issued_at`` 由队列补齐。迁移期间两个上位机版本都可能提交，入口在
        这里收敛，保证同车指令不会跨格式乱序。
        """

        moment = at if at is not None else (
            self._clock.now() if self._clock is not None else 0.0
        )
        with self._migration.lock:
            if self._migration.phase == "legacy":
                command = DoorCommand(
                    id=command_id,
                    train_id=train_id,
                    kind=kind,
                    epoch=self.epoch,
                    station_id=station_id,
                )
                self._append_legacy(command)
                return command
            if self._migration.phase == "dual" and self._migration.train_state(train_id) == "legacy":
                self._migration.migrate_train(train_id)
            command = DoorCommand(
                id=command_id,
                train_id=train_id,
                kind=kind,
                epoch=self.epoch,
                station_id=station_id,
                ver=V2,
                issued_at=moment,
                attrs=dict(attrs or {}),
            )
            self._append_v2(command)
            return self._read_v2(train_id)[-1]

    def clear(self, train_id: str) -> int:
        """清空列车活动队列并推进代际，返回被清掉的指令条数。

        归档文档（``commands_archive/...``）从不动：迁完的车发车，清的是
        v2 活动文档；迁移中途的车发车，v1 活动文档和 v2 文档一起清。
        """

        with self._migration.lock:
            removed = len(self.pending(train_id))
            self._store.remove(self._doc_id(train_id))
            self._store.remove(self._v2_doc_id(train_id))
            self._migration.advance_epoch()
            return removed

    def total_queued(self) -> int:
        """统计全部列车排队中的指令条数（两种格式合并、同车不重复计数）。"""

        train_ids = set(self.legacy_train_ids()) | set(self.v2_train_ids())
        return sum(len(self.pending(train_id)) for train_id in train_ids)
