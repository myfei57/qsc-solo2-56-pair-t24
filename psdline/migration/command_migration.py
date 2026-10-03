"""门控指令 v1 → v2 的在线列车级迁移。

迁移以列车为边界：每列车先取得短期租约，然后按"归档旧文档 → 写 v2 文档 →
标记完成"的顺序提交。旧文档写入不可变归档后才从活动路径删除，因此任意一步
崩溃都可以从状态文档恢复，不会重复执行旧指令。

在办指令默认等执行完再迁：列车队列非空时迁移停在 ``waiting_boundary``，发车
清队后的下一次重试自动完成。需要在安全边界前转换时，可显式传
``allow_queued=True``，转换结果仍按原顺序保留。
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from typing import Any, Callable

from ..errors import StorageError
from ..store import JsonStore, TrainLock
from ..store.commands import (
    COMMAND_FORMAT_V1,
    COMMAND_FORMAT_V2,
    DoorCommand,
    envelope_v2,
    parse_command_document,
)

STATUS_RUNNING = "running"
STATUS_FINISHED = "finished"
STATE_WAITING = "waiting_boundary"
STATE_MIGRATING = "migrating"
STATE_V2 = "v2"
DEFAULT_LEASE_SECONDS = 30.0
MANIFEST_DOC = "migration/commands/manifest"


class CommandMigrator:
    """v1/v2 门控指令并存期的迁移协调器。"""

    def __init__(
        self,
        store: JsonStore,
        *,
        clock: Callable[[], float] | None = None,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
        audit: Any | None = None,
        owner: str = "controller",
    ) -> None:
        self._store = store
        self._clock = clock or time.time
        self._lease_seconds = lease_seconds
        self._audit = audit
        self._owner = owner
        self._lock = threading.RLock()

    # ---- 状态 ---------------------------------------------------------

    def manifest(self) -> dict | None:
        raw = self._store.read(MANIFEST_DOC)
        return raw if isinstance(raw, dict) else None

    def status(self, train_ids: list[str] | None = None) -> dict:
        """返回全局、按列车与活动 v1 文档的迁移进度。"""

        known = self._known_train_ids()
        if train_ids is not None:
            known = sorted(set(known) | {str(item) for item in train_ids})
        trains: dict[str, dict] = {}
        legacy_documents: list[str] = []
        for train_id in known:
            detail = self.train_status(train_id)
            trains[train_id] = detail
            if detail["active_format"] == COMMAND_FORMAT_V1:
                legacy_documents.append(f"commands/{train_id}")
        manifest = self.manifest() or {}
        migrated = sum(1 for item in trains.values() if item["state"] == STATE_V2)
        pending_trains = [
            train_id
            for train_id, item in trains.items()
            if item["state"] != STATE_V2
        ]
        return {
            "phase": str(manifest.get("phase", "not_started")),
            "total": len(trains),
            "migrated": migrated,
            "pending": len(pending_trains),
            "queued_commands": sum(int(item["pending"]) for item in trains.values()),
            "waiting_boundary": [
                train_id
                for train_id, item in trains.items()
                if item["state"] == STATE_WAITING and int(item["pending"]) > 0
            ],
            "legacy_documents": sorted(legacy_documents),
            "trains": trains,
        }

    def train_status(self, train_id: str) -> dict:
        state = self._state(train_id)
        raw = self._store.read(self._command_doc(train_id))
        pending_count = 0
        active_format = 0
        migration_id = str(state.get("migration_id", ""))
        if raw is not None:
            commands, active_format, _epoch = parse_command_document(raw)
            pending_count = len(commands)
        elif state:
            active_format = COMMAND_FORMAT_V2
        else:
            active_format = COMMAND_FORMAT_V1
        return {
            "train_id": train_id,
            "state": str(state.get("state", "legacy")),
            "format": int(state.get("format", COMMAND_FORMAT_V1)),
            "active_format": active_format,
            "pending": pending_count,
            "migration_id": migration_id,
            "lease_owner": str(state.get("lease_owner", "")),
            "lease_expires_at": float(state.get("lease_expires_at", 0.0)),
            "archive_doc": str(state.get("archive_doc", "")),
            "source_hash": str(state.get("source_hash", "")),
        }

    def begin(self, train_ids: list[str]) -> dict:
        """开始一批列车的迁移；未启动前，新列车仍可按默认 v1 写入。"""

        with self._lock:
            if self.manifest() is not None:
                return self.manifest()  # type: ignore[return-value]
            now = self._clock()
            self._record(
                "command_migration_begin",
                "",
                "",
                at=now,
                trains=list(sorted({str(item) for item in train_ids})),
                entry_id="audit-command-migration-begin",
            )
            payload = {
                "phase": STATUS_RUNNING,
                "from_version": COMMAND_FORMAT_V1,
                "to_version": COMMAND_FORMAT_V2,
                "started_at": now,
                "updated_at": now,
                "trains": sorted({str(item) for item in train_ids}),
            }
            self._store.write(MANIFEST_DOC, payload)
            for train_id in payload["trains"]:
                if self._state(train_id):
                    continue
                self._store.write(
                    self._state_doc(train_id),
                    {
                        "train_id": train_id,
                        "state": STATE_WAITING,
                        "format": COMMAND_FORMAT_V1,
                        "migration_id": self._migration_id(train_id),
                        "epoch": 0,
                        "created_at": now,
                        "updated_at": now,
                    },
                )
            return payload

    def recover(self, train_ids: list[str] | None = None) -> list[dict]:
        """恢复启动前未完成的迁移租约；未崩溃的等待边界列车保持等待。"""

        results: list[dict] = []
        progress = self.status(train_ids)
        for train_id, detail in progress["trains"].items():
            if detail["state"] == STATE_MIGRATING:
                results.append(self.migrate_train(train_id, allow_queued=True))
            elif detail["state"] == STATE_WAITING and detail["pending"] == 0:
                results.append(self.migrate_train(train_id, allow_queued=False))
        return results

    def migrate_next(
        self,
        train_ids: list[str] | None = None,
        *,
        allow_queued: bool = False,
    ) -> dict | None:
        """找到下一列车可迁移的列车并迁移；无可做工作时返回 ``None``。"""

        progress = self.status(train_ids)
        waiting: list[dict] = []
        for train_id, detail in progress["trains"].items():
            if detail["state"] == STATE_V2:
                continue
            result = self.migrate_train(train_id, allow_queued=allow_queued)
            if result["state"] == STATE_WAITING:
                waiting.append(result)
                continue
            return result
        if waiting:
            return waiting[0]
        return None

    def finish(self) -> dict:
        """确认所有已知列车和活动 v1 文档均已迁出后关闭迁移窗口。"""

        with self._lock:
            progress = self.status()
            if progress["phase"] != STATUS_RUNNING:
                manifest = self.manifest()
                if manifest is None:
                    raise StorageError("门控指令迁移尚未开始", component="migration")
                return manifest
            if progress["legacy_documents"] or progress["pending"]:
                raise StorageError(
                    "仍有列车未完成门控指令格式迁移",
                    component="migration",
                )
            manifest = dict(self.manifest() or {})
            now = self._clock()
            self._record(
                "command_migration_finish",
                "",
                "",
                at=now,
                trains=progress["total"],
                entry_id="audit-command-migration-finish",
            )
            manifest.update(
                {
                    "phase": STATUS_FINISHED,
                    "finished_at": now,
                    "updated_at": now,
                }
            )
            self._store.write(MANIFEST_DOC, manifest)
            return manifest

    # ---- 单车迁移 -----------------------------------------------------

    def migrate_train(self, train_id: str, *, allow_queued: bool = False) -> dict:
        """迁移一列车；调用是幂等的，可在崩溃后重放。"""

        with self._lock, TrainLock(self._store.root, train_id):
            return self._migrate_train_locked(train_id, allow_queued=allow_queued)

    def _migrate_train_locked(self, train_id: str, *, allow_queued: bool) -> dict:
        now = self._clock()
        state = self._state(train_id)
        if not state:
            manifest = self.manifest()
            if not manifest or manifest.get("phase") != STATUS_RUNNING:
                return {
                    "train_id": train_id,
                    "state": "legacy",
                    "format": COMMAND_FORMAT_V1,
                    "active_format": self._active_format(train_id),
                    "pending": len(self._read_active_commands(train_id)[0]),
                    "skipped": "migration_not_started",
                }
            state = {
                "train_id": train_id,
                "state": STATE_WAITING,
                "format": COMMAND_FORMAT_V1,
                "migration_id": self._migration_id(train_id),
                "epoch": 0,
                "created_at": now,
                "updated_at": now,
            }
            self._store.write(self._state_doc(train_id), state)
        state = self._recover_or_start_state(train_id, state, now)
        if state["state"] == STATE_V2:
            return state

        commands, active_format, _epoch = self._read_active_commands(train_id)
        if commands and not allow_queued:
            waiting = self._touch_waiting(train_id, state)
            self._record(
                "command_migration_wait",
                train_id,
                commands[0].station_id if commands else "",
                at=now,
                migration_id=waiting["migration_id"],
                pending=len(commands),
            )
            return waiting
        raw = self._store.read(self._command_doc(train_id))
        if raw is None:
            epoch = self._current_epoch()
            self._store.write(self._command_doc(train_id), envelope_v2([], epoch=epoch))
            return self._mark_v2(
                train_id,
                {**state, "converted": 0, "epoch": epoch},
                now,
            )
        if active_format != COMMAND_FORMAT_V1:
            self._store.write(
                self._command_doc(train_id),
                envelope_v2(commands, epoch=max(int(state.get("epoch", 0)), _epoch)),
            )
            return self._mark_v2(train_id, state, now, recovered=True)

        raw_text = json.dumps(raw, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        source_hash = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()
        archive_doc = str(state.get("archive_doc", "")) or (
            f"migration/commands/{train_id}/archive/{state['migration_id']}"
        )
        self._write_immutable_archive(
            archive_doc,
            {
                "schema_version": COMMAND_FORMAT_V1,
                "kind": "archived_door_commands",
                "train_id": train_id,
                "migration_id": state["migration_id"],
                "source_doc": self._command_doc(train_id),
                "source_hash": source_hash,
                "archived_at": now,
                "payload": raw,
            },
        )
        self._store.write(
            self._state_doc(train_id),
            {
                **state,
                "archive_doc": archive_doc,
                "source_hash": source_hash,
                "converted": 0,
                "updated_at": now,
            },
        )
        converted = [
            DoorCommand(
                id=item.id,
                train_id=item.train_id,
                kind=item.kind,
                epoch=item.epoch,
                station_id=item.station_id,
                schema_version=COMMAND_FORMAT_V2,
                sequence=index,
                issued_at=item.issued_at,
                idempotency_key=item.idempotency_key or item.id,
                extra=dict(item.extra),
            )
            for index, item in enumerate(commands, start=1)
        ]
        target = envelope_v2(converted, epoch=int(state.get("epoch", 0)))
        self._store.write(self._command_doc(train_id), target)
        return self._mark_v2(
            train_id,
            {
                **state,
                "archive_doc": archive_doc,
                "source_hash": source_hash,
                "converted": len(converted),
            },
            now,
        )

    def wait_for_train(self, train_id: str) -> dict:
        """登记一列车为"等发车边界后迁移"，不阻断当前 v1 执行。"""

        with self._lock:
            state = self._state(train_id)
            if not state:
                now = self._clock()
                state = self._write_state(
                    train_id,
                    {
                        "train_id": train_id,
                        "state": STATE_WAITING,
                        "format": COMMAND_FORMAT_V1,
                        "migration_id": self._migration_id(train_id),
                        "epoch": 0,
                        "created_at": now,
                        "updated_at": now,
                    },
                )
            return state

    # ---- 内部 ---------------------------------------------------------

    def _recover_or_start_state(self, train_id: str, state: dict, now: float) -> dict:
        state = self._recover(train_id, state, now)
        if state["state"] == STATE_V2:
            return state
        return self._acquire_existing(train_id, state, now)

    def _recover(self, train_id: str, state: dict, now: float) -> dict:
        status = str(state.get("state"))
        if status == STATE_V2:
            return state
        if status not in {STATE_WAITING, STATE_MIGRATING}:
            raise StorageError(f"未知迁移状态: {status}", component="migration")
        if status == STATE_MIGRATING and float(state.get("lease_expires_at", 0.0)) <= now:
            self._reconcile_crash(train_id, state, now)
        return self._state(train_id)

    def _reconcile_crash(self, train_id: str, state: dict, now: float) -> None:
        raw = self._store.read(self._command_doc(train_id))
        archive_doc = str(state.get("archive_doc", ""))
        if raw is None:
            if not archive_doc or not self._store.exists(archive_doc):
                return
            archived = self._store.read(archive_doc)
            payload = archived.get("payload") if isinstance(archived, dict) else None
            _archived_commands, _archived_version, epoch = parse_command_document(payload)
            self._store.write(self._command_doc(train_id), envelope_v2([], epoch=epoch))
            self._mark_v2(train_id, state, now, recovered=True)
            return
        commands, version, _epoch = parse_command_document(raw)
        if version == COMMAND_FORMAT_V2:
            self._mark_v2(train_id, state, now, recovered=True)
            return
        # v1 仍在：无论归档是否存在，重放幂等转换；归档内容不变则不会新写。
        if archive_doc:
            self._assert_archive_matches(archive_doc, raw)

    def _acquire_existing(self, train_id: str, state: dict, now: float) -> dict:
        previous = str(state.get("state"))
        try:
            _commands, _version, epoch = self._read_active_commands(train_id)
        except StorageError:
            epoch = int(state.get("epoch", 0))
        updated = dict(state)
        updated.update(
            {
                "state": STATE_MIGRATING,
                "format": COMMAND_FORMAT_V2,
                "epoch": max(int(state.get("epoch", 0)), epoch),
                "lease_owner": self._owner,
                "lease_expires_at": now + self._lease_seconds,
                "updated_at": now,
            }
        )
        if previous == STATE_WAITING:
            self._record(
                "command_migration_start",
                train_id,
                "",
                at=now,
                migration_id=updated["migration_id"],
                entry_id=f"audit-{updated['migration_id']}-start",
            )
        self._store.write(self._state_doc(train_id), updated)
        return updated

    def _touch_waiting(self, train_id: str, state: dict) -> dict:
        now = self._clock()
        updated = dict(state)
        updated.update(
            {
                "state": STATE_WAITING,
                "format": COMMAND_FORMAT_V1,
                "lease_owner": "",
                "lease_expires_at": 0.0,
                "updated_at": now,
            }
        )
        self._store.write(self._state_doc(train_id), updated)
        return updated

    def _mark_v2(self, train_id: str, state: dict, now: float, *, recovered: bool = False) -> dict:
        updated = dict(state)
        updated.update(
            {
                "train_id": train_id,
                "state": STATE_V2,
                "format": COMMAND_FORMAT_V2,
                "lease_owner": "",
                "lease_expires_at": 0.0,
                "updated_at": now,
                "completed_at": state.get("completed_at", now),
            }
        )
        self._record(
            "command_migration_complete",
            train_id,
            "",
            at=now,
            migration_id=updated["migration_id"],
            archive_doc=updated.get("archive_doc", ""),
            converted=int(updated.get("converted", 0)),
            recovered=recovered,
            entry_id=f"audit-{updated['migration_id']}-complete",
        )
        self._store.write(self._state_doc(train_id), updated)
        self._update_manifest_train(train_id, now)
        return updated

    def _update_manifest_train(self, train_id: str, now: float) -> None:
        manifest = self.manifest()
        if not manifest:
            return
        trains = sorted(set(str(item) for item in manifest.get("trains", [])) | {train_id})
        updated = dict(manifest)
        updated.update({"trains": trains, "updated_at": now})
        self._store.write(MANIFEST_DOC, updated)

    def _read_active_commands(self, train_id: str) -> tuple[list[DoorCommand], int, int]:
        raw = self._store.read(self._command_doc(train_id))
        if raw is None:
            return [], COMMAND_FORMAT_V1, 0
        return parse_command_document(raw)

    def _active_format(self, train_id: str) -> int:
        raw = self._store.read(self._command_doc(train_id))
        if raw is None:
            return COMMAND_FORMAT_V1
        return parse_command_document(raw)[1]

    def _current_epoch(self) -> int:
        raw = self._store.read("commands/epoch")
        return int(raw.get("epoch", 0)) if isinstance(raw, dict) else 0

    def _write_immutable_archive(self, doc_id: str, payload: dict) -> None:
        payload = dict(payload)
        payload_without_hash = dict(payload)
        payload_without_hash.pop("sha256", None)
        canonical = json.dumps(
            payload_without_hash, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        payload["sha256"] = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        existing = self._store.read(doc_id)
        if existing is not None:
            if existing != payload:
                raise StorageError(f"不可变归档已存在且内容不同: {doc_id}", component="migration")
            return
        self._store.write(doc_id, payload)
        path = self._store.path_of(doc_id)
        try:
            os.chmod(path, 0o440)
        except OSError:
            # 某些部署卷不支持 chmod；审计与哈希仍提供不可变证据。
            pass

    def _assert_archive_matches(self, doc_id: str, raw: Any) -> None:
        archived = self._store.read(doc_id)
        if not isinstance(archived, dict) or archived.get("payload") != raw:
            raise StorageError(f"迁移归档与活动 v1 内容不一致: {doc_id}", component="migration")

    def _state(self, train_id: str) -> dict:
        raw = self._store.read(self._state_doc(train_id))
        if raw is None:
            return {}
        if not isinstance(raw, dict):
            raise StorageError(f"列车迁移状态格式错误: {train_id}", component="migration")
        return raw

    def _write_state(self, train_id: str, payload: dict) -> dict:
        self._store.write(self._state_doc(train_id), payload)
        return payload

    def _known_train_ids(self) -> list[str]:
        found: set[str] = set()
        prefix = "commands/"
        state_prefix = "migration/commands/"
        for doc_id in self._store.list_ids():
            if doc_id.startswith(prefix) and "/" in doc_id[len(prefix) :]:
                train_id = doc_id[len(prefix) :].split("/", 1)[0]
                if train_id:
                    found.add(train_id)
            if doc_id.startswith(state_prefix) and doc_id.endswith("/state"):
                train_id = doc_id[len(state_prefix) :].split("/", 1)[0]
                if train_id:
                    found.add(train_id)
        manifest = self.manifest()
        if manifest:
            found.update(str(item) for item in manifest.get("trains", []))
        return sorted(found)

    @staticmethod
    def _command_doc(train_id: str) -> str:
        return f"commands/{train_id}"

    @staticmethod
    def _state_doc(train_id: str) -> str:
        return f"migration/commands/{train_id}/state"

    @staticmethod
    def _migration_id(train_id: str) -> str:
        digest = hashlib.sha1(train_id.encode("utf-8")).hexdigest()[:12]
        return f"cmdmig-{digest}"

    def _record(self, kind: str, train_id: str, station_id: str, **detail: Any) -> Any | None:
        if self._audit is None:
            return None
        entry_id = detail.pop("entry_id", None)
        at = detail.pop("at", None)
        if entry_id:
            return self._audit.record(
                kind, train_id, station_id, at=at, entry_id=entry_id, **detail
            )
        return self._audit.record(kind, train_id, station_id, at=at, **detail)
