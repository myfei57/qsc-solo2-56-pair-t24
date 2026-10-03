"""门控指令格式迁移协调器。

迁移按列车逐条进行，新旧格式在 ``dual`` 阶段并存，旧版／新版上位机都能读；
全部列车迁完并等在办指令走空后，``finish`` 切到 ``done``——此后旧格式只留
归档，执行路径只认 v2。

阶段机：``legacy``（未开始，读写都是 v1）→ ``dual``（并存、逐车迁移）→
``done``（只执行 v2，v1 只归档）。``dual`` 可整体回滚回 ``legacy``（迁完即
不可回滚）。

每列车的状态机（存在迁移状态文档里）：

    legacy → inflight → migrated
                          └─ failed（转换／落盘失败，可重试，v1 原样未动）

单列车的迁移是"先写新、再归档旧、最后删旧"，全程崩溃安全：

1. 标 ``inflight``（原子状态文档）；
2. 读 v1 原列表，逐条升级成 v2（超集字段，语义零改变），写 v2 信封；
3. 把 v1 文档**按字节**复制到 ``commands_archive/``（历史不可变，一个字节
   都不重排），记下 SHA-256；
4. 校验：归档摘要 == v1 现读摘要、v2 逐条摘要自洽；
5. 标 ``migrated`` 并落 v2 指令条数；
6. 删除 v1 活动文档。

任一步崩溃，重启时 :meth:`recover` 按状态与现场文件收敛——v2 与归档都在就
续完，缺 v2 就从 v1 重做，已归档旧文档永远不改不删。
"""

from __future__ import annotations

import threading
from typing import Any

from ..errors import StorageError
from .commands import DoorCommand, QueueDocument, V1, V2, sha256_of
from .jsonfile import JsonStore

STATE_DOC = "migration/commands"
ARCHIVE_PREFIX = "commands_archive/"

LEGACY = "legacy"
DUAL = "dual"
DONE = "done"

_INFLIGHT = "inflight"
_MIGRATED = "migrated"
_FAILED = "failed"


class MigrationCoordinator:
    """门控指令 v1 → v2 的逐车迁移状态机。"""

    def __init__(self, store: JsonStore, clock: Any | None = None) -> None:
        self._store = store
        self._clock = clock
        # 队列读写与逐车迁移共用这把锁（CommandLog 复用同一实例），保证
        # "惰性迁移 + 读改写 v2" 这类跨方法序列整体串行，不丢在办指令。
        self._lock = threading.RLock()
        state = store.read(STATE_DOC)
        if not isinstance(state, dict):
            state = {}
        self._phase = str(state.get("phase", LEGACY))
        self._epoch = int(state.get("epoch", 0))
        trains = state.get("trains", {})
        self._trains: dict[str, dict] = {
            str(train_id): dict(detail) if isinstance(detail, dict) else {}
            for train_id, detail in trains.items()
            if isinstance(train_id, str)
        }
        self._started_at = float(state.get("started_at", 0.0) or 0.0)
        self._finished_at = float(state.get("finished_at", 0.0) or 0.0)
        self._generation = int(state.get("generation", 0))

    # ---- 持久化 -------------------------------------------------------

    @property
    def lock(self) -> threading.RLock:
        """队列变更与迁移共用的串行锁（CommandLog 复用）。"""

        return self._lock

    def _persist(self) -> None:
        payload = {
            "format_from": V1,
            "format_to": V2,
            "phase": self._phase,
            "epoch": self._epoch,
            "generation": self._generation,
            "started_at": self._started_at,
            "finished_at": self._finished_at,
            "trains": self._trains,
        }
        self._store.write(STATE_DOC, payload)

    # ---- 只读视图 -----------------------------------------------------

    @property
    def phase(self) -> str:
        return self._phase

    @property
    def epoch(self) -> int:
        with self._lock:
            return self._epoch

    @property
    def generation(self) -> int:
        """迁移尝试代际，回滚一次加一，用于区分"第几次升级尝试"。"""

        return self._generation

    def train_state(self, train_id: str) -> str:
        """返回单列车的迁移状态（未记录即 ``legacy``）。"""

        with self._lock:
            return str(self._trains.get(train_id, {}).get("state", LEGACY))

    def train_ids(self) -> list[str]:
        """状态文档里出现过的全部列车。"""

        with self._lock:
            return sorted(self._trains)

    def trains_by_state(self) -> dict[str, list[str]]:
        with self._lock:
            grouped: dict[str, list[str]] = {}
            for train_id, detail in self._trains.items():
                grouped.setdefault(str(detail.get("state", LEGACY)), []).append(train_id)
            return {state: sorted(ids) for state, ids in grouped.items()}

    def legacy_live_train_ids(self) -> list[str]:
        """磁盘上仍有 v1 活动文档的列车（归档目录被 JsonStore 排除）。"""

        prefix = "commands/"
        return [
            doc_id[len(prefix) :]
            for doc_id in self._store.list_ids()
            if doc_id.startswith(prefix)
        ]

    def v2_train_ids(self) -> list[str]:
        prefix = "commands_v2/"
        return [
            doc_id[len(prefix) :]
            for doc_id in self._store.list_ids()
            if doc_id.startswith(prefix)
        ]

    def pending_train_ids(self) -> list[str]:
        """dual 阶段尚未迁完的列车：没迁、迁了一半、或还挂着 v1 活动文档。"""

        live = set(self.legacy_live_train_ids())
        pending: list[str] = []
        with self._lock:
            known = set(self._trains)
        for train_id in sorted(live | known):
            state = self.train_state(train_id)
            if state != _MIGRATED:
                pending.append(train_id)
        return pending

    def status(self) -> dict:
        """迁移进度快照，供状态接口、CLI 与审计巡检读取。"""

        with self._lock:
            grouped = self.trains_by_state()
            live = self.legacy_live_train_ids()
            v2 = self.v2_train_ids()
            known = set(self._trains)
        migrated = set(grouped.get(_MIGRATED, []))
        inflight = set(grouped.get(_INFLIGHT, []))
        failed = set(grouped.get(_FAILED, []))
        # "总盘子"取并集：状态文档里的 + 磁盘上两种活动文档里的车。
        universe = known | set(live) | set(v2)
        total = len(universe)
        migrated_count = len(migrated)
        remaining = max(0, total - migrated_count)
        percent = round(100.0 * migrated_count / total, 2) if total else 100.0
        return {
            "format_from": V1,
            "format_to": V2,
            "phase": self._phase,
            "generation": self._generation,
            "epoch": self._epoch,
            "started_at": self._started_at,
            "finished_at": self._finished_at,
            "trains_total": total,
            "trains_migrated": migrated_count,
            "trains_inflight": len(inflight),
            "trains_failed": len(failed),
            "trains_remaining": remaining,
            "legacy_live": sorted(live),
            "v2_live": sorted(v2),
            "progress_percent": percent,
            "can_finish": self._phase == DUAL and not self.pending_train_ids(),
            "can_rollback": self._phase == DUAL,
        }

    # ---- 阶段切换 -----------------------------------------------------

    def begin(self, at: float | None = None) -> dict:
        """从 legacy 进入 dual；已在 dual 时幂等返回当前状态。"""

        with self._lock:
            if self._phase == DUAL:
                return self.status()
            if self._phase == DONE:
                raise StorageError("迁移已完成，不能重新开始", component="migration")
            self._phase = DUAL
            self._generation += 1
            self._started_at = self._now(at)
            self._finished_at = 0.0
            self._persist()
            return self.status()

    def finish(self, at: float | None = None) -> dict:
        """dual → done：要求没有在办／未迁列车，且磁盘上不剩 v1 活动文档。"""

        with self._lock:
            if self._phase == DONE:
                return self.status()
            if self._phase != DUAL:
                raise StorageError(f"阶段 {self._phase} 不能完成迁移", component="migration")
            pending = self.pending_train_ids()
            if pending:
                raise StorageError(
                    f"还有 {len(pending)} 列车未迁完，不能切换到新格式: {sorted(pending)[:10]}",
                    component="migration",
                )
            if self.legacy_live_train_ids():
                raise StorageError("仍有 v1 活动指令文档，不能完成迁移", component="migration")
            self._phase = DONE
            self._finished_at = self._now(at)
            self._persist()
            return self.status()

    def rollback(self) -> dict:
        """dual → legacy：把已迁列车从 v2 还原成 v1 活动文档。

        归档文件保留（历史不可变），只是状态机回到 legacy，执行路径重新读
        v1。``done`` 阶段不允许回滚——那时旧上位机已经下线。
        """

        with self._lock:
            if self._phase == LEGACY:
                return self.status()
            if self._phase != DUAL:
                raise StorageError("只有并存阶段可以回滚", component="migration")
            for train_id, detail in list(self._trains.items()):
                state = str(detail.get("state", LEGACY))
                v2_id = f"commands_v2/{train_id}"
                legacy_id = f"commands/{train_id}"
                if state == _MIGRATED and not self._store.exists(legacy_id):
                    raw = self._store.read(v2_id)
                    if isinstance(raw, dict) and isinstance(raw.get("commands"), list):
                        legacy = [
                            DoorCommand.from_dict(item).as_dict()
                            for item in raw["commands"]
                            if isinstance(item, dict)
                        ]
                        # v2 → v1 降级只保留旧字段，丢弃扩展字段。
                        downgraded = []
                        for item in legacy:
                            downgraded.append(
                                {
                                    key: item[key]
                                    for key in ("id", "train_id", "kind", "epoch", "station_id")
                                }
                            )
                        self._store.write(legacy_id, downgraded)
                self._store.remove(v2_id)
                detail["state"] = LEGACY
                detail["rolled_back"] = True
            self._phase = LEGACY
            self._persist()
            return self.status()

    # ---- 逐车迁移 -----------------------------------------------------

    def migrate_train(self, train_id: str) -> dict:
        """把一列车的在办队列从 v1 迁到 v2（幂等、可崩溃恢复）。"""

        with self._lock:
            if self._phase != DUAL:
                raise StorageError(
                    f"阶段 {self._phase} 不允许逐车迁移", component="migration"
                )
            return self._migrate_locked(train_id)

    def migrate_all(self) -> dict:
        """把磁盘上所有还挂着 v1 活动文档的列车迁完；单条失败不连累其他车。"""

        with self._lock:
            if self._phase != DUAL:
                raise StorageError(
                    f"阶段 {self._phase} 不允许批量迁移", component="migration"
                )
            results: dict[str, dict] = {}
            for train_id in self.legacy_live_train_ids():
                try:
                    results[train_id] = self._migrate_locked(train_id)
                except StorageError as error:
                    results[train_id] = {
                        "train_id": train_id,
                        "state": _FAILED,
                        "error": error.message,
                    }
            return {"results": results, **self.status()}

    def _migrate_locked(self, train_id: str) -> dict:
        legacy_id = f"commands/{train_id}"
        v2_id = f"commands_v2/{train_id}"
        detail = self._trains.setdefault(train_id, {})

        # 已迁完且 v2 在、v1 活动文档已删：幂等成功。
        if detail.get("state") == _MIGRATED and not self._store.exists(legacy_id):
            return {"train_id": train_id, "state": _MIGRATED, "noop": True}

        # 已迁完但又冒出 v1 活动文档（旧上位机补写的漏网指令）：只增量合并
        # 新指令，历史归档一字节不动，另存一份补充归档。
        if detail.get("state") == _MIGRATED and self._store.exists(legacy_id):
            return self._reconcile_stragglers_locked(train_id, detail)

        raw_legacy = self._store.read(legacy_id)
        if raw_legacy is None:
            # v1 没东西可迁（可能发车清空了，或这列车是 dual 阶段才出现的
            # 新车——它从来没有 v1 文档，自然也没有归档）。
            if self._store.exists(v2_id):
                detail.update(state=_MIGRATED, count=len(self._read_v2_items(train_id)), archived=False)
                self._persist()
                return {"train_id": train_id, "state": _MIGRATED, "empty": True}
            detail.update(state=_MIGRATED, count=0, archived=False)
            self._persist()
            return {"train_id": train_id, "state": _MIGRATED, "empty": True}

        if not isinstance(raw_legacy, list):
            detail.update(state=_FAILED)
            self._persist()
            raise StorageError(f"列车 {train_id} 的 v1 队列形态非法", component="migration")

        # 1. inflight 标记（即使重入也重打，保证恢复路径一致）。
        detail["state"] = _INFLIGHT
        self._persist()

        # 2. 升级并写 v2。已存在 v2 时按现场重建（崩溃后续做）。
        upgraded = [
            DoorCommand(
                id=str(item.get("id", "")),
                train_id=str(item.get("train_id", train_id)),
                kind=str(item.get("kind", "")),
                epoch=int(item.get("epoch", 0)),
                station_id=str(item.get("station_id", "")),
            )
            for item in raw_legacy
            if isinstance(item, dict)
        ]
        moment = self._clock.now() if self._clock is not None else 0.0
        v2_commands = tuple(
            DoorCommand(
                id=command.id,
                train_id=command.train_id,
                kind=command.kind,
                epoch=command.epoch,
                station_id=command.station_id,
                ver=V2,
                seq=index,
                issued_at=moment,
                attrs={"migrated_from": V1},
            )
            for index, command in enumerate(upgraded, start=1)
        )
        envelope = QueueDocument(train_id, v2_commands).as_envelope()
        self._store.write(v2_id, envelope)

        # 3. v1 按字节归档（历史不可变）。
        archive_id = self._archive_id(train_id)
        self._store.copy_file(legacy_id, archive_id)

        # 4. 校验：归档字节摘要 == 现存 v1 字节摘要；v2 信封摘要自洽。
        legacy_digest = self._file_sha(legacy_id)
        archived_digest = self._file_sha(archive_id)
        if legacy_digest != archived_digest:
            detail.update(state=_FAILED)
            self._persist()
            raise StorageError(
                f"列车 {train_id} 归档摘要与源不一致: {legacy_digest} != {archived_digest}",
                component="migration",
            )
        self._verify_v2(train_id, envelope)

        # 5. migrated 落盘（含归档定位与摘要，供历史核对）。
        detail.update(
            {
                "state": _MIGRATED,
                "count": len(v2_commands),
                "archived": True,
                "archive": archive_id,
                "legacy_digest": legacy_digest,
                "v2_digest": envelope["digest"],
                "migrated_at": moment,
                "generation": self._generation,
            }
        )
        detail.pop("error", None)
        self._persist()

        # 6. 删 v1 活动文档。归档已经在，删除后执行路径只读 v2。
        self._store.remove(legacy_id)
        return {
            "train_id": train_id,
            "state": _MIGRATED,
            "count": len(v2_commands),
            "archive": archive_id,
        }

    def _archive_id(self, train_id: str) -> str:
        # 同一代际内一列车只归档一次；重放／回滚后再迁由代际区分。
        return f"{ARCHIVE_PREFIX}{train_id}/gen{self._generation:03d}/commands"

    def _supplement_archive_id(self, train_id: str, index: int) -> str:
        # 漏网指令的补充归档：只新增、永不覆盖首份归档。
        return f"{ARCHIVE_PREFIX}{train_id}/gen{self._generation:03d}/commands-{index:02d}"

    def _reconcile_stragglers_locked(self, train_id: str, detail: dict) -> dict:
        """把迁移后冒出来的 v1 漏网指令增量并进 v2，归档只追加不覆盖。"""

        from .commands import DoorCommand as _DC

        legacy_id = f"commands/{train_id}"
        v2_id = f"commands_v2/{train_id}"
        raw_legacy = self._store.read(legacy_id)
        if not isinstance(raw_legacy, list):
            raise StorageError(f"列车 {train_id} 的 v1 队列形态非法", component="migration")
        incoming = {
            str(item.get("id", "")): item
            for item in raw_legacy
            if isinstance(item, dict)
        }
        existing = self._read_v2_items(train_id)
        known = {str(item.get("id", "")) for item in existing}
        new_ids = [cmd_id for cmd_id in incoming if cmd_id not in known]
        if not new_ids:
            # 没有新指令，残留的是已归档旧内容：直接清掉活动文档。
            self._store.remove(legacy_id)
            return {"train_id": train_id, "state": _MIGRATED, "noop": True}

        moment = self._clock.now() if self._clock is not None else 0.0
        seq = max((int(item.get("seq", 0)) for item in existing), default=0)
        upgraded = [_DC.from_dict(incoming[cmd_id]) for cmd_id in new_ids]
        extra = tuple(
            _DC(
                id=command.id, train_id=command.train_id, kind=command.kind,
                epoch=command.epoch, station_id=command.station_id,
                ver=V2, seq=seq + offset, issued_at=moment,
                attrs={"migrated_from": V1, "straggler": True},
            )
            for offset, command in enumerate(upgraded, start=1)
        )
        merged = tuple(_DC.from_dict(item) for item in existing) + extra
        envelope = QueueDocument(train_id, merged).as_envelope()
        self._store.write(v2_id, envelope)

        # 补充归档只记录本次新出现的漏网指令，且只新增文件。
        supplement_index = int(detail.get("supplements", 0)) + 1
        supplement_id = self._supplement_archive_id(train_id, supplement_index)
        self._store.write(supplement_id, [incoming[cmd_id] for cmd_id in new_ids])
        self._verify_v2(train_id, envelope)
        detail.update(
            {
                "state": _MIGRATED,
                "count": len(merged),
                "supplements": supplement_index,
                "last_supplement": supplement_id,
                "v2_digest": envelope["digest"],
            }
        )
        self._persist()
        self._store.remove(legacy_id)
        return {
            "train_id": train_id,
            "state": _MIGRATED,
            "stragglers": new_ids,
            "supplement": supplement_id,
        }

    # ---- 崩溃恢复与校验 ----------------------------------------------

    def recover(self) -> dict:
        """启动／巡检时按现场文件收敛中途状态。

        * inflight 但 v2 与归档都齐：补完 migrated 并删残留 v1；
        * inflight 但 v2 缺或校验不过：退回 legacy 状态，v1 原样还在；
        * migrated 但 v1 活动文档残留（崩在最后一删）：补删；
        * migrated 但 v2 丢失：从 v1 重做（v1 没删过）。
        """

        report = {"repaired": [], "left_legacy": [], "ok": True}
        with self._lock:
            if self._phase == LEGACY:
                return report
            for train_id, detail in list(self._trains.items()):
                state = str(detail.get("state", LEGACY))
                legacy_id = f"commands/{train_id}"
                v2_id = f"commands_v2/{train_id}"
                if state == _MIGRATED:
                    if self._store.exists(v2_id):
                        if self._store.exists(legacy_id):
                            self._store.remove(legacy_id)
                            report["repaired"].append(f"{train_id}:dangling-v1")
                    elif self._store.exists(legacy_id) and self._phase == DUAL:
                        detail["state"] = LEGACY
                        self._persist()
                        self._migrate_locked(train_id)
                        report["repaired"].append(f"{train_id}:redone")
                elif state == _INFLIGHT:
                    v2_ok = self._store.exists(v2_id) and self._verify_v2_file(train_id)
                    archive_ok = self._store.exists(self._archive_id(train_id))
                    if v2_ok and archive_ok and self._store.exists(legacy_id):
                        detail["state"] = _MIGRATED
                        self._persist()
                        self._store.remove(legacy_id)
                        report["repaired"].append(f"{train_id}:completed")
                    else:
                        detail["state"] = LEGACY
                        self._persist()
                        report["left_legacy"].append(train_id)
            return report

    def verify(self) -> dict:
        """只读巡检：逐条核对 v2 摘要、信封摘要与归档字节摘要。"""

        findings: list[str] = []
        with self._lock:
            for train_id, detail in self._trains.items():
                if detail.get("state") != _MIGRATED:
                    continue
                # 迁完后在办指令正常执行完、发车清空：v2 活动文档不存在是
                # 合法终态（指令已被执行，历史只在归档里）。
                if not self._store.exists(f"commands_v2/{train_id}"):
                    if self._store.exists(f"commands/{train_id}"):
                        findings.append(f"{train_id}: v2 缺失但仍有 v1 活动文档")
                    continue
                if not self._verify_v2_file(train_id):
                    findings.append(f"{train_id}: v2 摘要不自洽")
                if not detail.get("archived", False):
                    # 没有 v1 历史的车（dual 阶段的新车）无档可核。
                    continue
                archive_id = detail.get("archive")
                if isinstance(archive_id, str) and self._store.exists(archive_id):
                    actual = self._file_sha(archive_id)
                    if actual != detail.get("legacy_digest"):
                        findings.append(f"{train_id}: 归档摘要漂移")
                else:
                    findings.append(f"{train_id}: 归档缺失")
        return {"ok": not findings, "findings": findings}

    # ---- 内部 ---------------------------------------------------------

    def advance_epoch(self) -> None:
        """推进代际并持久化（发车清空队列时调用）。"""

        with self._lock:
            self._epoch += 1
            self._persist()

    def _read_v2_items(self, train_id: str) -> list[dict]:
        raw = self._store.read(f"commands_v2/{train_id}")
        if isinstance(raw, dict) and isinstance(raw.get("commands"), list):
            return [item for item in raw["commands"] if isinstance(item, dict)]
        return []

    def _verify_v2(self, train_id: str, envelope: dict) -> None:
        items = envelope.get("commands", [])
        for item in items:
            stored = item.get("digest")
            command = DoorCommand.from_dict(item)
            if stored != command.digest():
                raise StorageError(f"列车 {train_id} 指令 {item.get('id')} 摘要不符", "migration")
        body = {key: value for key, value in envelope.items() if key != "digest"}
        if envelope.get("digest") != sha256_of(body):
            raise StorageError(f"列车 {train_id} v2 信封摘要不符", "migration")

    def _verify_v2_file(self, train_id: str) -> bool:
        raw = self._store.read(f"commands_v2/{train_id}")
        if not isinstance(raw, dict):
            return False
        try:
            self._verify_v2(train_id, raw)
        except StorageError:
            return False
        return True

    def _file_sha(self, doc_id: str) -> str:
        import hashlib

        path = self._store.path_of(doc_id)
        try:
            with open(path, "rb") as handle:
                return hashlib.sha256(handle.read()).hexdigest()
        except OSError as error:
            raise StorageError(f"读取 {doc_id} 摘要失败: {error}", component="migration") from error

    def _now(self, at: float | None) -> float:
        if at is not None:
            return float(at)
        return self._clock.now() if self._clock is not None else 0.0
