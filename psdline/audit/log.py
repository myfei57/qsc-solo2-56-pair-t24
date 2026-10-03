"""审计流水写入。"""

from __future__ import annotations

import json

from ..clock import Clock
from ..errors import RailError
from ..eventbus import Event, EventBus
from ..ids import IdFactory
from ..store import JsonlJournal
from .entry import AuditEntry


class AuditLog:
    """审计流水：内存索引 + 分段 JSONL 落盘。"""

    def __init__(self, clock: Clock, bus: EventBus, journal: JsonlJournal) -> None:
        self._clock = clock
        self._bus = bus
        self._journal = journal
        self._ids = IdFactory("audit")
        self._index: list[AuditEntry] = []
        self.reload()

    @property
    def journal(self) -> JsonlJournal:
        return self._journal

    @property
    def ids(self) -> IdFactory:
        return self._ids

    def reload(self) -> int:
        """从流水恢复内存索引，返回恢复条数。"""

        records = self._journal.read_all()
        self._index = [AuditEntry.from_dict(item) for item in records if isinstance(item, dict)]
        return len(self._index)

    def record(
        self,
        kind: str,
        train_id: str,
        station_id: str,
        at: float | None = None,
        entry_id: str | None = None,
        **detail: object,
    ) -> AuditEntry:
        """写入一条审计流水；传入固定 ``entry_id`` 时重放恢复不会重复造号。"""

        if not kind:
            raise RailError("审计动作类型为空", component="audit")
        entry = AuditEntry(
            id=entry_id or self._ids.next(),
            kind=kind,
            train_id=train_id,
            station_id=station_id,
            at=self._clock.now() if at is None else at,
            detail={key: value for key, value in detail.items() if value is not None},
        )
        if entry_id and self.has_entry(entry_id):
            return AuditEntry.from_dict(
                next(item for item in self._journal.read_all() if item.get("id") == entry_id)
            )
        self._journal.append(entry.as_dict())
        self._index.append(entry)
        self._bus.publish(
            Event.of(
                "audit",
                "audit",
                kind,
                severity="notice",
                train_id=train_id,
                station_id=station_id,
            )
        )
        return entry

    def list(self) -> list[AuditEntry]:
        """返回内存里保留的审计条目。"""

        return list(self._index)

    def has_entry(self, entry_id: str) -> bool:
        """判断固定标识的审计条目是否已经落盘。"""

        if any(entry.id == entry_id for entry in self._index):
            return True
        return any(
            isinstance(record, dict) and record.get("id") == entry_id
            for record in self._journal.read_all()
        )

    def replay(self, limit: int = 100_000) -> list[AuditEntry]:
        """从落盘流水回放审计条目。"""

        records = self._journal.read_all()[:limit]
        return [AuditEntry.from_dict(item) for item in records if isinstance(item, dict)]

    def segments(self) -> list[list[dict]]:
        """按段返回落盘流水，供巡检逐段核对。"""

        return [
            self._journal.read_segment(index)
            for index in range(len(self._journal.segments()))
        ]

    def summary(self) -> dict:
        """返回审计用量与动作分布。"""

        entries = self.list()
        return {
            "entries": len(entries),
            "journal_records": self._journal.count(),
            "segment": self._journal.active_segment(),
            "segments": len(self._journal.segments()),
            "segment_sizes": [len(items) for items in self.segments()],
            "by_kind": count_kinds(entries),
        }

    def dump(self) -> str:
        """把内存索引序列化成一行 JSON，供状态接口内嵌。"""

        return json.dumps([entry.as_dict() for entry in self.list()], ensure_ascii=False)


def count_kinds(entries: list[AuditEntry]) -> dict[str, int]:
    """按动作类型统计条数。"""

    counts: dict[str, int] = {}
    for entry in entries:
        counts[entry.kind] = counts.get(entry.kind, 0) + 1
    return counts
