"""分段 JSONL 流水日志。

流水只追加不重写，按 ``segment_limit`` 条一段滚动成 ``<topic>-00000.jsonl``、
``<topic>-00001.jsonl``…。审计回放需要按段读取，因此同时提供段号列表、单段读取
和全量读取三种入口。
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
from typing import Any

try:
    import fcntl
except ImportError:  # pragma: no cover - 当前交付目标是 Linux
    fcntl = None  # type: ignore[assignment]

from ..errors import StorageError


class JsonlJournal:
    """按段落写入的 JSONL 流水。"""

    def __init__(self, root: str, topic: str, segment_limit: int = 512) -> None:
        self._root = os.path.abspath(root)
        self._topic = topic
        self._segment_limit = max(1, segment_limit)
        self._lock = threading.RLock()
        os.makedirs(self._root, exist_ok=True)
        self._count = self._existing_count()
        self._segment = max(0, self._count - 1) // self._segment_limit if self._count else 0

    @property
    def topic(self) -> str:
        return self._topic

    @property
    def segment_limit(self) -> int:
        return self._segment_limit

    def _segment_path(self, index: int) -> str:
        return os.path.join(self._root, f"{self._topic}-{index:05d}.jsonl")

    def _existing_count(self) -> int:
        total = 0
        for index in range(4096):
            path = self._segment_path(index)
            if not os.path.isfile(path):
                break
            total += len(self._read_file(path))
        return total

    @staticmethod
    def _encode(record: Any) -> str:
        body = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        checksum = hashlib.sha256(body.encode("utf-8")).hexdigest()
        envelope = {"sha256": checksum, "record": json.loads(body)}
        return json.dumps(envelope, ensure_ascii=False, separators=(",", ":"))

    @staticmethod
    def _decode(line: str) -> dict:
        item = json.loads(line)
        if not isinstance(item, dict) or "record" not in item:
            # 兼容升级前已存在的裸 JSONL 审计记录。
            return item if isinstance(item, dict) else {"record": item}
        record = item["record"]
        body = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        actual = hashlib.sha256(body.encode("utf-8")).hexdigest()
        if item.get("sha256") != actual:
            raise ValueError("流水校验和不匹配")
        return record if isinstance(record, dict) else {"record": record}

    def append(self, record: Any) -> str:
        """原子追加一条记录，返回写入的段路径。"""

        line = self._encode(record) + "\n"
        lock_path = os.path.join(self._root, f".{self._topic}.journal.lock")
        with self._lock:
            os.makedirs(self._root, exist_ok=True)
            with open(lock_path, "a+", encoding="utf-8") as lock_handle:
                if fcntl is not None:
                    fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
                index = self._count // self._segment_limit
                path = self._segment_path(index)
                try:
                    with open(path, "a", encoding="utf-8") as handle:
                        handle.write(line)
                        handle.flush()
                        os.fsync(handle.fileno())
                except OSError as error:
                    raise StorageError(f"追加流水失败: {error}", component="store") from error
                self._count += 1
                self._segment = index
                return path

    def segments(self) -> list[str]:
        """返回已存在的段文件路径（按段号升序）。"""

        found: list[str] = []
        for index in range(4096):
            path = self._segment_path(index)
            if not os.path.isfile(path):
                break
            found.append(path)
        return found

    def read_segment(self, index: int) -> list[dict]:
        """读取指定段。"""

        path = self._segment_path(index)
        if not os.path.isfile(path):
            return []
        return self._read_file(path)

    def read_all(self) -> list[dict]:
        """按段号顺序读取全部记录。"""

        records: list[dict] = []
        for path in self.segments():
            records.extend(self._read_file(path))
        return records

    def count(self) -> int:
        """返回累计写入条数。"""

        with self._lock:
            return self._count

    def active_segment(self) -> int:
        """返回当前正在写入的段号。"""

        with self._lock:
            return self._segment

    def _read_file(self, path: str) -> list[dict]:
        records: list[dict] = []
        corrupt: list[str] = []
        try:
            with open(path, "r", encoding="utf-8") as handle:
                for line_number, line in enumerate(handle, start=1):
                    if not line.strip():
                        continue
                    try:
                        item = self._decode(line)
                    except (json.JSONDecodeError, ValueError) as error:
                        corrupt.append(f"{line_number}:{error}:{line.rstrip()}")
                        continue
                    records.append(item)
        except OSError as error:
            raise StorageError(f"读取流水 {path} 失败: {error}", component="store") from error
        if corrupt:
            quarantine = path + ".corrupt"
            try:
                with open(quarantine, "a", encoding="utf-8") as handle:
                    for item in corrupt:
                        handle.write(item + "\n")
            except OSError:
                pass
            raise StorageError(f"流水 {path} 存在损坏记录，已隔离至 {quarantine}", component="store")
        return records
