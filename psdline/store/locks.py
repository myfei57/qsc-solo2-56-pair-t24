"""按列车串行化门控队列变更的文件锁。"""

from __future__ import annotations

import os
from contextlib import contextmanager

try:
    import fcntl
except ImportError:  # pragma: no cover - 当前交付目标是 Linux
    fcntl = None  # type: ignore[assignment]


class TrainLock:
    """跨进程的列车级互斥锁；同进程内也使用同一把 RLock。"""

    def __init__(self, root: str, train_id: str) -> None:
        self._path = os.path.join(root, "migration", "locks", f"{train_id}.lock")
        self._handle = None

    def __enter__(self) -> "TrainLock":
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        self._handle = open(self._path, "a+", encoding="utf-8")
        if fcntl is not None:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._handle is None:
            return
        try:
            if fcntl is not None:
                fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        finally:
            self._handle.close()
            self._handle = None


@contextmanager
def train_lock(root: str, train_id: str):
    with TrainLock(root, train_id):
        yield
