"""文件型持久化。

``JsonStore`` 保存"当前态"文档（每个文档一个 JSON 文件，写时先落临时文件再
原子替换）；``JsonlJournal`` 保存"流水"（按分段追加，只追加不重写）；
``StoreMetrics`` 给出目录用量；``CommandLog`` 保存门控指令队列（v1／v2 双
格式可并存）；``MigrationCoordinator`` 负责门控指令格式的逐车迁移、归档与
崩溃恢复。四者都落在控制服务配置的 ``storage_root`` 下。
"""

from __future__ import annotations

from .commands import V1, V2, CommandLog, DoorCommand, QueueDocument
from .jsonfile import JsonStore
from .journal import JsonlJournal
from .metrics import StoreMetrics
from .migration import DONE, DUAL, LEGACY, MigrationCoordinator

__all__ = [
    "DONE",
    "DUAL",
    "LEGACY",
    "V1",
    "V2",
    "CommandLog",
    "DoorCommand",
    "JsonStore",
    "JsonlJournal",
    "MigrationCoordinator",
    "QueueDocument",
    "StoreMetrics",
]
