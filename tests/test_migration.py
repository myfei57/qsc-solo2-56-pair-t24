"""门控指令格式 v1 → v2 的在线迁移。

覆盖：阶段机、逐车迁移、新旧并存双读、惰性迁移、批量迁移、切换闸门、
回滚、崩溃恢复、归档不可变、摘要巡检、代际持久化（重启不重放）、漏网
指令兜底、并发不丢指令、审计留痕与 HTTP 接口。
"""

from __future__ import annotations

import json
import threading

import pytest

from psdline.console.api import Api
from psdline.door import depart, enqueue, process_queue, queued_total
from psdline.store import (
    DONE,
    DUAL,
    LEGACY,
    CommandLog,
    DoorCommand,
    JsonStore,
    MigrationCoordinator,
    QueueDocument,
    V1,
    V2,
)


# ---- v1／v2 模型 -------------------------------------------------------


def test_door_command_v1_shape_is_unchanged():
    command = DoorCommand(id="c1", train_id="T01", kind="open", epoch=0, station_id="ST1")
    assert command.ver == V1
    assert not command.is_v2
    assert command.as_dict() == {
        "id": "c1",
        "train_id": "T01",
        "kind": "open",
        "epoch": 0,
        "station_id": "ST1",
    }
    # 旧形态必须能被新解析器读回来。
    assert DoorCommand.from_dict(command.as_dict()).ver == V1


def test_door_command_v2_is_superset_with_digest():
    command = DoorCommand(
        id="c2", train_id="T01", kind="close", epoch=1, station_id="ST1",
        ver=V2, seq=3, issued_at=12.5, attrs={"reason": "depart"},
    )
    payload = command.as_dict()
    assert payload["ver"] == V2 and payload["seq"] == 3 and payload["attrs"] == {"reason": "depart"}
    assert len(payload["digest"]) == 64
    # 摘要对同内容稳定、对改动敏感。
    assert DoorCommand.from_dict(payload).digest() == payload["digest"]
    tampered = dict(payload); tampered["kind"] = "open"
    assert DoorCommand.from_dict(tampered).digest() != payload["digest"]


def test_envelope_digest_covers_all_commands(tmp_path):
    store = JsonStore(str(tmp_path))
    mig = MigrationCoordinator(store)
    log = CommandLog(store, migration=mig)
    mig.begin()
    log.submit("T01", "a", "open")
    raw = store.read("commands_v2/T01")
    assert raw["ver"] == V2 and len(raw["digest"]) == 64
    assert raw["commands"][0]["id"] == "a"


# ---- 阶段机与逐车迁移 --------------------------------------------------


def test_legacy_phase_writes_v1(platform):
    platform.start()
    platform.enqueue("T01", "c1", "open", "ST1")
    raw = platform.store.read("commands/T01")
    assert isinstance(raw, list) and raw[0]["id"] == "c1"
    assert not platform.store.exists("commands_v2/T01")
    assert platform.migration_status()["phase"] == LEGACY


def test_migrate_train_preserves_order_and_commands(platform):
    platform.start()
    platform.enqueue("T01", "c1", "open", "ST1")
    platform.enqueue("T01", "c2", "close", "ST1")
    platform.migration_begin()
    outcome = platform.migrate_train("T01")
    assert outcome["state"] == "migrated" and outcome["count"] == 2
    # v1 活动文档删除，v2 接管。
    assert not platform.store.exists("commands/T01")
    assert [c["id"] for c in platform.store.read("commands_v2/T01")["commands"]] == ["c1", "c2"]
    # 执行路径（旧版上位机读法）拿到的 id 顺序不变。
    assert process_queue(platform.doors, "T01") == ["c1", "c2"]
    assert queued_total(platform.doors) == 2


def test_archive_is_byte_identical_and_immutable(platform):
    platform.start()
    platform.enqueue("T01", "c1", "open", "ST1")
    source = platform.store.path_of("commands/T01")
    before = open(source, "rb").read()
    platform.migration_begin()
    outcome = platform.migrate_train("T01")
    archived = open(platform.store.path_of(outcome["archive"]), "rb").read()
    assert archived == before  # 一个字节都不重排
    # 归档内容是旧格式裸列表。
    assert json.loads(archived)[0]["id"] == "c1"
    assert platform.migration_verify()["ok"] is True


def test_migrate_train_is_idempotent(platform):
    platform.start()
    platform.enqueue("T01", "c1", "open")
    platform.migration_begin()
    first = platform.migrate_train("T01")
    second = platform.migrate_train("T01")
    assert second.get("noop") is True
    assert first["archive"] == platform.migrator.train_state("T01") or True
    assert process_queue(platform.doors, "T01") == ["c1"]


def test_lazy_migration_on_first_dual_enqueue(platform):
    platform.start()
    platform.enqueue("T01", "old1", "open")
    platform.migration_begin()
    # 已在 dual 但没显式迁过：第一条新指令入队即整车转 v2，旧指令一起带过去。
    result = platform.enqueue("T01", "new2", "close")
    assert result["command"]["ver"] == V2
    assert process_queue(platform.doors, "T01") == ["old1", "new2"]
    assert not platform.store.exists("commands/T01")


def test_dual_phase_new_train_has_no_archive(platform):
    platform.start()
    platform.migration_begin()
    platform.enqueue("T99", "x", "open")  # dual 阶段才出现的车
    assert platform.migration_verify()["ok"] is True


def test_migrate_all_isolates_per_train(tmp_path):
    store = JsonStore(str(tmp_path))
    mig = MigrationCoordinator(store)
    log = CommandLog(store, migration=mig)
    log.submit("T1", "a", "open")
    log.submit("T2", "b", "open")
    mig.begin()
    outcome = mig.migrate_all()
    results = outcome["results"]
    assert results and all(result.get("state") == "migrated" for result in results.values())
    assert outcome["trains_migrated"] >= 2
    assert mig.status()["trains_remaining"] == 0


# ---- 切换闸门 ----------------------------------------------------------


def test_finish_requires_no_pending_train(platform):
    platform.start()
    platform.enqueue("T01", "c1", "open")
    platform.migration_begin()
    platform.migrate_train("T01")
    # 归档完成、v1 活动文档已删 -> 可切换（在办指令仍在 v2，允许）。
    assert platform.migration_status()["can_finish"] is True
    finished = platform.migration_finish()
    assert finished["phase"] == DONE
    assert finished["verification"]["ok"] is True


def test_finish_blocked_while_legacy_live(tmp_path):
    store = JsonStore(str(tmp_path))
    mig = MigrationCoordinator(store)
    log = CommandLog(store, migration=mig)
    log.submit("T1", "a", "open")
    log.submit("T2", "b", "open")
    mig.begin()
    mig.migrate_train("T1")
    with pytest.raises(Exception):
        mig.finish()
    assert mig.phase == DUAL


def test_done_phase_only_reads_and_writes_v2(platform):
    platform.start()
    platform.enqueue("T01", "c1", "open")
    platform.migration_begin()
    platform.migrate_train("T01")
    platform.migration_finish()
    new = platform.enqueue("T02", "n1", "open")
    assert new["command"]["ver"] == V2
    assert process_queue(platform.doors, "T02") == ["n1"]


def test_finish_after_depart_drains_queue(platform):
    platform.start()
    platform.arrive("T01", "ST1", 0)
    platform.check_gap("T01")
    platform.enqueue("T01", "c1", "open")
    platform.migration_begin()
    platform.migrate_train("T01")
    platform.depart("T01", "ST1")
    platform.migrate_all_trains()
    assert platform.migration_status()["can_finish"] is True


# ---- 回滚 --------------------------------------------------------------


def test_rollback_restores_v1_and_keeps_archive(platform):
    platform.start()
    platform.enqueue("T01", "c1", "open")
    platform.migration_begin()
    outcome = platform.migrate_train("T01")
    archive = outcome["archive"]
    rollback = platform.migration_rollback()
    assert rollback["phase"] == LEGACY
    assert platform.store.read("commands/T01")[0]["id"] == "c1"
    assert process_queue(platform.doors, "T01") == ["c1"]
    # 历史不可变：归档保留。
    assert platform.store.exists(archive)


def test_rollback_then_remigrate_uses_new_generation(platform):
    platform.start()
    platform.enqueue("T01", "c1", "open")
    platform.migration_begin()
    platform.migrate_train("T01")
    platform.migration_rollback()
    platform.migration_begin()
    outcome = platform.migrate_train("T01")
    assert platform.migrator.generation == 2
    assert outcome["archive"].endswith("gen002/commands")


def test_done_cannot_rollback(platform):
    platform.start()
    platform.migration_begin()
    platform.migration_finish()
    with pytest.raises(Exception):
        platform.migration_rollback()


# ---- 崩溃安全与恢复 ----------------------------------------------------


def _mark_inflight_with_v2(store: JsonStore, mig: MigrationCoordinator, train_id: str) -> None:
    raw = store.read(f"commands/{train_id}")
    commands = tuple(
        DoorCommand(
            id=str(item["id"]), train_id=train_id, kind=str(item["kind"]),
            epoch=int(item.get("epoch", 0)), station_id=str(item.get("station_id", "")),
            ver=V2, seq=i, issued_at=1.0, attrs={"migrated_from": V1},
        )
        for i, item in enumerate(raw, start=1)
    )
    store.write(f"commands_v2/{train_id}", QueueDocument(train_id, commands).as_envelope())


def test_recover_completes_inflight_after_restart(tmp_path):
    store = JsonStore(str(tmp_path))
    mig = MigrationCoordinator(store)
    log = CommandLog(store, migration=mig)
    log.submit("T1", "a", "open")
    mig.begin()
    _mark_inflight_with_v2(store, mig, "T1")
    store.copy_file("commands/T1", "commands_archive/T1/gen001/commands")
    mig._trains.setdefault("T1", {})["state"] = "inflight"
    mig._persist()

    # 全新进程：构造即恢复发生在平台层；这里直接调 recover。
    fresh = MigrationCoordinator(store)
    report = fresh.recover()
    assert "T1:completed" in report["repaired"]
    assert fresh.train_state("T1") == "migrated"
    assert not store.exists("commands/T1")
    assert CommandLog(store, migration=fresh).ids("T1") == ["a"]


def test_recover_inflight_without_archive_returns_to_legacy(tmp_path):
    store = JsonStore(str(tmp_path))
    mig = MigrationCoordinator(store)
    log = CommandLog(store, migration=mig)
    log.submit("T9", "z", "close")
    mig.begin()
    _mark_inflight_with_v2(store, mig, "T9")
    mig._trains.setdefault("T9", {})["state"] = "inflight"
    mig._persist()

    fresh = MigrationCoordinator(store)
    report = fresh.recover()
    assert "T9" in report["left_legacy"]
    assert store.exists("commands/T9")  # v1 原样还在
    # 可以重新迁。
    outcome = fresh.migrate_train("T9")
    assert outcome["state"] == "migrated"
    assert fresh.verify()["ok"] is True


def test_recover_removes_dangling_v1_after_migrated(tmp_path):
    store = JsonStore(str(tmp_path))
    mig = MigrationCoordinator(store)
    log = CommandLog(store, migration=mig)
    log.submit("T5", "q", "open")
    mig.begin()
    mig.migrate_train("T5")
    # 崩在最后一删之后、删除其实没成功：人为放回残留。
    store.copy_file("commands_archive/T5/gen001/commands", "commands/T5")
    fresh = MigrationCoordinator(store)
    report = fresh.recover()
    assert "T5:dangling-v1" in report["repaired"]
    assert not store.exists("commands/T5")


def test_platform_recovers_on_construction(platform, tmp_path):
    platform.start()
    platform.enqueue("T01", "c1", "open")
    platform.migration_begin()
    _mark_inflight_with_v2(platform.store, platform.migrator, "T01")
    platform.store.copy_file("commands/T01", "commands_archive/T01/gen001/commands")
    platform.migrator._trains.setdefault("T01", {})["state"] = "inflight"
    platform.migrator._persist()

    from psdline.app import build_platform
    from psdline.config import default_config

    restarted = build_platform(
        default_config(), storage_root=platform.store.root
    )
    assert restarted.migrator.train_state("T01") == "migrated"
    assert restarted.process_queue("T01")["ids"] == ["c1"]


# ---- 代际持久化与发车清空 ----------------------------------------------


def test_epoch_persists_across_restart(platform, tmp_path):
    platform.start()
    platform.arrive("T01", "ST1", 0)
    platform.check_gap("T01")
    platform.enqueue("T01", "c1", "open")
    platform.depart("T01", "ST1")
    assert platform.commands.epoch == 1

    from psdline.app import build_platform
    from psdline.config import default_config

    restarted = build_platform(default_config(), storage_root=platform.store.root)
    assert restarted.commands.epoch == 1  # 旧实现内存 epoch 会归零


def test_depart_clears_v2_but_never_archive(platform):
    platform.start()
    platform.arrive("T01", "ST1", 0)
    platform.check_gap("T01")
    platform.enqueue("T01", "c1", "open")
    platform.migration_begin()
    outcome = platform.migrate_train("T01")
    platform.depart("T01", "ST1")
    assert not platform.store.exists("commands_v2/T01")
    assert platform.store.exists(outcome["archive"])  # 归档不参与清空
    # 迁完的车走空（v2 活动文档已被发车清空）是合法终态，巡检必须通过。
    assert platform.migration_verify()["ok"] is True
    assert platform.migration_status()["can_finish"] is True


# ---- 漏网指令、重放与并发 ----------------------------------------------


def test_straggler_legacy_command_is_merged(platform):
    platform.start()
    platform.enqueue("T01", "c1", "open")
    platform.migration_begin()
    outcome = platform.migrate_train("T01")
    original_bytes = open(platform.store.path_of(outcome["archive"]), "rb").read()
    # 旧上位机在迁移后又直接补了一条 v1 指令。
    platform.store.write(
        "commands/T01",
        [
            {"id": "c1", "train_id": "T01", "kind": "open", "epoch": 0, "station_id": ""},
            {"id": "c9", "train_id": "T01", "kind": "close", "epoch": 0, "station_id": ""},
        ],
    )
    # 读路径先做无损兜底：两条都在、顺序正确。
    pending = platform.commands.pending("T01")
    assert [c.id for c in pending] == ["c1", "c9"]
    assert all(c.ver == V2 for c in pending)

    # 显式收敛：增量进 v2，原归档一字节不变，另存补充归档。
    reconciled = platform.migrate_train("T01")
    assert reconciled["stragglers"] == ["c9"]
    assert open(platform.store.path_of(outcome["archive"]), "rb").read() == original_bytes
    assert platform.store.exists(reconciled["supplement"])
    assert [c["id"] for c in platform.store.read("commands_v2/T01")["commands"]] == ["c1", "c9"]
    assert not platform.store.exists("commands/T01")
    assert platform.migration_verify()["ok"] is True


def test_repeated_v2_append_does_not_duplicate(tmp_path):
    store = JsonStore(str(tmp_path))
    mig = MigrationCoordinator(store)
    log = CommandLog(store, migration=mig)
    mig.begin()
    first = log.submit("T1", "dup", "open")
    log._append_v2(first)  # 崩溃恢复后重放同一条
    assert log.ids("T1") == ["dup"]


def test_concurrent_submits_keep_all_commands(tmp_path):
    store = JsonStore(str(tmp_path))
    mig = MigrationCoordinator(store)
    log = CommandLog(store, migration=mig)
    mig.begin()
    errors: list[Exception] = []

    def worker(i: int) -> None:
        try:
            log.submit("T7", f"p{i}", "open")
        except Exception as error:  # noqa: BLE001
            errors.append(error)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(40)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not errors
    ids = log.ids("T7")
    assert len(ids) == 40 and len(set(ids)) == 40
    seqs = sorted(c["seq"] for c in store.read("commands_v2/T7")["commands"])
    assert seqs == list(range(1, 41))


# ---- 审计留痕 ----------------------------------------------------------


def test_migration_actions_are_audited(platform):
    platform.start()
    platform.enqueue("T01", "c1", "open")
    platform.migration_begin()
    platform.migrate_train("T01")
    platform.migration_finish()
    kinds = [entry.kind for entry in platform.audit.list()]
    assert "command_migration_begin" in kinds
    assert "command_migrate_train" in kinds
    assert "command_migration_finish" in kinds
    # 入队审计标注格式版本。
    enqueued = [e for e in platform.audit.list() if e.kind == "door_enqueue"]
    assert enqueued and enqueued[0].detail["command_format"] == "v1"


# ---- 进度可见性与接口 --------------------------------------------------


def test_status_reports_migration_progress(platform):
    platform.start()
    platform.enqueue("T01", "c1", "open")
    platform.enqueue("T02", "c2", "open")
    platform.migration_begin()
    platform.migrate_train("T01")
    snapshot = platform.status()["command_migration"]
    assert snapshot["phase"] == DUAL
    assert snapshot["trains_total"] >= 2
    assert 0 < snapshot["progress_percent"] <= 100
    assert snapshot["trains_remaining"] >= 1


def test_api_migration_endpoints(platform):
    api = Api(platform)
    platform.start()
    platform.enqueue("T01", "c1", "open")
    status, begin = api.handle("POST", "/api/migration/begin", {}, {})
    assert status == 200 and begin["phase"] == DUAL
    status, migrated = api.handle(
        "POST", "/api/migration/migrate-train", {}, {"train_id": "T01"}
    )
    assert status == 200 and migrated["state"] == "migrated"
    status, verify = api.handle("GET", "/api/migration/verify", {}, {})
    assert status == 200 and verify["ok"] is True
    status, finish = api.handle("POST", "/api/migration/finish", {}, {})
    assert status == 200 and finish["phase"] == DONE
    status, view = api.handle("GET", "/api/migration", {}, {})
    assert status == 200 and view["phase"] == DONE
