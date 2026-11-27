"""Backup of lakehouse/ and inbox/kvk/ (tools/backup.py, risk R14)."""
import json
import tarfile

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from pipeline.common import paths
from tools import backup as bk


def some_data():
    """Two small Delta tables (one with two versions), a staging table and a KVK upload."""
    write_deltalake(str(paths.lakehouse().root / "silver" / "t"), pa.table({"v": [1]}))
    write_deltalake(str(paths.lakehouse().root / "silver" / "t"), pa.table({"v": [2]}), mode="append")
    write_deltalake(str(paths.lakehouse().root / "gold" / "g"), pa.table({"w": ["x"]}))
    write_deltalake(str(paths.lakehouse().root / "silver" / "_staging" / "run" / "s"), pa.table({"v": [9]}))
    (paths.inbox() / "kvk").mkdir(parents=True)
    ((paths.inbox() / "kvk") / "kvk.zip").write_bytes(b"zip")


def members(archive):
    with tarfile.open(archive) as tar:
        return tar.getnames()


def test_backup_holds_the_lakehouse_and_kvk_uploads_and_verifies():
    some_data()
    archive = bk.create()
    names = members(archive)
    assert "inbox/kvk/kvk.zip" in names and "manifest.json" in names
    assert not any("_staging" in n for n in names)          # half-finished writes left out
    manifest = json.loads(tarfile.open(archive).extractfile("manifest.json").read())
    assert manifest["tables"] == {"gold/g": {"version": 0, "files": 1},
                                  "silver/t": {"version": 1, "files": 2}}
    assert bk.verify(archive) == []
    assert bk.backups() == [archive]


def test_delta_logs_are_archived_before_the_data_files():
    some_data()
    names = [n for n in members(bk.create()) if n.startswith("lakehouse/")]
    first_data = min(i for i, n in enumerate(names) if "_delta_log" not in n)
    assert all("_delta_log" not in n for n in names[first_data:])


def test_verify_finds_a_missing_data_file(tmp_path):
    some_data()
    archive = bk.create()
    broken = tmp_path / "broken.tar.gz"
    with tarfile.open(archive) as src, tarfile.open(broken, "w:gz") as dst:
        dropped = False
        for m in src.getmembers():
            if m.name.startswith("lakehouse/silver/t/") and m.name.endswith(".parquet") \
                    and not dropped:
                dropped = True
                continue
            dst.addfile(m, src.extractfile(m) if m.isfile() else None)
    problems = bk.verify(broken)
    assert len(problems) == 1 and problems[0].startswith("silver/t: 1 data files missing")


def test_only_the_newest_backups_are_kept():
    some_data()
    paths.backup_dir().mkdir()
    for stamp in ("20260101T000000Z", "20260201T000000Z", "20260301T000000Z"):
        (paths.backup_dir() / f"backup_{stamp}.tar.gz").write_bytes(b"old")
    new = bk.create(keep=2)
    assert [p.name for p in bk.backups()] == [new.name, "backup_20260301T000000Z.tar.gz"]


def test_keep_zero_is_refused_before_anything_is_written():
    """keep: 0 would delete the new archive along with the old ones: no backup at all."""
    some_data()
    paths.backup_dir().mkdir()
    (paths.backup_dir() / "backup_20260301T000000Z.tar.gz").write_bytes(b"old")
    with pytest.raises(ValueError, match="at least 1"):
        bk.create(keep=0)
    assert [p.name for p in bk.backups()] == ["backup_20260301T000000Z.tar.gz"]


def test_restore_into_an_empty_folder_only(tmp_path_factory):
    some_data()
    archive = bk.create()
    target = tmp_path_factory.mktemp("elsewhere") / "restored"   # outside the lakehouse
    bk.restore(archive, target)
    assert DeltaTable(str(target / "lakehouse" / "silver" / "t")).to_pandas()["v"].sort_values().tolist() == [1, 2]
    assert (target / "inbox" / "kvk" / "kvk.zip").read_bytes() == b"zip"
    with pytest.raises(ValueError, match="not empty"):
        bk.restore(archive, target)
    with pytest.raises(ValueError, match="not over the lakehouse"):
        bk.restore(archive, paths.lakehouse().root)
    with pytest.raises(ValueError, match="not over the lakehouse"):
        bk.restore(archive, paths.lakehouse().root / "silver" / "new")


def test_backup_of_an_empty_lakehouse():
    archive = bk.create()
    assert members(archive) == ["manifest.json"] and bk.verify(archive) == []


def test_a_backup_on_the_lakehouse_disk_warns_or_refuses(caplog):
    some_data()
    assert bk.same_disk(paths.backup_dir(), paths.lakehouse().root)       # both in tmp_path
    bk.create(require_other_disk=False)
    assert "same disk as the lakehouse" in caplog.text
    unplugged = paths.lakehouse().root / "media" / "usb" / "backups"  # a drive that isn't there
    with pytest.raises(RuntimeError, match="same disk"):
        bk.create(unplugged, require_other_disk=True)
    assert not unplugged.exists()                          # nothing created on this disk
