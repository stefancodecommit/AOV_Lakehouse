"""
Where the data lives: the lakehouse layout, in one place.

    from pipeline.common.paths import lakehouse
    lakehouse().gold                    # <lakehouse>/gold
    Lakehouse(copy).silver              # the same layout in another folder (e.g. a copy)

lakehouse() reads the settings (config.yaml: paths.lakehouse) each time it is called,
not once when a module is imported. So pointing the settings at another folder moves
every layer at once: the tests do that with three settings (tests/conftest.py) instead
of patching a path constant in every module, and a module that is added later can't be
forgotten. Functions that take `lakehouse=None` (the pipeline's run(), ingest(), ...)
use lakehouse() for None and Lakehouse(<folder>) otherwise.

Until 2026-11-27 every module computed its own folders at import (BRONZE_ROOT,
SILVER_ROOT, GOLD_ROOT, RUN_LOG, ...), and the tests patched about 40 of them.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pipeline.common.config import project_path, settings


@dataclass(frozen=True)
class Lakehouse:
    """The folders and tables of a lakehouse at `root`."""
    root: Path

    # layers and shared folders
    @property
    def bronze(self) -> Path:
        return self.root / "bronze"

    @property
    def silver(self) -> Path:
        return self.root / "silver"

    @property
    def gold(self) -> Path:
        return self.root / "gold"

    @property
    def run_log(self) -> Path:
        """One row per silver/gold run (pipeline/common/runlog.py)."""
        return self.root / "_run_log"

    @property
    def quarantine(self) -> Path:
        """Batches and files that failed their checks (checks/quarantine.py)."""
        return self.root / "quarantine"

    # bronze per source
    @property
    def cbs_log(self) -> Path:
        """CBS: which API requests are in bronze (pipeline/bronze/cbs.py)."""
        return self.bronze / "_ingestion_log"

    @property
    def kvk_bronze(self) -> Path:
        return self.bronze / "kvk"

    @property
    def kvk_data(self) -> Path:
        """KVK rows as received (pipeline/bronze/kvk.py)."""
        return self.kvk_bronze / "basis_bedrijfsgegevens"

    @property
    def kvk_log(self) -> Path:
        """Which KVK files are loaded."""
        return self.kvk_bronze / "_batch_log"

    @property
    def reference_bronze(self) -> Path:
        return self.bronze / "reference"

    @property
    def crosswalk(self) -> Path:
        """The SBI 2008 -> 2025 crosswalk rows (pipeline/bronze/reference.py)."""
        return self.reference_bronze / "sbi2008_sbi2025"

    @property
    def reference_log(self) -> Path:
        """Which crosswalk files are loaded."""
        return self.reference_bronze / "_batch_log"


def lakehouse(root: Path | None = None) -> Lakehouse:
    """The lakehouse at `root`, or the configured one (paths.lakehouse) for None."""
    return Lakehouse(project_path(settings["paths"]["lakehouse"]) if root is None else Path(root))


def inbox() -> Path:
    """Where files arrive before bronze (paths.inbox): inbox/kvk/, inbox/reference/.
    Not part of the lakehouse: Docker shares it with the computer."""
    return project_path(settings["paths"]["inbox"])


def backup_dir() -> Path:
    """Where tools/backup.py writes its archives (backup.path)."""
    return project_path(settings["backup"]["path"])
