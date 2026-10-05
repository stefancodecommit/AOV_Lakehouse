"""Data checks shared by ingestion, pytest and (later) Dagster asset checks.

Each check returns CheckResult objects. A failed check with severity "error" stops a
batch; a failed "warning" is reported but doesn't.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str = ""
    examples: list = field(default_factory=list)   # a few offending values, for the report
    severity: str = "error"                          # "error" stops a batch, "warning" doesn't

    def as_dict(self) -> dict:
        return asdict(self)


def all_passed(results: list[CheckResult]) -> bool:
    return all(r.passed for r in results)


def failures(results: list[CheckResult]) -> list[CheckResult]:
    """Failed checks that stop a batch (severity error)."""
    return [r for r in results if not r.passed and r.severity == "error"]


def warnings(results: list[CheckResult]) -> list[CheckResult]:
    """Failed checks that are only reported (severity warning)."""
    return [r for r in results if not r.passed and r.severity == "warning"]
