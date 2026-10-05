"""Data checks shared by ingestion, pytest and (later) Dagster asset checks.

Each check returns CheckResult objects; a batch passes when none failed.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str = ""
    examples: list = field(default_factory=list)   # a few offending values, for the report

    def as_dict(self) -> dict:
        return asdict(self)


def all_passed(results: list[CheckResult]) -> bool:
    return all(r.passed for r in results)


def failures(results: list[CheckResult]) -> list[CheckResult]:
    return [r for r in results if not r.passed]
