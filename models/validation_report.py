# models/validation_report.py
"""
ValidationReport — Structured Output of the Metadata Validation Agent
======================================================================

The ValidationReport is the gating artifact between Phase 02 (Validation)
and Phase 03 (Connectivity Testing). The orchestrator reads:

    report.can_proceed  →  True if 0 CRITICAL errors
    report.summary()    →  dict for structured logging
    report.findings     →  list of ValidationFinding (errors + warnings)

Usage:
    report = ValidationReport()
    report.add_finding("metrics.csv", "formula syntax invalid: SUM([X]",
                       severity=FindingSeverity.CRITICAL, rule="formula_syntax")
    if report.can_proceed:
        # move to Phase 03
"""

from __future__ import annotations

from dataclasses import dataclass, field as dc_field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional


class FindingSeverity(str, Enum):
    CRITICAL = "CRITICAL"   # Blocks workflow — must be fixed
    HIGH     = "HIGH"       # Blocks with manual override
    MEDIUM   = "MEDIUM"     # Warning — logged, workflow continues
    INFO     = "INFO"       # Informational — no action required


class FindingCategory(str, Enum):
    SCHEMA       = "schema"         # Field type, format, allowed values
    BUSINESS     = "business"       # Business rule (formula syntax, etc.)
    COMPLETENESS = "completeness"   # Required fields populated
    CONSISTENCY  = "consistency"    # Cross-field and cross-file consistency
    PERFORMANCE  = "performance"    # Potential performance concerns
    SECURITY     = "security"       # Credential / security issues


@dataclass
class ValidationFinding:
    """A single validation finding (error, warning, or info notice)."""
    source:     str                             # CSV file or field name
    message:    str
    severity:   FindingSeverity = FindingSeverity.MEDIUM
    category:   FindingCategory = FindingCategory.SCHEMA
    rule:       str             = ""            # Rule ID for traceability
    field:      Optional[str]   = None          # Specific field within source
    value:      Optional[str]   = None          # The problematic value
    suggestion: Optional[str]   = None          # How to fix it
    timestamp:  str             = dc_field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    @property
    def is_blocking(self) -> bool:
        return self.severity in (FindingSeverity.CRITICAL, FindingSeverity.HIGH)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source":     self.source,
            "message":    self.message,
            "severity":   self.severity.value,
            "category":   self.category.value,
            "rule":       self.rule,
            "field":      self.field,
            "value":      self.value,
            "suggestion": self.suggestion,
            "timestamp":  self.timestamp,
        }


@dataclass
class ValidationReport:
    """
    Complete output of the Metadata Validation Agent.

    The orchestrator checks `report.can_proceed` before routing to Phase 03.
    All downstream agents receive this report via WorkbookState.
    """
    project_id:  str = ""
    run_id:      str = ""
    findings:    List[ValidationFinding] = dc_field(default_factory=list)
    rules_run:   List[str]              = dc_field(default_factory=list)
    timestamp:   str = dc_field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    # ── Convenience ────────────────────────────────────────────────────────

    def add_finding(
        self,
        source: str,
        message: str,
        severity: FindingSeverity = FindingSeverity.MEDIUM,
        category: FindingCategory = FindingCategory.SCHEMA,
        rule: str = "",
        field: Optional[str] = None,
        value: Optional[str] = None,
        suggestion: Optional[str] = None,
    ) -> "ValidationReport":
        self.findings.append(ValidationFinding(
            source=source, message=message, severity=severity,
            category=category, rule=rule, field=field,
            value=value, suggestion=suggestion,
        ))
        return self  # fluent

    def add_critical(self, source: str, message: str, rule: str = "",
                     field: Optional[str] = None,
                     value: Optional[str] = None,
                     suggestion: Optional[str] = None) -> "ValidationReport":
        return self.add_finding(source, message, FindingSeverity.CRITICAL,
                                rule=rule, field=field, value=value,
                                suggestion=suggestion)

    def add_warning(self, source: str, message: str, rule: str = "",
                    field: Optional[str] = None,
                    value: Optional[str] = None,
                    suggestion: Optional[str] = None) -> "ValidationReport":
        return self.add_finding(source, message, FindingSeverity.MEDIUM,
                                rule=rule, field=field, value=value,
                                suggestion=suggestion)

    def add_info(self, source: str, message: str,
                 rule: str = "") -> "ValidationReport":
        return self.add_finding(source, message, FindingSeverity.INFO, rule=rule)

    # ── Counts ─────────────────────────────────────────────────────────────

    @property
    def critical_count(self) -> int:
        return sum(1 for f in self.findings
                   if f.severity == FindingSeverity.CRITICAL)

    @property
    def high_count(self) -> int:
        return sum(1 for f in self.findings
                   if f.severity == FindingSeverity.HIGH)

    @property
    def warning_count(self) -> int:
        return sum(1 for f in self.findings
                   if f.severity == FindingSeverity.MEDIUM)

    @property
    def info_count(self) -> int:
        return sum(1 for f in self.findings
                   if f.severity == FindingSeverity.INFO)

    @property
    def blocking_count(self) -> int:
        return sum(1 for f in self.findings if f.is_blocking)

    @property
    def can_proceed(self) -> bool:
        """True if no CRITICAL or HIGH findings — safe to move to Phase 03."""
        return self.blocking_count == 0

    def findings_by_severity(self, severity: FindingSeverity) -> List[ValidationFinding]:
        return [f for f in self.findings if f.severity == severity]

    def findings_by_source(self, source: str) -> List[ValidationFinding]:
        return [f for f in self.findings if f.source == source]

    def findings_by_category(self, category: FindingCategory) -> List[ValidationFinding]:
        return [f for f in self.findings if f.category == category]

    # ── Serialization ──────────────────────────────────────────────────────

    def summary(self) -> Dict[str, Any]:
        return {
            "project_id":    self.project_id,
            "can_proceed":   self.can_proceed,
            "rules_run":     len(self.rules_run),
            "total_findings": len(self.findings),
            "critical":      self.critical_count,
            "high":          self.high_count,
            "warnings":      self.warning_count,
            "info":          self.info_count,
            "timestamp":     self.timestamp,
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            **self.summary(),
            "run_id":   self.run_id,
            "rules_run": self.rules_run,
            "findings": [f.to_dict() for f in self.findings],
        }

    def __repr__(self) -> str:
        return (
            f"ValidationReport(can_proceed={self.can_proceed}, "
            f"critical={self.critical_count}, high={self.high_count}, "
            f"warnings={self.warning_count})"
        )
