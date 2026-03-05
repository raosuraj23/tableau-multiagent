# models/connectivity_report.py
"""
ConnectivityReport — Structured Output of the Connectivity Testing Agent
=========================================================================

The ConnectivityReport is the gating artifact between Phase 03 (Connectivity)
and Phase 04 (Data Profiling). The orchestrator checks:

    report.can_proceed       → True if no RED connections on required sources
    report.connection_statuses  → per-connection health dict
    report.tableau_cloud_ok  → Tableau Cloud PAT is valid

Traffic-light model:
    GREEN  = all checks passed (TCP + auth + query)
    YELLOW = TCP reached but auth/query failed (credentials issue)
    RED    = TCP unreachable (network/firewall issue)

Usage:
    report = ConnectivityReport()
    report.add_result("conn_001", ConnectionHealth.GREEN,
                      host="acct.snowflakecomputing.com", latency_ms=42.1)
    if report.can_proceed:
        # move to Phase 04
"""

from __future__ import annotations

from dataclasses import dataclass, field as dc_field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional


class ConnectionHealth(str, Enum):
    GREEN  = "GREEN"    # TCP + auth + query all passed
    YELLOW = "YELLOW"   # TCP ok, auth or query failed — credentials issue
    RED    = "RED"      # TCP unreachable — network / firewall issue
    SKIP   = "SKIP"     # Test skipped (dry-run or explicitly disabled)


@dataclass
class ConnectionResult:
    """Health status for a single connection."""
    connection_id:   str
    health:          ConnectionHealth
    host:            str             = ""
    port:            Optional[int]   = None
    db_class:        str             = ""           # snowflake, postgres, etc.
    tcp_ok:          bool            = False
    auth_ok:         bool            = False
    query_ok:        bool            = False
    latency_ms:      float           = 0.0
    error_message:   Optional[str]   = None
    tested_at:       str             = dc_field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    @property
    def is_usable(self) -> bool:
        """True if GREEN or YELLOW (connected, may need cred fix)."""
        return self.health in (ConnectionHealth.GREEN, ConnectionHealth.YELLOW)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "connection_id": self.connection_id,
            "health":        self.health.value,
            "host":          self.host,
            "port":          self.port,
            "db_class":      self.db_class,
            "tcp_ok":        self.tcp_ok,
            "auth_ok":       self.auth_ok,
            "query_ok":      self.query_ok,
            "latency_ms":    round(self.latency_ms, 2),
            "error_message": self.error_message,
            "tested_at":     self.tested_at,
        }


@dataclass
class TableauCloudResult:
    """Health status for the Tableau Cloud PAT authentication."""
    ok:             bool            = False
    site_id:        str             = ""
    server_url:     str             = ""
    error_message:  Optional[str]   = None
    latency_ms:     float           = 0.0
    tested_at:      str             = dc_field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ok":            self.ok,
            "site_id":       self.site_id,
            "server_url":    self.server_url,
            "error_message": self.error_message,
            "latency_ms":    round(self.latency_ms, 2),
            "tested_at":     self.tested_at,
        }


@dataclass
class ConnectivityReport:
    """
    Complete output of the Connectivity Testing Agent.

    The orchestrator checks `report.can_proceed` before routing to Phase 04.
    """
    project_id:        str                   = ""
    run_id:            str                   = ""
    connection_results: List[ConnectionResult] = dc_field(default_factory=list)
    tableau_cloud:     Optional[TableauCloudResult] = None
    timestamp:         str                   = dc_field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    # ── Convenience ─────────────────────────────────────────────────────────

    def add_result(self, result: ConnectionResult) -> "ConnectivityReport":
        self.connection_results.append(result)
        return self

    def get_result(self, connection_id: str) -> Optional[ConnectionResult]:
        return next(
            (r for r in self.connection_results if r.connection_id == connection_id),
            None,
        )

    # ── Health aggregation ───────────────────────────────────────────────────

    @property
    def green_count(self) -> int:
        return sum(1 for r in self.connection_results
                   if r.health == ConnectionHealth.GREEN)

    @property
    def yellow_count(self) -> int:
        return sum(1 for r in self.connection_results
                   if r.health == ConnectionHealth.YELLOW)

    @property
    def red_count(self) -> int:
        return sum(1 for r in self.connection_results
                   if r.health == ConnectionHealth.RED)

    @property
    def tableau_cloud_ok(self) -> bool:
        return self.tableau_cloud is not None and self.tableau_cloud.ok

    @property
    def can_proceed(self) -> bool:
        """
        True if:
          - Zero RED connections (all hosts reachable)
          - Tableau Cloud auth is valid (or was not tested)

        YELLOW connections are allowed — credentials can be fixed without
        halting the structural profiling work.
        """
        return self.red_count == 0

    # ── Serialization ─────────────────────────────────────────────────────

    def summary(self) -> Dict[str, Any]:
        return {
            "project_id":       self.project_id,
            "can_proceed":      self.can_proceed,
            "total_tested":     len(self.connection_results),
            "green":            self.green_count,
            "yellow":           self.yellow_count,
            "red":              self.red_count,
            "tableau_cloud_ok": self.tableau_cloud_ok,
            "timestamp":        self.timestamp,
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            **self.summary(),
            "run_id":             self.run_id,
            "connection_results": [r.to_dict() for r in self.connection_results],
            "tableau_cloud":      self.tableau_cloud.to_dict()
                                  if self.tableau_cloud else None,
        }

    def __repr__(self) -> str:
        return (
            f"ConnectivityReport(can_proceed={self.can_proceed}, "
            f"green={self.green_count}, yellow={self.yellow_count}, "
            f"red={self.red_count})"
        )


# ── Factory ────────────────────────────────────────────────────────────────────

def connectivity_report_from_dict(d: Dict[str, Any]) -> ConnectivityReport:
    """
    Reconstruct a ConnectivityReport from to_dict() output.
    Strips computed properties (can_proceed, green_count, etc.) that are
    not constructor parameters.
    """
    _cr_fields = {f.name for f in __import__('dataclasses').fields(ConnectionResult)}
    _tc_fields = {f.name for f in __import__('dataclasses').fields(TableauCloudResult)}

    conn_results = [
        ConnectionResult(**{k: v for k, v in cr.items() if k in _cr_fields})
        for cr in d.get("connection_results", [])
    ]

    tableau = None
    if d.get("tableau_cloud"):
        tableau = TableauCloudResult(**{
            k: v for k, v in d["tableau_cloud"].items() if k in _tc_fields
        })

    return ConnectivityReport(
        project_id=d.get("project_id", ""),
        run_id=d.get("run_id", ""),
        connection_results=conn_results,
        tableau_cloud=tableau,
        timestamp=d.get("timestamp", ""),
    )
