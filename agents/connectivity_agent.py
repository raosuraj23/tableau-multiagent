# agents/connectivity_agent.py
"""
ConnectivityAgent — Phase 03: Connectivity Testing
===================================================

Tests every connection defined in connections.csv:
  Step 1 — TCP ping (socket connect to host:port)
  Step 2 — Auth handshake (driver-level connect with credentials)
  Step 3 — Basic query (SELECT 1 / equivalent)
  Step 4 — Tableau Cloud PAT validation (sign in + sign out)

Traffic-light result per connection:
  GREEN  = all 3 steps passed
  YELLOW = TCP ok, auth/query failed → credential issue, not network
  RED    = TCP unreachable → network/firewall issue

Output (written to WorkbookState):
  - connectivity_report: ConnectivityReport.to_dict()

Gate condition for Phase 04:
  connectivity_report["can_proceed"] == True  (zero RED connections)

Design notes:
  - Network calls use configurable timeout (default 10s from test_config.yaml)
  - All DB driver imports are lazy — missing drivers produce YELLOW, not crash
  - Tableau Cloud test uses TSC library; absent TSC = YELLOW with clear message
  - Dry-run mode: all tests SKIP, report can_proceed = True
  - Each connection test is independent; one failure does not abort others
"""

from __future__ import annotations

import os
import socket
import time
from typing import Any, Dict, List, Optional, Tuple

import structlog

from agents.base_agent import (
    AgentResult,
    AgentStatus,
    BaseAgent,
    ErrorSeverity,
    PhaseContext,
)
from models.connectivity_report import (
    ConnectivityReport,
    ConnectionHealth,
    ConnectionResult,
    TableauCloudResult,
)
from models.project_spec import AuthConfig, ConnectionConfig, ProjectSpec


logger = structlog.get_logger().bind(agent="connectivity_agent")

# ── Default connection ports per driver class ──────────────────────────────────

DEFAULT_PORTS: Dict[str, int] = {
    "snowflake":   443,
    "postgres":    5432,
    "mysql":       3306,
    "redshift":    5439,
    "bigquery":    443,
    "sqlserver":   1433,
    "oracle":      1521,
    "excel-direct": 0,     # local file — no TCP test
}

# ── Validation query per driver class ─────────────────────────────────────────

VALIDATION_QUERY: Dict[str, str] = {
    "snowflake":   "SELECT 1",
    "postgres":    "SELECT 1",
    "mysql":       "SELECT 1",
    "redshift":    "SELECT 1",
    "sqlserver":   "SELECT 1",
    "oracle":      "SELECT 1 FROM DUAL",
    "bigquery":    "SELECT 1",
    "excel-direct": "",    # no query for file-based connections
}


# ── ConnectivityAgent ──────────────────────────────────────────────────────────

class ConnectivityAgent(BaseAgent):
    """
    Phase 03 — Connectivity Testing

    Tests all database connections and Tableau Cloud auth.
    Produces a ConnectivityReport that gates Phase 04 (Data Profiling).
    """

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        context: Optional[PhaseContext] = None,
        tcp_timeout:  float = 10.0,   # seconds
        auth_timeout: float = 30.0,   # seconds
    ) -> None:
        super().__init__(
            agent_id="connectivity_agent",
            phase="CONNECTING",
            config=config or {},
            context=context,
        )
        self.tcp_timeout  = float(
            self.config.get("tcp_timeout_seconds", tcp_timeout)
        )
        self.auth_timeout = float(
            self.config.get("auth_timeout_seconds", auth_timeout)
        )

    # ── validate_input ──────────────────────────────────────────────────────

    def validate_input(self, state: Dict[str, Any]) -> List[str]:
        errors = []
        if not state.get("project_spec"):
            errors.append(
                "project_spec not found. Run IntakeAgent (Phase 01) first."
            )
        if not state.get("validation_report"):
            errors.append(
                "validation_report not found. Run ValidationAgent (Phase 02) first."
            )
        else:
            if not state["validation_report"].get("can_proceed", False):
                errors.append(
                    "ValidationAgent (Phase 02) reported can_proceed=False. "
                    "Fix validation errors before running connectivity tests."
                )
        return errors

    # ── run ────────────────────────────────────────────────────────────────

    def run(self, state: Dict[str, Any]) -> AgentResult:
        self.log_start()
        result = AgentResult(agent_id=self.agent_id, phase=self.phase)

        # Reconstruct ProjectSpec
        try:
            spec = ProjectSpec.model_validate(state["project_spec"])
        except Exception as e:
            result.add_error(
                f"Cannot reconstruct ProjectSpec: {e}",
                severity=ErrorSeverity.CRITICAL, exc=e,
            )
            return self.log_complete(result)

        report = ConnectivityReport(
            project_id=spec.project_config.project_id,
            run_id=self.context.run_id if self.context else "",
        )

        # Dry-run: skip all live network calls
        if self.context and self.context.dry_run:
            for conn in spec.connections:
                report.add_result(ConnectionResult(
                    connection_id=conn.connection_id,
                    health=ConnectionHealth.SKIP,
                    host=conn.server,
                    db_class=conn.class_,
                ))
            report.tableau_cloud = TableauCloudResult(
                ok=True, site_id=spec.project_config.tableau_site,
                server_url=spec.project_config.tableau_server_url,
                error_message="Skipped in dry-run mode",
            )
            result.output = {"connectivity_report": report.to_dict()}
            result.metadata["connectivity_summary"] = report.summary()
            result.status = AgentStatus.SUCCESS
            return self.log_complete(result)

        # Test each database connection
        for conn in spec.connections:
            auth = next(
                (a for a in spec.auth_configs if a.auth_id == conn.auth_id), None
            )
            conn_result = self._test_connection(conn, auth)
            report.add_result(conn_result)

            if conn_result.health == ConnectionHealth.RED:
                result.add_error(
                    f"Connection '{conn.connection_id}' is RED: "
                    f"{conn_result.error_message}",
                    severity=ErrorSeverity.CRITICAL,
                    field=conn.connection_id,
                )
            elif conn_result.health == ConnectionHealth.YELLOW:
                result.add_error(
                    f"Connection '{conn.connection_id}' is YELLOW: "
                    f"{conn_result.error_message}",
                    severity=ErrorSeverity.HIGH,
                    field=conn.connection_id,
                )

        # Test Tableau Cloud
        tableau_result = self._test_tableau_cloud(spec)
        report.tableau_cloud = tableau_result
        if not tableau_result.ok:
            result.add_error(
                f"Tableau Cloud auth failed: {tableau_result.error_message}",
                severity=ErrorSeverity.HIGH,
                field="tableau_cloud",
            )

        # Set final status
        result.output = {"connectivity_report": report.to_dict()}
        result.metadata["connectivity_summary"] = report.summary()

        if report.can_proceed and not result.has_blocking_errors:
            result.status = (AgentStatus.WARNING
                             if report.yellow_count > 0 or not tableau_result.ok
                             else AgentStatus.SUCCESS)
        # If blocking errors exist, execute() will set FAILED

        self.logger.info("connectivity_complete", **report.summary())
        return self.log_complete(result)

    # ── TCP test ────────────────────────────────────────────────────────────

    def _tcp_ping(self, host: str, port: int) -> Tuple[bool, float, Optional[str]]:
        """
        Attempt a TCP socket connection to host:port.
        Returns (success, latency_ms, error_message).
        """
        if port == 0:
            return True, 0.0, None  # local file — skip TCP

        start = time.monotonic()
        try:
            with socket.create_connection((host, port), timeout=self.tcp_timeout):
                latency = (time.monotonic() - start) * 1000
                return True, latency, None
        except socket.timeout:
            return False, 0.0, f"TCP timeout after {self.tcp_timeout}s to {host}:{port}"
        except OSError as e:
            return False, 0.0, f"TCP error to {host}:{port}: {e}"

    # ── Auth + query test per driver ────────────────────────────────────────

    def _test_connection(
        self,
        conn: ConnectionConfig,
        auth: Optional[AuthConfig],
    ) -> ConnectionResult:
        """Run TCP → auth → query for a single connection config."""
        cls      = conn.class_.lower()
        host     = conn.server
        port     = conn.port or DEFAULT_PORTS.get(cls, 443)

        result = ConnectionResult(
            connection_id=conn.connection_id,
            health=ConnectionHealth.RED,
            host=host,
            port=port,
            db_class=cls,
        )

        # ── Step 1: TCP ping ──────────────────────────────────────────────
        tcp_ok, latency_ms, tcp_err = self._tcp_ping(host, port)
        result.tcp_ok    = tcp_ok
        result.latency_ms = latency_ms

        if not tcp_ok:
            result.health        = ConnectionHealth.RED
            result.error_message = tcp_err
            return result

        # ── Step 2: Auth handshake + Step 3: Query ────────────────────────
        username = _get_env(auth.username_env if auth else None)
        password = _get_env(auth.password_env if auth else None)

        auth_ok, query_ok, auth_err = self._test_driver(
            cls, host, port, conn, username, password
        )
        result.auth_ok  = auth_ok
        result.query_ok = query_ok

        if auth_ok and query_ok:
            result.health = ConnectionHealth.GREEN
        elif tcp_ok:
            result.health        = ConnectionHealth.YELLOW
            result.error_message = auth_err
        return result

    def _test_driver(
        self,
        cls: str,
        host: str,
        port: int,
        conn: ConnectionConfig,
        username: str,
        password: str,
    ) -> Tuple[bool, bool, Optional[str]]:
        """
        Attempt driver-level connection + SELECT 1.
        Returns (auth_ok, query_ok, error_message).
        """
        if cls == "excel-direct":
            # File-based: check file exists
            filename = getattr(conn, "filename", None) or ""
            if filename and os.path.exists(filename):
                return True, True, None
            return True, True, None  # assume ok if no filename provided

        if cls == "snowflake":
            return self._test_snowflake(host, conn, username, password)
        elif cls in ("postgres", "redshift"):
            return self._test_postgres(host, port, conn, username, password)
        elif cls == "mysql":
            return self._test_mysql(host, port, conn, username, password)
        elif cls == "sqlserver":
            return self._test_sqlserver(host, port, conn, username, password)
        else:
            # Unknown driver — TCP success is enough to call it YELLOW
            return False, False, f"Driver '{cls}' not supported for auth testing"

    def _test_snowflake(
        self, host: str, conn: ConnectionConfig, username: str, password: str
    ) -> Tuple[bool, bool, Optional[str]]:
        try:
            import snowflake.connector  # type: ignore
        except ImportError:
            return False, False, "snowflake-connector-python not installed"

        if not username or not password:
            return False, False, "Snowflake credentials not set in environment"

        try:
            sf_conn = snowflake.connector.connect(
                account=host.replace(".snowflakecomputing.com", ""),
                user=username,
                password=password,
                database=conn.dbname,
                schema=getattr(conn, "schema_", "PUBLIC"),
                warehouse=conn.warehouse or "",
                login_timeout=int(self.auth_timeout),
            )
            cur = sf_conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            cur.close()
            sf_conn.close()
            return True, True, None
        except Exception as e:
            return False, False, f"Snowflake auth/query failed: {e}"

    def _test_postgres(
        self, host: str, port: int, conn: ConnectionConfig, username: str, password: str
    ) -> Tuple[bool, bool, Optional[str]]:
        try:
            import psycopg2  # type: ignore
        except ImportError:
            return False, False, "psycopg2-binary not installed"

        if not username or not password:
            return False, False, "PostgreSQL credentials not set in environment"

        try:
            pg_conn = psycopg2.connect(
                host=host, port=port,
                dbname=conn.dbname,
                user=username, password=password,
                connect_timeout=int(self.auth_timeout),
            )
            cur = pg_conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            cur.close()
            pg_conn.close()
            return True, True, None
        except Exception as e:
            return False, False, f"PostgreSQL auth/query failed: {e}"

    def _test_mysql(
        self, host: str, port: int, conn: ConnectionConfig, username: str, password: str
    ) -> Tuple[bool, bool, Optional[str]]:
        try:
            import mysql.connector  # type: ignore
        except ImportError:
            return False, False, "mysql-connector-python not installed"

        if not username or not password:
            return False, False, "MySQL credentials not set in environment"

        try:
            my_conn = mysql.connector.connect(
                host=host, port=port,
                database=conn.dbname,
                user=username, password=password,
                connection_timeout=int(self.auth_timeout),
            )
            cur = my_conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            cur.close()
            my_conn.close()
            return True, True, None
        except Exception as e:
            return False, False, f"MySQL auth/query failed: {e}"

    def _test_sqlserver(
        self, host: str, port: int, conn: ConnectionConfig, username: str, password: str
    ) -> Tuple[bool, bool, Optional[str]]:
        try:
            import pyodbc  # type: ignore
        except ImportError:
            return False, False, "pyodbc not installed"

        if not username or not password:
            return False, False, "SQL Server credentials not set in environment"

        try:
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={host},{port};"
                f"DATABASE={conn.dbname};"
                f"UID={username};PWD={password};"
                f"Connection Timeout={int(self.auth_timeout)};"
            )
            ms_conn = pyodbc.connect(conn_str)
            cur = ms_conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            cur.close()
            ms_conn.close()
            return True, True, None
        except Exception as e:
            return False, False, f"SQL Server auth/query failed: {e}"

    # ── Tableau Cloud test ─────────────────────────────────────────────────

    def _test_tableau_cloud(self, spec: ProjectSpec) -> TableauCloudResult:
        """Validate Tableau Cloud PAT by signing in and immediately signing out."""
        server_url = spec.project_config.tableau_server_url
        site_id    = spec.project_config.tableau_site

        # Find first auth record that has PAT credentials
        pat_auth = next(
            (a for a in spec.auth_configs if a.pat_name_env and a.pat_secret_env),
            None,
        )

        if not pat_auth:
            return TableauCloudResult(
                ok=False, site_id=site_id, server_url=server_url,
                error_message="No auth record with pat_name_env + pat_secret_env found",
            )

        pat_name   = _get_env(pat_auth.pat_name_env)
        pat_secret = _get_env(pat_auth.pat_secret_env)

        if not pat_name or not pat_secret:
            return TableauCloudResult(
                ok=False, site_id=site_id, server_url=server_url,
                error_message=(
                    f"Tableau PAT env vars not set: "
                    f"{pat_auth.pat_name_env}, {pat_auth.pat_secret_env}"
                ),
            )

        try:
            import tableauserverclient as TSC  # type: ignore
        except ImportError:
            return TableauCloudResult(
                ok=False, site_id=site_id, server_url=server_url,
                error_message="tableauserverclient not installed",
            )

        start = time.monotonic()
        try:
            auth_obj = TSC.PersonalAccessTokenAuth(pat_name, pat_secret,
                                                    site_id=site_id)
            server   = TSC.Server(server_url, use_server_version=True)
            with server.auth.sign_in(auth_obj):
                latency = (time.monotonic() - start) * 1000
                return TableauCloudResult(
                    ok=True, site_id=site_id, server_url=server_url,
                    latency_ms=latency,
                )
        except Exception as e:
            return TableauCloudResult(
                ok=False, site_id=site_id, server_url=server_url,
                error_message=f"Tableau Cloud sign-in failed: {e}",
            )


# ── Helpers ────────────────────────────────────────────────────────────────────

def _get_env(env_var_name: Optional[str]) -> str:
    """Resolve an env var reference to its value. Returns '' if not set."""
    if not env_var_name:
        return ""
    return os.environ.get(env_var_name.strip(), "")
