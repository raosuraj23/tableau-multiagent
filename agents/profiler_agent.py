# agents/profiler_agent.py
"""
ProfilerAgent — Phase 04: Source Schema Profiling
==================================================

Introspects the actual database schema for every table defined in tables.csv,
using INFORMATION_SCHEMA queries, and compares against columns.csv declarations.

Per-table workflow:
  1. Check ConnectivityReport — skip tables on RED connections
  2. Query INFORMATION_SCHEMA.COLUMNS for the table
  3. Cross-reference each physical column against columns.csv declarations
  4. Detect type mismatches (physical type -> Tableau type != declared type)
  5. Optionally sample row counts and NULL statistics
  6. Assemble TableProfile -> SchemaProfile

Output (written to WorkbookState):
    schema_profiles: List[Dict]  — one SchemaProfile per datasource

Gate condition for Phase 05 (DataConversionAgent):
    ALL schema_profiles have can_proceed == True

Design notes:
  - DB driver imports are lazy — missing drivers produce warning, not crash
  - Dry-run: profiles synthesized from columns.csv declarations (no DB call)
  - YELLOW connections attempt profiling (TCP succeeded); failure -> warning
  - Type mismatch detection uses map_physical_to_tableau()
  - Row-count sampling is optional (config: enable_row_count_sampling, default True)
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

import structlog

from agents.base_agent import (
    AgentResult,
    AgentStatus,
    BaseAgent,
    ErrorSeverity,
    PhaseContext,
)
from models.connectivity_report import ConnectionHealth, ConnectivityReport, connectivity_report_from_dict
from models.project_spec import (
    AuthConfig,
    ColumnConfig,
    ConnectionConfig,
    DataSourceConfig,
    ProjectSpec,
    TableConfig,
)
from models.schema_profile import (
    ColumnProfile,
    SchemaProfile,
    TableProfile,
    map_physical_to_tableau,
)

logger = structlog.get_logger().bind(agent="profiler_agent")

_INFO_SCHEMA_COLS = """
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    ORDINAL_POSITION
FROM {db}.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = '{schema}'
  AND TABLE_NAME   = '{table}'
ORDER BY ORDINAL_POSITION
"""

_ROW_COUNT_QUERY = "SELECT COUNT(*) FROM {schema}.{table}"


class ProfilerAgent(BaseAgent):
    """Phase 04 — Source Schema Profiler."""

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        context: Optional[PhaseContext] = None,
    ) -> None:
        super().__init__(
            agent_id="profiler_agent",
            phase="PROFILING",
            config=config or {},
            context=context,
        )
        self.enable_row_counts   = bool(self.config.get("enable_row_count_sampling", True))
        self.enable_null_profile = bool(self.config.get("enable_null_profiling", False))

    # ------------------------------------------------------------------ #
    def validate_input(self, state: Dict[str, Any]) -> List[str]:
        errors = []
        if not state.get("project_spec"):
            errors.append("project_spec missing — run IntakeAgent first.")
        if not state.get("connectivity_report"):
            errors.append("connectivity_report missing — run ConnectivityAgent first.")
        # NOTE: We do NOT block on can_proceed=False here.
        # RED connections are handled gracefully inside run() — those tables
        # are marked unprofiled. This allows partial profiling of GREEN/YELLOW
        # datasources even when some connections are RED.
        return errors

    # ------------------------------------------------------------------ #
    def run(self, state: Dict[str, Any]) -> AgentResult:
        self.log_start()
        result = AgentResult(agent_id=self.agent_id, phase=self.phase)

        try:
            spec        = ProjectSpec.model_validate(state["project_spec"])
            conn_report = connectivity_report_from_dict(state["connectivity_report"])
        except Exception as e:
            result.add_error(f"Cannot reconstruct input models: {e}",
                             severity=ErrorSeverity.CRITICAL, exc=e)
            return self.log_complete(result)

        all_profiles: List[Dict[str, Any]] = []

        for ds in spec.data_sources:
            profile = self._profile_datasource(ds, spec, conn_report)
            all_profiles.append(profile.to_dict())

            if not profile.can_proceed:
                result.add_error(
                    f"Datasource '{ds.datasource_id}' produced zero profiled tables.",
                    severity=ErrorSeverity.CRITICAL,
                    field=ds.datasource_id,
                )
            elif profile.unprofiled_tables:
                for t in profile.unprofiled_tables:
                    result.add_error(
                        f"Table '{t.table_name}' in '{ds.datasource_id}' "
                        f"could not be profiled: {t.error}",
                        severity=ErrorSeverity.HIGH,
                        field=t.table_id,
                    )
            if profile.total_type_mismatches:
                result.add_error(
                    f"Datasource '{ds.datasource_id}' has "
                    f"{profile.total_type_mismatches} type mismatch(es). "
                    "Update columns.csv datatype declarations.",
                    severity=ErrorSeverity.HIGH,
                    field=ds.datasource_id,
                )

        result.output = {"schema_profiles": all_profiles}
        result.metadata["profiling_summary"] = {
            "datasources_profiled": len(all_profiles),
            "total_tables":   sum(p["total_tables"]  for p in all_profiles),
            "total_columns":  sum(p["total_columns"] for p in all_profiles),
            "type_mismatches": sum(p["type_mismatches"] for p in all_profiles),
        }
        self.logger.info("profiling_complete", **result.metadata["profiling_summary"])
        if not result.has_blocking_errors:
            result.status = AgentStatus.SUCCESS
        return self.log_complete(result)

    # ------------------------------------------------------------------ #
    def _profile_datasource(
        self,
        ds: DataSourceConfig,
        spec: ProjectSpec,
        conn_report: ConnectivityReport,
    ) -> SchemaProfile:
        profile = SchemaProfile(
            datasource_id=ds.datasource_id,
            project_id=spec.project_config.project_id,
            run_id=self.context.run_id if self.context else "",
        )
        conn_result = conn_report.get_result(ds.connection_id)
        conn_health = conn_result.health if conn_result else ConnectionHealth.RED
        conn_cfg    = spec.get_connection(ds.connection_id)
        auth_cfg    = next(
            (a for a in spec.auth_configs
             if conn_cfg and a.auth_id == conn_cfg.auth_id), None
        )

        for tbl in spec.get_tables_for_datasource(ds.datasource_id):
            cols_declared = spec.get_columns_for_table(tbl.table_id)

            if conn_health == ConnectionHealth.RED:
                profile.add_table(TableProfile(
                    table_id=tbl.table_id, table_name=tbl.table_name,
                    schema=tbl.schema_ or ds.default_schema or "PUBLIC",
                    profiled=False, error="Connection is RED — host unreachable",
                ))
                continue

            if self.context and self.context.dry_run:
                profile.add_table(
                    self._synthesize_from_csv(tbl, ds, cols_declared)
                )
                continue

            profile.add_table(
                self._profile_table(tbl, ds, conn_cfg, auth_cfg, cols_declared)
            )

        return profile

    # ------------------------------------------------------------------ #
    def _profile_table(
        self,
        tbl: TableConfig,
        ds: DataSourceConfig,
        conn_cfg: Optional[ConnectionConfig],
        auth_cfg: Optional[AuthConfig],
        columns_declared: List[ColumnConfig],
    ) -> TableProfile:
        schema = (tbl.schema_ or ds.default_schema or "PUBLIC").upper()
        tp = TableProfile(table_id=tbl.table_id, table_name=tbl.table_name,
                          schema=schema)

        if not conn_cfg:
            tp.error = "ConnectionConfig not found"
            return tp

        cls      = conn_cfg.class_.lower()
        username = _env(auth_cfg.username_env if auth_cfg else None)
        password = _env(auth_cfg.password_env if auth_cfg else None)

        if cls == "snowflake":
            raw_cols, row_count, err = self._introspect_snowflake(
                conn_cfg, schema, tbl.table_name, username, password)
        elif cls in ("postgres", "redshift"):
            raw_cols, row_count, err = self._introspect_postgres(
                conn_cfg, schema, tbl.table_name, username, password)
        elif cls == "mysql":
            raw_cols, row_count, err = self._introspect_mysql(
                conn_cfg, schema, tbl.table_name, username, password)
        elif cls == "excel-direct":
            return self._synthesize_from_csv(tbl, ds, columns_declared)
        else:
            tp.error = f"Driver '{cls}' not supported for profiling"
            return tp

        if err:
            tp.error = err
            return tp

        declared_map = {c.column_name.upper(): c for c in columns_declared}
        tp.row_count = row_count
        tp.profiled  = True

        for col_name, col_type, is_nullable in raw_cols:
            decl     = declared_map.get(col_name.upper())
            tab_type = map_physical_to_tableau(col_type)
            tp.add_column(ColumnProfile(
                physical_name=col_name,
                physical_type=col_type,
                tableau_datatype=tab_type,
                declared_name=decl.column_name if decl else "",
                declared_datatype=decl.datatype if decl else "",
                nullable=(is_nullable.upper() == "YES")
                         if isinstance(is_nullable, str) else bool(is_nullable),
            ))
        return tp

    # ------------------------------------------------------------------ #
    def _introspect_snowflake(
        self, conn: ConnectionConfig, schema: str, table: str,
        username: str, password: str,
    ) -> Tuple[List[Tuple[str, str, str]], Optional[int], Optional[str]]:
        try:
            import snowflake.connector  # type: ignore
        except ImportError:
            return [], None, "snowflake-connector-python not installed"
        if not username or not password:
            return [], None, "Snowflake credentials not set"
        try:
            sf = snowflake.connector.connect(
                account=conn.server.replace(".snowflakecomputing.com", ""),
                user=username, password=password,
                database=conn.dbname, schema=schema,
                warehouse=conn.warehouse or "", login_timeout=30,
            )
            cur = sf.cursor()
            cur.execute(_INFO_SCHEMA_COLS.format(
                db=conn.dbname, schema=schema, table=table.upper()))
            cols = [(r[0], r[1], r[2]) for r in cur.fetchall()]
            row_count = None
            if self.enable_row_counts and cols:
                cur.execute(_ROW_COUNT_QUERY.format(schema=schema, table=table))
                row_count = cur.fetchone()[0]
            cur.close(); sf.close()
            return cols, row_count, None
        except Exception as e:
            return [], None, f"Snowflake introspection failed: {e}"

    def _introspect_postgres(
        self, conn: ConnectionConfig, schema: str, table: str,
        username: str, password: str,
    ) -> Tuple[List[Tuple[str, str, str]], Optional[int], Optional[str]]:
        try:
            import psycopg2  # type: ignore
        except ImportError:
            return [], None, "psycopg2-binary not installed"
        if not username or not password:
            return [], None, "PostgreSQL credentials not set"
        try:
            pg = psycopg2.connect(
                host=conn.server, port=conn.port or 5432,
                dbname=conn.dbname, user=username, password=password,
                connect_timeout=30,
            )
            cur = pg.cursor()
            cur.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema=%s AND table_name=%s
                ORDER BY ordinal_position
            """, (schema.lower(), table.lower()))
            cols = [(r[0], r[1].upper(), r[2]) for r in cur.fetchall()]
            row_count = None
            if self.enable_row_counts and cols:
                cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
                row_count = cur.fetchone()[0]
            cur.close(); pg.close()
            return cols, row_count, None
        except Exception as e:
            return [], None, f"PostgreSQL introspection failed: {e}"

    def _introspect_mysql(
        self, conn: ConnectionConfig, schema: str, table: str,
        username: str, password: str,
    ) -> Tuple[List[Tuple[str, str, str]], Optional[int], Optional[str]]:
        try:
            import mysql.connector  # type: ignore
        except ImportError:
            return [], None, "mysql-connector-python not installed"
        if not username or not password:
            return [], None, "MySQL credentials not set"
        try:
            my = mysql.connector.connect(
                host=conn.server, port=conn.port or 3306,
                database=conn.dbname, user=username, password=password,
                connection_timeout=30,
            )
            cur = my.cursor()
            cur.execute("""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
                ORDER BY ORDINAL_POSITION
            """, (schema, table))
            cols = [(r[0], r[1].upper(), r[2]) for r in cur.fetchall()]
            row_count = None
            if self.enable_row_counts and cols:
                cur.execute(f"SELECT COUNT(*) FROM `{schema}`.`{table}`")
                row_count = cur.fetchone()[0]
            cur.close(); my.close()
            return cols, row_count, None
        except Exception as e:
            return [], None, f"MySQL introspection failed: {e}"

    # ------------------------------------------------------------------ #
    def _synthesize_from_csv(
        self,
        tbl: TableConfig,
        ds: DataSourceConfig,
        columns_declared: List[ColumnConfig],
    ) -> TableProfile:
        schema = (tbl.schema_ or ds.default_schema or "PUBLIC").upper()
        tp = TableProfile(
            table_id=tbl.table_id, table_name=tbl.table_name,
            schema=schema, profiled=True, row_count=None,
        )
        for col in columns_declared:
            tp.add_column(ColumnProfile(
                physical_name=col.column_name,
                physical_type=col.datatype.upper(),
                tableau_datatype=col.datatype.lower(),
                declared_name=col.column_name,
                declared_datatype=col.datatype.lower(),
                nullable=True,
            ))
        return tp


# ── Helper ─────────────────────────────────────────────────────────────────────

def _env(var_name: Optional[str]) -> str:
    if not var_name:
        return ""
    return os.environ.get(var_name.strip(), "")
