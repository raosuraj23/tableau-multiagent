# agents/intake_agent.py
"""
IntakeAgent — CSV Input Ingestion and Validation
=================================================

Phase: INTAKE (Phase 01 in the workflow)
LLM: Gemini (semantic interpretation of ambiguous field values)

Responsibilities:
  1. Discover and read all 12 CSV input files from csv_inputs/
  2. Validate structure (required columns, row counts, encoding)
  3. Perform FK integrity checks across files
  4. Normalize data types (boolean strings, empty values, etc.)
  5. Produce a validated ProjectSpec for downstream agents

Output (written to WorkbookState):
  - project_spec: ProjectSpec  ← the canonical input model for all agents
  - intake_report: dict         ← summary of files read, rows, warnings

Failure modes:
  - CRITICAL: missing required CSV file → halt workflow
  - CRITICAL: missing required column in CSV → halt workflow
  - HIGH:     FK integrity violation → halt with detail
  - MEDIUM:   optional file missing (figma_layout.csv) → warn, continue
  - MEDIUM:   unexpected extra columns → warn, continue
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import structlog

from agents.base_agent import (
    AgentResult,
    AgentStatus,
    BaseAgent,
    ErrorSeverity,
    PhaseContext,
)
from models.project_spec import ProjectSpec


logger = structlog.get_logger().bind(agent="intake_agent")


# ── Required CSV schemas ───────────────────────────────────────────────────────
# Maps filename → set of required column names

REQUIRED_CSV_SCHEMAS: Dict[str, List[str]] = {
    "project_config.csv": [
        "project_id", "project_name", "environment",
        "tableau_site", "tableau_server_url", "target_project", "workbook_name",
    ],
    "connections.csv": [
        "connection_id", "class", "server", "dbname", "schema",
        "auth_method", "auth_id",
    ],
    "auth.csv": ["auth_id", "username_env", "password_env"],
    "data_sources.csv": [
        "datasource_id", "datasource_name", "connection_id",
        "datasource_type", "primary_table",
    ],
    "tables.csv": ["table_id", "datasource_id", "table_name"],
    "columns.csv": ["column_id", "table_id", "column_name", "datatype", "role"],
    "relationships.csv": [
        "relationship_id", "datasource_id",
        "left_table_id", "right_table_id", "left_key", "right_key", "join_type",
    ],
    "metrics.csv": [
        "metric_id", "datasource_id", "metric_name", "formula", "datatype",
    ],
    "dimensions.csv": [
        "dimension_id", "datasource_id", "dimension_name",
        "dimension_type", "columns",
    ],
    "dashboard_requirements.csv": [
        "view_id", "view_name", "view_type", "datasource_id",
    ],
}

# Optional files — warn if missing, don't block
OPTIONAL_CSV_FILES: List[str] = [
    "figma_layout.csv",
    "mstr_attributes.csv",
    "mstr_metrics.csv",
]

# FK integrity checks: (child_file, child_col, parent_file, parent_col)
FK_CHECKS: List[Tuple[str, str, str, str]] = [
    ("data_sources.csv",  "connection_id",  "connections.csv",   "connection_id"),
    ("data_sources.csv",  "auth_id",        "auth.csv",          "auth_id"),
    ("tables.csv",        "datasource_id",  "data_sources.csv",  "datasource_id"),
    ("columns.csv",       "table_id",       "tables.csv",        "table_id"),
    ("relationships.csv", "datasource_id",  "data_sources.csv",  "datasource_id"),
    ("relationships.csv", "left_table_id",  "tables.csv",        "table_id"),
    ("relationships.csv", "right_table_id", "tables.csv",        "table_id"),
    ("metrics.csv",       "datasource_id",  "data_sources.csv",  "datasource_id"),
    ("dimensions.csv",    "datasource_id",  "data_sources.csv",  "datasource_id"),
    ("dashboard_requirements.csv", "datasource_id", "data_sources.csv", "datasource_id"),
]


# ── IntakeAgent ────────────────────────────────────────────────────────────────

class IntakeAgent(BaseAgent):
    """
    Phase 01 — Input Intake

    Reads all CSV files from csv_dir, validates schemas and FK integrity,
    and produces a validated ProjectSpec in the workflow state.
    """

    def __init__(
        self,
        csv_dir: Optional[Path] = None,
        config: Optional[Dict[str, Any]] = None,
        context: Optional[PhaseContext] = None,
    ) -> None:
        super().__init__(
            agent_id="intake_agent",
            phase="INTAKE",
            config=config or {},
            context=context,
        )
        self.csv_dir = Path(csv_dir or self.config.get("csv_dir", "csv_inputs"))

    # ── validate_input ─────────────────────────────────────────────────────

    def validate_input(self, state: Dict[str, Any]) -> List[str]:
        """
        Precondition: csv_dir must exist and contain at least project_config.csv.
        Called by execute() before run().
        """
        errors = []
        if not self.csv_dir.exists():
            errors.append(
                f"CSV directory not found: {self.csv_dir.resolve()}. "
                "Run setup.bat first or pass csv_dir to IntakeAgent()."
            )
            return errors  # no point checking files if dir doesn't exist

        if not (self.csv_dir / "project_config.csv").exists():
            errors.append(
                "project_config.csv not found in csv_dir. "
                "Copy from csv_inputs/examples/ as a starting point."
            )
        return errors

    # ── run ────────────────────────────────────────────────────────────────

    def run(self, state: Dict[str, Any]) -> AgentResult:
        """
        Full intake pipeline:
          1. Check all required files present
          2. Validate column schemas
          3. Check FK integrity
          4. Load ProjectSpec via Pydantic
          5. Return validated spec in result.output
        """
        self.log_start()
        result = AgentResult(agent_id=self.agent_id, phase=self.phase)
        intake_report: Dict[str, Any] = {
            "csv_dir":       str(self.csv_dir.resolve()),
            "files_found":   [],
            "files_missing": [],
            "row_counts":    {},
            "fk_violations": [],
            "schema_issues": [],
        }

        # ── Step 1: Check required files ────────────────────────────────

        all_dfs: Dict[str, pd.DataFrame] = {}

        for filename in REQUIRED_CSV_SCHEMAS:
            path = self.csv_dir / filename
            if not path.exists():
                result.add_error(
                    f"Required file missing: {filename}",
                    severity=ErrorSeverity.CRITICAL,
                    field=filename,
                )
                intake_report["files_missing"].append(filename)
            else:
                try:
                    df = pd.read_csv(path, dtype=str).fillna("")
                    all_dfs[filename] = df
                    intake_report["files_found"].append(filename)
                    intake_report["row_counts"][filename] = len(df)
                except Exception as e:
                    result.add_error(
                        f"Cannot parse {filename}: {e}",
                        severity=ErrorSeverity.CRITICAL,
                        field=filename,
                    )

        # Check optional files
        for filename in OPTIONAL_CSV_FILES:
            path = self.csv_dir / filename
            if not path.exists():
                result.add_warning(f"Optional file not found: {filename} — skipping")
                intake_report["files_missing"].append(filename)
            else:
                try:
                    df = pd.read_csv(path, dtype=str).fillna("")
                    all_dfs[filename] = df
                    intake_report["files_found"].append(filename)
                    intake_report["row_counts"][filename] = len(df)
                except Exception as e:
                    result.add_warning(f"Cannot parse optional {filename}: {e}")

        if result.has_blocking_errors:
            result.output = {"intake_report": intake_report}
            return self.log_complete(result)

        # ── Step 2: Validate column schemas ─────────────────────────────

        for filename, required_cols in REQUIRED_CSV_SCHEMAS.items():
            if filename not in all_dfs:
                continue
            df = all_dfs[filename]
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                result.add_error(
                    f"{filename} missing required columns: {missing_cols}",
                    severity=ErrorSeverity.CRITICAL,
                    field=filename,
                )
                intake_report["schema_issues"].append({
                    "file": filename,
                    "missing_columns": missing_cols,
                })

            # Warn about empty required files
            if len(df) == 0:
                result.add_error(
                    f"{filename} has 0 rows — at least 1 row required",
                    severity=ErrorSeverity.CRITICAL,
                    field=filename,
                )

        # Validate project_config.csv has exactly 1 row
        if "project_config.csv" in all_dfs and len(all_dfs["project_config.csv"]) != 1:
            result.add_error(
                f"project_config.csv must have exactly 1 row, "
                f"found {len(all_dfs['project_config.csv'])}",
                severity=ErrorSeverity.CRITICAL,
                field="project_config.csv",
            )

        if result.has_blocking_errors:
            result.output = {"intake_report": intake_report}
            return self.log_complete(result)

        # ── Step 3: FK integrity checks ──────────────────────────────────

        for child_file, child_col, parent_file, parent_col in FK_CHECKS:
            if child_file not in all_dfs or parent_file not in all_dfs:
                continue

            child_df  = all_dfs[child_file]
            parent_df = all_dfs[parent_file]

            # Skip if columns don't exist (already caught in schema check)
            if child_col not in child_df.columns or parent_col not in parent_df.columns:
                continue

            child_vals  = set(child_df[child_col].str.strip().unique()) - {""}
            parent_vals = set(parent_df[parent_col].str.strip().unique()) - {""}
            bad_vals    = child_vals - parent_vals

            if bad_vals:
                msg = (
                    f"FK violation: {child_file}.{child_col} → "
                    f"{parent_file}.{parent_col} — "
                    f"unmatched values: {sorted(bad_vals)}"
                )
                result.add_error(msg, severity=ErrorSeverity.HIGH,
                                 field=f"{child_file}.{child_col}")
                intake_report["fk_violations"].append({
                    "child":  f"{child_file}.{child_col}",
                    "parent": f"{parent_file}.{parent_col}",
                    "bad_values": sorted(bad_vals),
                })

        if result.has_blocking_errors:
            result.output = {"intake_report": intake_report}
            return self.log_complete(result)

        # ── Step 4: Business rule validations ────────────────────────────

        # At least one data source must be marked primary
        ds_df = all_dfs["data_sources.csv"]
        primary_count = ds_df["is_primary"].str.lower().isin(["true", "1", "yes"]).sum() \
            if "is_primary" in ds_df.columns else 0
        if primary_count == 0:
            result.add_warning(
                "No primary data source marked in data_sources.csv "
                "(is_primary=true). First row will be treated as primary."
            )
        elif primary_count > 1:
            result.add_warning(
                f"{primary_count} data sources marked as primary. "
                "Only the first will be used."
            )

        # Validate metric formulas are non-empty
        if "metrics.csv" in all_dfs:
            empty_formulas = all_dfs["metrics.csv"][
                all_dfs["metrics.csv"]["formula"].str.strip() == ""
            ]
            if len(empty_formulas) > 0:
                ids = list(empty_formulas["metric_id"])
                result.add_error(
                    f"Metrics with empty formula: {ids}",
                    severity=ErrorSeverity.HIGH,
                    field="metrics.csv",
                )

        # Validate dashboard views_in_dashboard reference valid view_ids
        if "dashboard_requirements.csv" in all_dfs:
            req_df     = all_dfs["dashboard_requirements.csv"]
            all_view_ids = set(req_df["view_id"].str.strip().unique())
            dashboards = req_df[req_df["view_type"].str.lower() == "dashboard"]
            for _, dash_row in dashboards.iterrows():
                views_str = str(dash_row.get("views_in_dashboard", "")).strip()
                if views_str:
                    referenced = {v.strip() for v in views_str.split("|") if v.strip()}
                    bad = referenced - all_view_ids
                    if bad:
                        result.add_error(
                            f"Dashboard '{dash_row['view_id']}' references "
                            f"non-existent view_ids: {sorted(bad)}",
                            severity=ErrorSeverity.HIGH,
                            field="dashboard_requirements.csv",
                        )

        if result.has_blocking_errors:
            result.output = {"intake_report": intake_report}
            return self.log_complete(result)

        # ── Step 5: Load ProjectSpec via Pydantic ────────────────────────

        try:
            project_spec = ProjectSpec.from_csv_dir(self.csv_dir)
        except Exception as e:
            result.add_error(
                f"ProjectSpec construction failed: {e}",
                severity=ErrorSeverity.CRITICAL,
                exc=e,
            )
            result.output = {"intake_report": intake_report}
            return self.log_complete(result)

        # ── Step 6: Finalize ─────────────────────────────────────────────

        intake_report.update(project_spec.summary())
        intake_report["validation_passed"] = True

        result.output = {
            "project_spec":  project_spec.model_dump(),
            "intake_report": intake_report,
        }
        result.metadata["spec_summary"] = project_spec.summary()

        if not result.has_blocking_errors and result.status == AgentStatus.PENDING:
            result.status = AgentStatus.SUCCESS

        self.logger.info(
            "intake_complete",
            **project_spec.summary(),
        )

        return self.log_complete(result)
