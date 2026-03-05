# agents/validation_agent.py
"""
ValidationAgent — Metadata Validation (Phase 02)
=================================================

Reads the ProjectSpec produced by IntakeAgent and applies 6 categories
of validation rules. Produces a ValidationReport that gates Phase 03.

Rule categories:
  CAT-1  Schema validation        Field types, formats, allowed enum values
  CAT-2  Completeness checks      Required fields populated for each entity type
  CAT-3  Business rules           Tableau-specific constraints (formula syntax, etc.)
  CAT-4  Cross-file consistency   Relationships reference real columns, etc.
  CAT-5  Performance warnings     Large column counts, complex LOD chains
  CAT-6  Security checks          Credential env vars referenced but not set

Severity model (mirrors test framework Section 7):
  CRITICAL → blocks Phase 03 (orchestrator will not proceed)
  HIGH     → blocks with human override
  MEDIUM   → warning only, workflow continues
  INFO     → informational

Output (written to WorkbookState):
  - validation_report: ValidationReport.to_dict()

Gate condition for Phase 03:
  validation_report["can_proceed"] == True
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional

import structlog

from agents.base_agent import (
    AgentResult,
    AgentStatus,
    BaseAgent,
    ErrorSeverity,
    PhaseContext,
)
from models.project_spec import (
    DashboardRequirement,
    MetricConfig,
    ProjectSpec,
)
from models.validation_report import (
    FindingCategory,
    FindingSeverity,
    ValidationReport,
)


logger = structlog.get_logger().bind(agent="validation_agent")

# ── Constants ──────────────────────────────────────────────────────────────────

# Tableau chart types that are valid mark classes
VALID_CHART_TYPES = {
    "Bar", "Line", "Area", "Circle", "Square", "Automatic",
    "Text", "Pie", "Shape", "Map", "Scatter", "Gantt",
}

# Valid Tableau field data types
VALID_DATATYPES = {"string", "integer", "real", "date", "datetime", "boolean"}

# Valid Tableau field roles
VALID_ROLES = {"dimension", "measure"}

# Valid dashboard layout templates
VALID_LAYOUTS = {
    "grid-2x2", "grid-2x3", "grid-3x2", "grid-3x3",
    "horizontal", "vertical", "floating", "blank",
}

# Tableau formula function names for basic syntax checking
TABLEAU_AGGREGATIONS = {"SUM", "AVG", "COUNT", "COUNTD", "MIN", "MAX",
                        "MEDIAN", "STDEV", "VAR", "ATTR", "TOTAL"}
TABLEAU_LOD_KEYWORDS = {"FIXED", "INCLUDE", "EXCLUDE"}
TABLEAU_TABLE_CALCS   = {"RUNNING_SUM", "RUNNING_AVG", "WINDOW_SUM",
                         "WINDOW_AVG", "RANK", "RANK_DENSE", "FIRST", "LAST",
                         "INDEX", "SIZE"}

# Performance thresholds
MAX_COLUMNS_PER_DATASOURCE  = 200    # warn above this
MAX_METRICS_PER_DATASOURCE  = 50     # warn above this
MAX_WORKSHEETS_PER_DASHBOARD = 16    # warn above this


# ── ValidationAgent ────────────────────────────────────────────────────────────

class ValidationAgent(BaseAgent):
    """
    Phase 02 — Metadata Validation

    Validates the ProjectSpec from IntakeAgent using 6 rule categories.
    Produces a ValidationReport that the orchestrator uses to gate Phase 03.
    """

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        context: Optional[PhaseContext] = None,
    ) -> None:
        super().__init__(
            agent_id="validation_agent",
            phase="VALIDATING",
            config=config or {},
            context=context,
        )

    # ── validate_input ─────────────────────────────────────────────────────

    def validate_input(self, state: Dict[str, Any]) -> List[str]:
        """Require project_spec from IntakeAgent output."""
        if not state.get("project_spec"):
            return [
                "project_spec not found in state. "
                "Run IntakeAgent (Phase 01) before ValidationAgent."
            ]
        return []

    # ── run ────────────────────────────────────────────────────────────────

    def run(self, state: Dict[str, Any]) -> AgentResult:
        self.log_start()
        result = AgentResult(agent_id=self.agent_id, phase=self.phase)

        # Reconstruct ProjectSpec from state dict — catch Pydantic validation errors
        # and surface them as CRITICAL findings so the report is still produced.
        spec = None
        try:
            spec = ProjectSpec.model_validate(state["project_spec"])
        except Exception as e:
            # Pydantic ValidationError or other construction error
            report = ValidationReport(
                project_id=state["project_spec"].get("project_config", {}).get(
                    "project_id", ""
                ),
                run_id=self.context.run_id if self.context else "",
            )
            # Try to extract per-field errors from Pydantic's error list
            try:
                from pydantic import ValidationError as PydanticValidationError
                if isinstance(e, PydanticValidationError):
                    for err in e.errors():
                        loc  = " → ".join(str(x) for x in err["loc"])
                        msg  = err["msg"]
                        val  = str(err.get("input", ""))
                        report.add_critical(
                            "project_spec",
                            f"Schema error at {loc}: {msg} (value: {val!r})",
                            rule="CAT1.pydantic_schema",
                            field=loc,
                            value=val,
                        )
                else:
                    report.add_critical(
                        "project_spec",
                        f"Cannot reconstruct ProjectSpec from state: {e}",
                        rule="CAT1.pydantic_schema",
                    )
            except Exception:
                report.add_critical(
                    "project_spec",
                    f"Cannot reconstruct ProjectSpec from state: {e}",
                    rule="CAT1.pydantic_schema",
                )

            report.rules_run = ["CAT1"]  # partial run
            result.output = {"validation_report": report.to_dict()}
            result.metadata["validation_summary"] = report.summary()
            # Add to agent result so orchestrator sees it
            result.add_error(
                f"ProjectSpec construction failed: {e}",
                severity=ErrorSeverity.CRITICAL,
                exc=e,
            )
            return self.log_complete(result)

        # Build ValidationReport
        report = ValidationReport(
            project_id=spec.project_config.project_id,
            run_id=self.context.run_id if self.context else "",
        )

        # Run all 6 rule categories
        self._cat1_schema_validation(spec, report)
        self._cat2_completeness_checks(spec, report)
        self._cat3_business_rules(spec, report)
        self._cat4_cross_file_consistency(spec, report)
        self._cat5_performance_warnings(spec, report)
        self._cat6_security_checks(spec, report)

        # Write output
        result.output = {"validation_report": report.to_dict()}
        result.metadata["validation_summary"] = report.summary()

        # Set agent status based on report
        if report.can_proceed:
            result.status = (AgentStatus.WARNING
                             if report.warning_count > 0
                             else AgentStatus.SUCCESS)
        else:
            # Add blocking findings as agent errors for orchestrator visibility
            for f in report.findings:
                if f.is_blocking:
                    result.add_error(
                        f"[{f.rule}] {f.source}: {f.message}",
                        severity=(ErrorSeverity.CRITICAL
                                  if f.severity == FindingSeverity.CRITICAL
                                  else ErrorSeverity.HIGH),
                        field=f.field,
                    )

        self.logger.info(
            "validation_complete",
            **report.summary(),
        )

        return self.log_complete(result)

    # ══════════════════════════════════════════════════════════════════════
    # CAT-1: Schema validation
    # ══════════════════════════════════════════════════════════════════════

    def _cat1_schema_validation(
        self, spec: ProjectSpec, report: ValidationReport
    ) -> None:
        """Validate field types, formats, and allowed enum values."""
        rule_base = "CAT1"
        report.rules_run.append(rule_base)

        # project_config: environment values
        env = spec.project_config.environment
        if env not in {"dev", "staging", "prod"}:
            report.add_critical(
                "project_config.csv", f"Invalid environment '{env}'",
                rule=f"{rule_base}.environment",
                field="environment",
                suggestion="Set environment to: dev, staging, or prod",
            )

        # project_config: URL format — must use https for Tableau Cloud
        url = spec.project_config.tableau_server_url
        if url.startswith("http://"):
            report.add_finding(
                "project_config.csv",
                f"tableau_server_url uses http:// instead of https://: '{url}' — "
                f"Tableau Cloud requires HTTPS",
                severity=FindingSeverity.HIGH,
                rule=f"{rule_base}.server_url",
                field="tableau_server_url",
                suggestion=url.replace("http://", "https://", 1),
            )
        elif not url.startswith("https://"):
            report.add_finding(
                "project_config.csv",
                f"tableau_server_url should start with https://: '{url}'",
                severity=FindingSeverity.HIGH,
                rule=f"{rule_base}.server_url",
                field="tableau_server_url",
                suggestion=f"https://{url}",
            )

        # connections: class values
        valid_classes = {"snowflake", "postgres", "mysql", "excel-direct",
                         "redshift", "bigquery", "sqlserver", "oracle"}
        for conn in spec.connections:
            cls = conn.class_.lower()
            if cls not in valid_classes:
                report.add_finding(
                    "connections.csv",
                    f"Connection '{conn.connection_id}' has unknown class '{conn.class_}'",
                    severity=FindingSeverity.HIGH,
                    rule=f"{rule_base}.connection_class",
                    field="class",
                    value=conn.class_,
                    suggestion=f"Use one of: {sorted(valid_classes)}",
                )

        # columns: datatype values
        for col in spec.columns:
            if col.datatype not in VALID_DATATYPES:
                report.add_critical(
                    "columns.csv",
                    f"Column '{col.column_id}' has invalid datatype '{col.datatype}'",
                    rule=f"{rule_base}.column_datatype",
                    field="datatype",
                    value=col.datatype,
                    suggestion=f"Use one of: {sorted(VALID_DATATYPES)}",
                )

        # columns: role values
        for col in spec.columns:
            if col.role not in VALID_ROLES:
                report.add_critical(
                    "columns.csv",
                    f"Column '{col.column_id}' has invalid role '{col.role}'",
                    rule=f"{rule_base}.column_role",
                    field="role",
                    value=col.role,
                )

        # dashboard_requirements: chart_type values (worksheets only)
        for req in spec.get_worksheets():
            if req.chart_type and req.chart_type not in VALID_CHART_TYPES:
                report.add_finding(
                    "dashboard_requirements.csv",
                    f"Worksheet '{req.view_id}' has unknown chart_type '{req.chart_type}'",
                    severity=FindingSeverity.HIGH,
                    rule=f"{rule_base}.chart_type",
                    field="chart_type",
                    value=req.chart_type,
                    suggestion=f"Use one of: {sorted(VALID_CHART_TYPES)}",
                )

        # dashboard_requirements: sort_direction
        for req in spec.dashboard_requirements:
            if req.sort_direction and req.sort_direction.lower() not in {"asc", "desc"}:
                report.add_warning(
                    "dashboard_requirements.csv",
                    f"View '{req.view_id}' has invalid sort_direction '{req.sort_direction}'",
                    rule=f"{rule_base}.sort_direction",
                    field="sort_direction",
                )

        # metrics: datatype
        for m in spec.metrics:
            if m.datatype not in VALID_DATATYPES:
                report.add_critical(
                    "metrics.csv",
                    f"Metric '{m.metric_id}' has invalid datatype '{m.datatype}'",
                    rule=f"{rule_base}.metric_datatype",
                    field="datatype",
                    value=m.datatype,
                )

    # ══════════════════════════════════════════════════════════════════════
    # CAT-2: Completeness checks
    # ══════════════════════════════════════════════════════════════════════

    def _cat2_completeness_checks(
        self, spec: ProjectSpec, report: ValidationReport
    ) -> None:
        """Ensure required fields are populated for each entity."""
        rule_base = "CAT2"
        report.rules_run.append(rule_base)

        # Worksheets must have at least rows OR columns populated
        for req in spec.get_worksheets():
            if not req.row_fields and not req.column_fields:
                report.add_finding(
                    "dashboard_requirements.csv",
                    f"Worksheet '{req.view_id}' ({req.view_name}) has no rows or columns "
                    f"defined — it will render as an empty view",
                    severity=FindingSeverity.HIGH,
                    rule=f"{rule_base}.worksheet_shelves",
                    field="rows/columns",
                    suggestion="Populate at least one of: rows, columns",
                )

        # Worksheets must have chart_type
        for req in spec.get_worksheets():
            if not req.chart_type:
                report.add_finding(
                    "dashboard_requirements.csv",
                    f"Worksheet '{req.view_id}' ({req.view_name}) has no chart_type",
                    severity=FindingSeverity.HIGH,
                    rule=f"{rule_base}.worksheet_chart_type",
                    field="chart_type",
                    suggestion=f"Set chart_type to one of: {sorted(VALID_CHART_TYPES)}",
                )

        # Dashboards must have views_in_dashboard
        for dash in spec.get_dashboards():
            if not dash.dashboard_view_ids:
                report.add_finding(
                    "dashboard_requirements.csv",
                    f"Dashboard '{dash.view_id}' ({dash.view_name}) has no worksheets "
                    f"assigned in views_in_dashboard",
                    severity=FindingSeverity.HIGH,
                    rule=f"{rule_base}.dashboard_views",
                    field="views_in_dashboard",
                    suggestion="List worksheet view_ids separated by | in views_in_dashboard",
                )

        # Pie charts require color encoding
        for req in spec.get_worksheets():
            if req.chart_type == "Pie" and not req.color:
                report.add_warning(
                    "dashboard_requirements.csv",
                    f"Pie chart '{req.view_id}' has no color encoding — "
                    f"Tableau requires color for Pie mark type",
                    rule=f"{rule_base}.pie_color",
                    field="color",
                )

        # Connections must have warehouse set for Snowflake
        for conn in spec.connections:
            if conn.class_.lower() == "snowflake" and not conn.warehouse:
                report.add_finding(
                    "connections.csv",
                    f"Snowflake connection '{conn.connection_id}' has no warehouse specified",
                    severity=FindingSeverity.HIGH,
                    rule=f"{rule_base}.snowflake_warehouse",
                    field="warehouse",
                    suggestion="Set warehouse to your Snowflake virtual warehouse name",
                )

        # Published datasources must have published_ds_name
        for ds in spec.data_sources:
            if ds.datasource_type == "published" and not ds.published_ds_name:
                report.add_critical(
                    "data_sources.csv",
                    f"Published datasource '{ds.datasource_id}' has no published_ds_name",
                    rule=f"{rule_base}.published_ds_name",
                    field="published_ds_name",
                    suggestion="Set published_ds_name to the Tableau Cloud datasource name",
                )

        # Auth records must have username_env and password_env set
        for auth in spec.auth_configs:
            if not auth.username_env:
                report.add_critical(
                    "auth.csv",
                    f"Auth record '{auth.auth_id}' has no username_env",
                    rule=f"{rule_base}.auth_username_env",
                    field="username_env",
                )
            if not auth.password_env:
                report.add_critical(
                    "auth.csv",
                    f"Auth record '{auth.auth_id}' has no password_env",
                    rule=f"{rule_base}.auth_password_env",
                    field="password_env",
                )

    # ══════════════════════════════════════════════════════════════════════
    # CAT-3: Business rules (Tableau-specific)
    # ══════════════════════════════════════════════════════════════════════

    def _cat3_business_rules(
        self, spec: ProjectSpec, report: ValidationReport
    ) -> None:
        """Tableau-specific constraint validation."""
        rule_base = "CAT3"
        report.rules_run.append(rule_base)

        for m in spec.metrics:
            self._validate_metric_formula(m, report, rule_base)

        # LOD metrics: flag the is_lod marker is consistent with formula content
        for m in spec.metrics:
            has_lod_syntax = bool(re.search(r'\{\s*(FIXED|INCLUDE|EXCLUDE)', m.formula,
                                             re.IGNORECASE))
            if m.is_lod and not has_lod_syntax:
                report.add_warning(
                    "metrics.csv",
                    f"Metric '{m.metric_id}' is marked is_lod=true but formula "
                    f"has no LOD syntax (FIXED/INCLUDE/EXCLUDE): {m.formula}",
                    rule=f"{rule_base}.lod_marker_consistency",
                    field="is_lod",
                )
            if not m.is_lod and has_lod_syntax:
                report.add_warning(
                    "metrics.csv",
                    f"Metric '{m.metric_id}' has LOD syntax but is_lod=false: {m.formula}",
                    rule=f"{rule_base}.lod_marker_consistency",
                    field="is_lod",
                )

        # Dashboard dimensions: validate height and width are positive
        for dash in spec.get_dashboards():
            if dash.width_px <= 0:
                report.add_finding(
                    "dashboard_requirements.csv",
                    f"Dashboard '{dash.view_id}' has invalid width_px={dash.width_px}",
                    severity=FindingSeverity.HIGH,
                    rule=f"{rule_base}.dashboard_dimensions",
                    field="width_px",
                )
            if dash.height_px <= 0:
                report.add_finding(
                    "dashboard_requirements.csv",
                    f"Dashboard '{dash.view_id}' has invalid height_px={dash.height_px}",
                    severity=FindingSeverity.HIGH,
                    rule=f"{rule_base}.dashboard_dimensions",
                    field="height_px",
                )

        # Tableau version must be a known value
        version = spec.project_config.tableau_version
        if version not in {"18.1", "18.0", "17.4", "17.3"}:
            report.add_warning(
                "project_config.csv",
                f"tableau_version '{version}' is not a recognized schema version. "
                f"Recommended: 18.1",
                rule=f"{rule_base}.tableau_version",
                field="tableau_version",
            )

        # Workbook name should not contain spaces (Tableau Cloud may reject)
        name = spec.project_config.workbook_name
        if " " in name:
            report.add_warning(
                "project_config.csv",
                f"workbook_name '{name}' contains spaces — use underscores for "
                f"Tableau Cloud compatibility",
                rule=f"{rule_base}.workbook_name_spaces",
                field="workbook_name",
                suggestion=name.replace(" ", "_"),
            )

        # Dimension columns should not have aggregation set
        for col in spec.columns:
            if col.role == "dimension" and col.aggregation:
                report.add_warning(
                    "columns.csv",
                    f"Column '{col.column_id}' is a dimension but has aggregation "
                    f"'{col.aggregation}' — dimensions are not aggregated in Tableau",
                    rule=f"{rule_base}.dimension_aggregation",
                    field="aggregation",
                )

    def _validate_metric_formula(
        self,
        m: MetricConfig,
        report: ValidationReport,
        rule_base: str,
    ) -> None:
        """Validate a single metric formula for common Tableau syntax issues."""
        formula = m.formula.strip()

        # Empty formula (already caught by intake, but double-check)
        if not formula:
            report.add_critical(
                "metrics.csv",
                f"Metric '{m.metric_id}' ({m.metric_name}) has empty formula",
                rule=f"{rule_base}.formula_empty",
                field="formula",
            )
            return

        # Check bracket balance: [ must match ]
        if formula.count("[") != formula.count("]"):
            report.add_critical(
                "metrics.csv",
                f"Metric '{m.metric_id}' formula has unbalanced brackets: {formula}",
                rule=f"{rule_base}.formula_brackets",
                field="formula",
                value=formula,
                suggestion="Ensure every [ has a matching ]",
            )

        # Check paren balance
        if formula.count("(") != formula.count(")"):
            report.add_critical(
                "metrics.csv",
                f"Metric '{m.metric_id}' formula has unbalanced parentheses: {formula}",
                rule=f"{rule_base}.formula_parens",
                field="formula",
                value=formula,
            )

        # LOD: must have colon between scope and expression
        if re.search(r'\{\s*(FIXED|INCLUDE|EXCLUDE)', formula, re.IGNORECASE):
            if ":" not in formula:
                report.add_critical(
                    "metrics.csv",
                    f"Metric '{m.metric_id}' LOD expression missing ':' separator: {formula}",
                    rule=f"{rule_base}.lod_colon",
                    field="formula",
                    suggestion="Format: { FIXED [Dim] : SUM([Measure]) }",
                )
            # LOD must have closing brace
            if formula.count("{") != formula.count("}"):
                report.add_critical(
                    "metrics.csv",
                    f"Metric '{m.metric_id}' LOD expression has unbalanced braces: {formula}",
                    rule=f"{rule_base}.lod_braces",
                    field="formula",
                )

        # Division by zero risk: SUM([X]) / SUM([Y]) — warn if no NULLIF/ZN
        if "/" in formula and not re.search(r'nullif|zn\s*\(', formula, re.IGNORECASE):
            report.add_info(
                "metrics.csv",
                f"Metric '{m.metric_id}' uses division — consider wrapping denominator "
                f"with ZN() or NULLIF() to handle division by zero: {formula}",
                rule=f"{rule_base}.division_by_zero",
            )

    # ══════════════════════════════════════════════════════════════════════
    # CAT-4: Cross-file consistency
    # ══════════════════════════════════════════════════════════════════════

    def _cat4_cross_file_consistency(
        self, spec: ProjectSpec, report: ValidationReport
    ) -> None:
        """Check consistency between related entities across CSV files."""
        rule_base = "CAT4"
        report.rules_run.append(rule_base)

        # Relationship left_key and right_key must exist in their respective tables
        all_column_names_by_table: Dict[str, set] = {}
        for col in spec.columns:
            all_column_names_by_table.setdefault(col.table_id, set()).add(
                col.column_name.upper()
            )

        for rel in spec.relationships:
            left_cols  = all_column_names_by_table.get(rel.left_table_id,  set())
            right_cols = all_column_names_by_table.get(rel.right_table_id, set())

            if left_cols and rel.left_key.upper() not in left_cols:
                report.add_finding(
                    "relationships.csv",
                    f"Relationship '{rel.relationship_id}': left_key '{rel.left_key}' "
                    f"not found in table '{rel.left_table_id}' columns",
                    severity=FindingSeverity.HIGH,
                    rule=f"{rule_base}.join_key_exists",
                    field="left_key",
                    value=rel.left_key,
                    suggestion=f"Available columns: {sorted(left_cols)}",
                )
            if right_cols and rel.right_key.upper() not in right_cols:
                report.add_finding(
                    "relationships.csv",
                    f"Relationship '{rel.relationship_id}': right_key '{rel.right_key}' "
                    f"not found in table '{rel.right_table_id}' columns",
                    severity=FindingSeverity.HIGH,
                    rule=f"{rule_base}.join_key_exists",
                    field="right_key",
                    value=rel.right_key,
                    suggestion=f"Available columns: {sorted(right_cols)}",
                )

        # Dimension column_ids must all exist in columns.csv
        all_column_ids = {c.column_id for c in spec.columns}
        for dim in spec.dimensions:
            for cid in dim.column_ids:
                if cid not in all_column_ids:
                    report.add_finding(
                        "dimensions.csv",
                        f"Dimension '{dim.dimension_id}' references unknown column_id "
                        f"'{cid}' in columns field",
                        severity=FindingSeverity.HIGH,
                        rule=f"{rule_base}.dimension_column_ids",
                        field="columns",
                        value=cid,
                    )

        # Metric field references: [FieldName] in formula should exist in columns
        # (best-effort — we check for names wrapped in [] that look like column refs)
        col_names_upper = {c.column_name.upper() for c in spec.columns}
        metric_names_upper = {m.metric_name.upper() for m in spec.metrics}

        for m in spec.metrics:
            refs = re.findall(r'\[([^\]]+)\]', m.formula)
            for ref in refs:
                ref_upper = ref.strip().upper()
                # Skip date parts and functions (e.g. [MONTH], [Order Date])
                if ref_upper in col_names_upper or ref_upper in metric_names_upper:
                    continue
                # Skip known Tableau keywords used as field references
                skip = {"MONTH", "YEAR", "QUARTER", "WEEK", "DAY",
                        "DATEPART", "TODAY", "NOW"}
                if ref_upper in skip:
                    continue
                report.add_info(
                    "metrics.csv",
                    f"Metric '{m.metric_id}' references '[{ref}]' — "
                    f"not found in columns.csv. Verify this field exists in the datasource.",
                    rule=f"{rule_base}.metric_field_refs",
                )

        # Dashboard layout template consistency
        for dash in spec.get_dashboards():
            if dash.dashboard_layout and dash.dashboard_layout not in VALID_LAYOUTS:
                report.add_warning(
                    "dashboard_requirements.csv",
                    f"Dashboard '{dash.view_id}' uses unknown layout "
                    f"'{dash.dashboard_layout}'",
                    rule=f"{rule_base}.dashboard_layout",
                    field="dashboard_layout",
                )

    # ══════════════════════════════════════════════════════════════════════
    # CAT-5: Performance warnings
    # ══════════════════════════════════════════════════════════════════════

    def _cat5_performance_warnings(
        self, spec: ProjectSpec, report: ValidationReport
    ) -> None:
        """Flag configurations that may cause performance issues."""
        rule_base = "CAT5"
        report.rules_run.append(rule_base)

        # Column count per datasource
        for ds in spec.data_sources:
            cols = sum(
                len(spec.get_columns_for_table(t.table_id))
                for t in spec.get_tables_for_datasource(ds.datasource_id)
            )
            if cols > MAX_COLUMNS_PER_DATASOURCE:
                report.add_warning(
                    "columns.csv",
                    f"Datasource '{ds.datasource_id}' has {cols} columns — "
                    f"consider hiding unused columns to improve performance "
                    f"(threshold: {MAX_COLUMNS_PER_DATASOURCE})",
                    rule=f"{rule_base}.column_count",
                )

        # Metric count per datasource
        for ds in spec.data_sources:
            m_count = len(spec.get_metrics_for_datasource(ds.datasource_id))
            if m_count > MAX_METRICS_PER_DATASOURCE:
                report.add_warning(
                    "metrics.csv",
                    f"Datasource '{ds.datasource_id}' has {m_count} calculated fields — "
                    f"many calculated fields can slow query performance "
                    f"(threshold: {MAX_METRICS_PER_DATASOURCE})",
                    rule=f"{rule_base}.metric_count",
                )

        # Worksheets per dashboard
        for dash in spec.get_dashboards():
            ws_count = len(dash.dashboard_view_ids)
            if ws_count > MAX_WORKSHEETS_PER_DASHBOARD:
                report.add_warning(
                    "dashboard_requirements.csv",
                    f"Dashboard '{dash.view_id}' has {ws_count} worksheets — "
                    f"dashboards with many sheets may load slowly "
                    f"(threshold: {MAX_WORKSHEETS_PER_DASHBOARD})",
                    rule=f"{rule_base}.worksheets_per_dashboard",
                )

        # Nested LOD expressions (LOD within LOD) — high performance cost
        for m in spec.metrics:
            lod_matches = re.findall(
                r'\{\s*(FIXED|INCLUDE|EXCLUDE)', m.formula, re.IGNORECASE
            )
            if len(lod_matches) > 1:
                report.add_warning(
                    "metrics.csv",
                    f"Metric '{m.metric_id}' ({m.metric_name}) contains {len(lod_matches)} "
                    f"LOD expressions — nested LODs can significantly impact query performance",
                    rule=f"{rule_base}.nested_lod",
                )

        # Multiple full-outer joins (expensive)
        full_joins = [r for r in spec.relationships
                      if r.join_type.lower() == "full"]
        if len(full_joins) > 2:
            report.add_warning(
                "relationships.csv",
                f"{len(full_joins)} FULL OUTER JOINs detected — full outer joins "
                f"are expensive; verify each is necessary",
                rule=f"{rule_base}.full_outer_joins",
            )

    # ══════════════════════════════════════════════════════════════════════
    # CAT-6: Security checks
    # ══════════════════════════════════════════════════════════════════════

    def _cat6_security_checks(
        self, spec: ProjectSpec, report: ValidationReport
    ) -> None:
        """Validate credential references and flag security concerns."""
        rule_base = "CAT6"
        report.rules_run.append(rule_base)

        # Check that env vars referenced in auth.csv are actually set
        for auth in spec.auth_configs:
            for env_var_name, field_name in [
                (auth.username_env,    "username_env"),
                (auth.password_env,    "password_env"),
                (auth.pat_name_env,    "pat_name_env"),
                (auth.pat_secret_env,  "pat_secret_env"),
                (auth.oauth_token_env, "oauth_token_env"),
            ]:
                if env_var_name and env_var_name.strip():
                    val = os.environ.get(env_var_name.strip(), "")
                    if not val:
                        report.add_warning(
                            "auth.csv",
                            f"Auth '{auth.auth_id}': env var '{env_var_name}' "
                            f"({field_name}) is not set in the environment",
                            rule=f"{rule_base}.env_var_not_set",
                            field=field_name,
                        )

        # Check Tableau PAT env vars if they're referenced in any auth record
        tab_pat_records = [a for a in spec.auth_configs
                           if a.pat_name_env or a.pat_secret_env]
        if tab_pat_records:
            for auth in tab_pat_records:
                if auth.pat_name_env and not os.environ.get(auth.pat_name_env, ""):
                    report.add_warning(
                        "auth.csv",
                        f"Tableau PAT name env var '{auth.pat_name_env}' not set — "
                        f"Tableau Cloud publishing will fail",
                        rule=f"{rule_base}.tableau_pat_not_set",
                        field="pat_name_env",
                    )
                if auth.pat_secret_env and not os.environ.get(auth.pat_secret_env, ""):
                    report.add_warning(
                        "auth.csv",
                        f"Tableau PAT secret env var '{auth.pat_secret_env}' not set — "
                        f"Tableau Cloud publishing will fail",
                        rule=f"{rule_base}.tableau_pat_not_set",
                        field="pat_secret_env",
                    )

        # Warn if any connection uses username/password but no auth record has password_env
        for conn in spec.connections:
            if conn.auth_method == "Username Password":
                auth = next(
                    (a for a in spec.auth_configs if a.auth_id == conn.auth_id), None
                )
                if auth and not auth.password_env:
                    report.add_finding(
                        "auth.csv",
                        f"Connection '{conn.connection_id}' uses Username Password auth "
                        f"but auth record '{conn.auth_id}' has no password_env",
                        severity=FindingSeverity.HIGH,
                        rule=f"{rule_base}.password_env_required",
                        field="password_env",
                    )

        # Info: remind about credential rotation
        report.add_info(
            "auth.csv",
            "Security reminder: Tableau PAT tokens should be rotated every 90 days. "
            "Snowflake service account passwords should follow your org's rotation policy.",
            rule=f"{rule_base}.credential_rotation_reminder",
        )
