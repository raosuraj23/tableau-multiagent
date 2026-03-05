# agents/validation_agent.py
"""
Agent #03 — Metadata Validation Agent
Phase 02: Metadata Validation

Validates all CSV input files against JSON Schema rules, checks referential
integrity across CSV files, enforces data-type contracts, and emits a
structured ValidationReport with CRITICAL / WARNING / INFO severities.

Failure policy:
  CRITICAL  → blocks DAG progression (0 allowed to advance)
  WARNING   → logged, workflow continues
  INFO      → informational annotations only
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import structlog

from agents.base_agent import AgentResult, BaseAgent

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# Domain types
# ---------------------------------------------------------------------------


class Severity(str, Enum):
    CRITICAL = "CRITICAL"
    WARNING = "WARNING"
    INFO = "INFO"


@dataclass
class ValidationIssue:
    severity: Severity
    rule: str
    file: str
    column: Optional[str]
    row_index: Optional[int]
    message: str
    value: Optional[Any] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "severity": self.severity.value,
            "rule": self.rule,
            "file": self.file,
            "column": self.column,
            "row_index": self.row_index,
            "message": self.message,
            "value": str(self.value) if self.value is not None else None,
        }


@dataclass
class ValidationReport:
    project_id: str
    issues: List[ValidationIssue] = field(default_factory=list)
    files_checked: List[str] = field(default_factory=list)
    duration_ms: float = 0.0

    # ── convenience accessors ──────────────────────────────────────────────
    @property
    def criticals(self) -> List[ValidationIssue]:
        return [i for i in self.issues if i.severity == Severity.CRITICAL]

    @property
    def warnings(self) -> List[ValidationIssue]:
        return [i for i in self.issues if i.severity == Severity.WARNING]

    @property
    def infos(self) -> List[ValidationIssue]:
        return [i for i in self.issues if i.severity == Severity.INFO]

    @property
    def is_valid(self) -> bool:
        """True only when zero CRITICAL issues exist."""
        return len(self.criticals) == 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "project_id": self.project_id,
            "is_valid": self.is_valid,
            "summary": {
                "critical": len(self.criticals),
                "warning": len(self.warnings),
                "info": len(self.infos),
                "total": len(self.issues),
            },
            "files_checked": self.files_checked,
            "duration_ms": round(self.duration_ms, 2),
            "issues": [i.to_dict() for i in self.issues],
        }


# ---------------------------------------------------------------------------
# Per-file schemas: (column_name, dtype_hint, required)
# dtype_hint: "string" | "boolean" | "integer" | "float" | "enum:<v1,v2,...>"
# ---------------------------------------------------------------------------

FILE_SCHEMAS: Dict[str, List[Tuple[str, str, bool]]] = {
    "project_config.csv": [
        ("project_id", "string", True),
        ("project_name", "string", True),
        ("environment", "enum:dev,staging,prod", True),
        ("tableau_site", "string", True),
        ("tableau_server_url", "string", True),
        ("target_project", "string", True),
        ("workbook_name", "string", True),
        ("tableau_version", "string", False),
        ("figma_file_id", "string", False),
        ("enable_extract", "boolean", False),
        ("description", "string", False),
    ],
    "data_sources.csv": [
        ("datasource_id", "string", True),
        ("datasource_name", "string", True),
        ("connection_id", "string", True),
        ("datasource_type", "enum:live,extract,published", True),
        ("published_ds_name", "string", False),
        ("default_schema", "string", False),
        ("primary_table", "string", True),
        ("is_primary", "boolean", False),
    ],
    "connections.csv": [
        ("connection_id", "string", True),
        ("class", "enum:snowflake,postgres,mysql,excel-direct,sqlserver,redshift", True),
        ("server", "string", True),
        ("dbname", "string", True),
        ("schema", "string", True),
        ("warehouse", "string", False),
        ("port", "integer", False),
        ("role", "string", False),
        ("auth_method", "enum:Username Password,OAuth,Key-pair", True),
        ("auth_id", "string", True),
    ],
    "auth.csv": [
        ("auth_id", "string", True),
        ("username_env", "string", True),
        ("password_env", "string", True),
        ("pat_name_env", "string", False),
        ("pat_secret_env", "string", False),
        ("oauth_token_env", "string", False),
        ("description", "string", False),
    ],
    "tables.csv": [
        ("table_id", "string", True),
        ("datasource_id", "string", True),
        ("table_name", "string", True),
        ("schema", "string", False),
        ("alias", "string", False),
        ("is_custom_sql", "boolean", False),
        ("custom_sql", "string", False),
    ],
    "columns.csv": [
        ("column_id", "string", True),
        ("table_id", "string", True),
        ("column_name", "string", True),
        ("display_name", "string", False),
        ("datatype", "enum:string,integer,real,date,datetime,boolean", True),
        ("role", "enum:dimension,measure", True),
        ("aggregation", "enum:Sum,Avg,Count,Min,Max,CountD", False),
        ("description", "string", False),
        ("hidden", "boolean", False),
        ("group", "string", False),
    ],
    "relationships.csv": [
        ("relationship_id", "string", True),
        ("datasource_id", "string", True),
        ("left_table_id", "string", True),
        ("right_table_id", "string", True),
        ("left_key", "string", True),
        ("right_key", "string", True),
        ("join_type", "enum:inner,left,right,full", True),
        ("relationship_type", "enum:join,relationship", False),
    ],
    "metrics.csv": [
        ("metric_id", "string", True),
        ("datasource_id", "string", True),
        ("metric_name", "string", True),
        ("formula", "string", True),
        ("datatype", "enum:string,integer,real,date,datetime,boolean", True),
        ("format_string", "string", False),
        ("description", "string", False),
        ("is_lod", "boolean", False),
        ("calculation_id", "string", False),
    ],
    "dimensions.csv": [
        ("dimension_id", "string", True),
        ("datasource_id", "string", True),
        ("dimension_name", "string", True),
        ("dimension_type", "enum:hierarchy,group,set,calc", True),
        ("columns", "string", True),
        ("description", "string", False),
    ],
    "dashboard_requirements.csv": [
        ("view_id", "string", True),
        ("view_name", "string", True),
        ("view_type", "enum:worksheet,dashboard", True),
        ("datasource_id", "string", True),
        ("chart_type", "enum:Bar,Line,Pie,Text,Map,Scatter,Area,Shape,Circle,Square,Automatic", True),
        ("rows", "string", False),
        ("columns", "string", False),
        ("color", "string", False),
        ("size", "string", False),
        ("label", "string", False),
        ("filter_fields", "string", False),
        ("sort_by", "string", False),
        ("sort_direction", "enum:asc,desc", False),
        ("dashboard_id", "string", False),
        ("dashboard_layout", "enum:grid-2x2,grid-3x2,grid-2x3,horizontal,vertical,freeform", False),
        ("views_in_dashboard", "string", False),
        ("width_px", "integer", False),
        ("height_px", "integer", False),
    ],
    "figma_layout.csv": [
        ("element_id", "string", True),
        ("element_type", "enum:color,font,zone,spacing,component", True),
        ("name", "string", True),
        ("value", "string", True),
        ("category", "string", False),
        ("zone_view_id", "string", False),
        ("x_px", "integer", False),
        ("y_px", "integer", False),
        ("w_px", "integer", False),
        ("h_px", "integer", False),
    ],
}

# ---------------------------------------------------------------------------
# FK integrity rules: (child_file, child_col, parent_file, parent_col, severity)
# ---------------------------------------------------------------------------

FK_RULES: List[Tuple[str, str, str, str, Severity]] = [
    ("data_sources.csv", "connection_id", "connections.csv", "connection_id", Severity.CRITICAL),
    ("tables.csv", "datasource_id", "data_sources.csv", "datasource_id", Severity.CRITICAL),
    ("columns.csv", "table_id", "tables.csv", "table_id", Severity.CRITICAL),
    ("relationships.csv", "datasource_id", "data_sources.csv", "datasource_id", Severity.CRITICAL),
    ("relationships.csv", "left_table_id", "tables.csv", "table_id", Severity.CRITICAL),
    ("relationships.csv", "right_table_id", "tables.csv", "table_id", Severity.CRITICAL),
    ("metrics.csv", "datasource_id", "data_sources.csv", "datasource_id", Severity.CRITICAL),
    ("dimensions.csv", "datasource_id", "data_sources.csv", "datasource_id", Severity.CRITICAL),
    ("dashboard_requirements.csv", "datasource_id", "data_sources.csv", "datasource_id", Severity.CRITICAL),
    ("connections.csv", "auth_id", "auth.csv", "auth_id", Severity.CRITICAL),
    # Soft FK: figma zone → dashboard_requirements view_id
    ("figma_layout.csv", "zone_view_id", "dashboard_requirements.csv", "view_id", Severity.WARNING),
]

# ---------------------------------------------------------------------------
# Business rule validators
# ---------------------------------------------------------------------------

_HEX_COLOR_RE = re.compile(r"^#[0-9A-Fa-f]{6}([0-9A-Fa-f]{2})?$")
_URL_RE = re.compile(r"^https?://")
_TABLEAU_FORMULA_BRACKETS_RE = re.compile(r"\[.*?\]")
_LOD_RE = re.compile(r"\{.*?(FIXED|INCLUDE|EXCLUDE).*?\}", re.IGNORECASE | re.DOTALL)


def _check_hex_colors(df: pd.DataFrame, file_name: str) -> List[ValidationIssue]:
    """Figma color tokens must be valid hex codes."""
    issues: List[ValidationIssue] = []
    if file_name != "figma_layout.csv":
        return issues
    color_rows = df[df["element_type"].str.lower() == "color"] if "element_type" in df.columns else pd.DataFrame()
    for idx, row in color_rows.iterrows():
        val = str(row.get("value", "")).strip()
        if val and not _HEX_COLOR_RE.match(val):
            issues.append(ValidationIssue(
                severity=Severity.WARNING,
                rule="figma_color_hex_format",
                file=file_name,
                column="value",
                row_index=int(idx),  # type: ignore[arg-type]
                message=f"Color token '{row.get('name')}' has invalid hex value '{val}'. Expected #RRGGBB.",
                value=val,
            ))
    return issues


def _check_tableau_server_url(df: pd.DataFrame, file_name: str) -> List[ValidationIssue]:
    """project_config.tableau_server_url must start with https://"""
    issues: List[ValidationIssue] = []
    if file_name != "project_config.csv" or "tableau_server_url" not in df.columns:
        return issues
    for idx, row in df.iterrows():
        url = str(row.get("tableau_server_url", "")).strip()
        if url and not _URL_RE.match(url):
            issues.append(ValidationIssue(
                severity=Severity.CRITICAL,
                rule="tableau_server_url_format",
                file=file_name,
                column="tableau_server_url",
                row_index=int(idx),  # type: ignore[arg-type]
                message=f"tableau_server_url '{url}' must start with https://",
                value=url,
            ))
    return issues


def _check_metric_formula_brackets(df: pd.DataFrame, file_name: str) -> List[ValidationIssue]:
    """Tableau formulas must contain at least one [FieldName] reference."""
    issues: List[ValidationIssue] = []
    if file_name != "metrics.csv" or "formula" not in df.columns:
        return issues
    for idx, row in df.iterrows():
        formula = str(row.get("formula", "")).strip()
        if formula and not _TABLEAU_FORMULA_BRACKETS_RE.search(formula):
            issues.append(ValidationIssue(
                severity=Severity.WARNING,
                rule="metric_formula_no_field_ref",
                file=file_name,
                column="formula",
                row_index=int(idx),  # type: ignore[arg-type]
                message=(
                    f"Metric '{row.get('metric_name')}' formula '{formula}' contains no "
                    "[FieldName] references. Verify this is intentional."
                ),
                value=formula,
            ))
    return issues


def _check_lod_flag_consistency(df: pd.DataFrame, file_name: str) -> List[ValidationIssue]:
    """If formula contains LOD syntax, is_lod should be true."""
    issues: List[ValidationIssue] = []
    if file_name != "metrics.csv":
        return issues
    if "formula" not in df.columns or "is_lod" not in df.columns:
        return issues
    for idx, row in df.iterrows():
        formula = str(row.get("formula", "")).strip()
        is_lod_val = str(row.get("is_lod", "false")).strip().lower()
        has_lod = bool(_LOD_RE.search(formula))
        is_lod_flag = is_lod_val in ("true", "1", "yes")
        if has_lod and not is_lod_flag:
            issues.append(ValidationIssue(
                severity=Severity.WARNING,
                rule="lod_flag_mismatch",
                file=file_name,
                column="is_lod",
                row_index=int(idx),  # type: ignore[arg-type]
                message=(
                    f"Metric '{row.get('metric_name')}' formula contains LOD syntax but "
                    "is_lod=false. Set is_lod=true."
                ),
                value=formula,
            ))
        if not has_lod and is_lod_flag:
            issues.append(ValidationIssue(
                severity=Severity.INFO,
                rule="lod_flag_mismatch",
                file=file_name,
                column="is_lod",
                row_index=int(idx),  # type: ignore[arg-type]
                message=(
                    f"Metric '{row.get('metric_name')}' is_lod=true but formula has no "
                    "LOD syntax. Verify formula."
                ),
                value=formula,
            ))
    return issues


def _check_dashboard_has_worksheets(
    dash_df: pd.DataFrame, req_df: pd.DataFrame, file_name: str
) -> List[ValidationIssue]:
    """Each dashboard must reference at least one worksheet in views_in_dashboard."""
    issues: List[ValidationIssue] = []
    if "view_type" not in dash_df.columns:
        return issues
    dashboards = dash_df[dash_df["view_type"].str.lower() == "dashboard"]
    worksheet_ids: set = set()
    if "view_id" in req_df.columns and "view_type" in req_df.columns:
        ws_rows = req_df[req_df["view_type"].str.lower() == "worksheet"]
        worksheet_ids = set(ws_rows["view_id"].dropna().astype(str))

    for idx, row in dashboards.iterrows():
        views_raw = str(row.get("views_in_dashboard", "")).strip()
        if not views_raw:
            issues.append(ValidationIssue(
                severity=Severity.CRITICAL,
                rule="dashboard_missing_worksheets",
                file=file_name,
                column="views_in_dashboard",
                row_index=int(idx),  # type: ignore[arg-type]
                message=f"Dashboard '{row.get('view_name')}' has no views_in_dashboard.",
                value=None,
            ))
            continue
        referenced = {v.strip() for v in views_raw.split("|") if v.strip()}
        orphans = referenced - worksheet_ids
        for orphan in orphans:
            issues.append(ValidationIssue(
                severity=Severity.CRITICAL,
                rule="dashboard_orphan_worksheet_ref",
                file=file_name,
                column="views_in_dashboard",
                row_index=int(idx),  # type: ignore[arg-type]
                message=(
                    f"Dashboard '{row.get('view_name')}' references worksheet "
                    f"'{orphan}' which does not exist in dashboard_requirements.csv."
                ),
                value=orphan,
            ))
    return issues


def _check_duplicate_ids(df: pd.DataFrame, file_name: str, id_col: str) -> List[ValidationIssue]:
    """Primary key column must be unique within the file."""
    issues: List[ValidationIssue] = []
    if id_col not in df.columns:
        return issues
    dupes = df[df.duplicated(subset=[id_col], keep=False)]
    seen: set = set()
    for idx, row in dupes.iterrows():
        val = str(row[id_col])
        if val not in seen:
            seen.add(val)
            issues.append(ValidationIssue(
                severity=Severity.CRITICAL,
                rule="duplicate_primary_key",
                file=file_name,
                column=id_col,
                row_index=int(idx),  # type: ignore[arg-type]
                message=f"Duplicate ID '{val}' in column '{id_col}'.",
                value=val,
            ))
    return issues


def _check_snowflake_warehouse_required(df: pd.DataFrame, file_name: str) -> List[ValidationIssue]:
    """Snowflake connections must specify a warehouse."""
    issues: List[ValidationIssue] = []
    if file_name != "connections.csv":
        return issues
    if "class" not in df.columns or "warehouse" not in df.columns:
        return issues
    sf_rows = df[df["class"].str.lower() == "snowflake"]
    for idx, row in sf_rows.iterrows():
        wh = str(row.get("warehouse", "")).strip()
        if not wh or wh.lower() in ("nan", "none", ""):
            issues.append(ValidationIssue(
                severity=Severity.WARNING,
                rule="snowflake_warehouse_missing",
                file=file_name,
                column="warehouse",
                row_index=int(idx),  # type: ignore[arg-type]
                message=(
                    f"Snowflake connection '{row.get('connection_id')}' has no warehouse. "
                    "Tableau will use the account default — confirm this is intentional."
                ),
                value=None,
            ))
    return issues


def _check_auth_credential_format(df: pd.DataFrame, file_name: str) -> List[ValidationIssue]:
    """Warn if env var names contain spaces or look like actual secret values."""
    issues: List[ValidationIssue] = []
    if file_name != "auth.csv":
        return issues
    env_cols = ["username_env", "password_env", "pat_name_env", "pat_secret_env", "oauth_token_env"]
    # Patterns that look like actual secrets, not env var names
    _looks_like_secret_re = re.compile(r"^(sk-|xoxb-|xoxp-|ya29\.|ey[A-Z])", re.IGNORECASE)
    for col in env_cols:
        if col not in df.columns:
            continue
        for idx, row in df.iterrows():
            val = str(row.get(col, "")).strip()
            if not val or val.lower() in ("nan", "none", ""):
                continue
            if " " in val:
                issues.append(ValidationIssue(
                    severity=Severity.CRITICAL,
                    rule="auth_env_var_has_spaces",
                    file=file_name,
                    column=col,
                    row_index=int(idx),  # type: ignore[arg-type]
                    message=f"Column '{col}' value '{val}' contains spaces. Must be an env var name.",
                    value=val,
                ))
            elif _looks_like_secret_re.match(val):
                issues.append(ValidationIssue(
                    severity=Severity.CRITICAL,
                    rule="auth_secret_in_csv",
                    file=file_name,
                    column=col,
                    row_index=int(idx),  # type: ignore[arg-type]
                    message=(
                        f"Column '{col}' value appears to be an actual secret token, "
                        "not an environment variable name. Never store secrets in CSV!"
                    ),
                    value="<redacted>",
                ))
    return issues


def _check_dashboard_dimension_requirements(
    req_df: pd.DataFrame, file_name: str
) -> List[ValidationIssue]:
    """Chart-type specific field requirements."""
    issues: List[ValidationIssue] = []
    if file_name != "dashboard_requirements.csv":
        return issues
    chart_rules: Dict[str, Dict[str, List[str]]] = {
        "Pie": {"required_any": ["color"], "desc": "Pie charts need a 'color' (segment) field."},
        "Map": {"required_any": ["rows", "columns"], "desc": "Map charts need geographic rows or columns."},
        "Scatter": {"required_all": ["rows", "columns"], "desc": "Scatter charts need both rows and columns."},
    }
    for idx, row in req_df.iterrows():
        chart = str(row.get("chart_type", "")).strip()
        if chart not in chart_rules:
            continue
        rule = chart_rules[chart]
        desc = rule["desc"]
        if "required_any" in rule:
            for f in rule["required_any"]:
                val = str(row.get(f, "")).strip()
                if not val or val.lower() in ("nan", "none", ""):
                    issues.append(ValidationIssue(
                        severity=Severity.WARNING,
                        rule="chart_type_field_requirement",
                        file=file_name,
                        column=f,
                        row_index=int(idx),  # type: ignore[arg-type]
                        message=f"Worksheet '{row.get('view_name')}': {desc}",
                        value=chart,
                    ))
        if "required_all" in rule:
            missing = [
                f for f in rule["required_all"]
                if not str(row.get(f, "")).strip() or str(row.get(f, "")).strip().lower() in ("nan", "none")
            ]
            for f in missing:
                issues.append(ValidationIssue(
                    severity=Severity.WARNING,
                    rule="chart_type_field_requirement",
                    file=file_name,
                    column=f,
                    row_index=int(idx),  # type: ignore[arg-type]
                    message=f"Worksheet '{row.get('view_name')}': {desc}",
                    value=chart,
                ))
    return issues


# ---------------------------------------------------------------------------
# Primary key map (file → pk column)
# ---------------------------------------------------------------------------
_PK_MAP: Dict[str, str] = {
    "project_config.csv": "project_id",
    "data_sources.csv": "datasource_id",
    "connections.csv": "connection_id",
    "auth.csv": "auth_id",
    "tables.csv": "table_id",
    "columns.csv": "column_id",
    "relationships.csv": "relationship_id",
    "metrics.csv": "metric_id",
    "dimensions.csv": "dimension_id",
    "dashboard_requirements.csv": "view_id",
    "figma_layout.csv": "element_id",
}


# ---------------------------------------------------------------------------
# MetadataValidationAgent
# ---------------------------------------------------------------------------

class MetadataValidationAgent(BaseAgent):
    """
    Agent #03 — Metadata Validation

    Validates all CSV input files for:
    - File presence
    - Required column presence
    - Data type compliance
    - Enum value constraints
    - Primary key uniqueness
    - Referential integrity (FK rules)
    - Business logic rules (URL format, color hex, formula syntax, etc.)
    """

    AGENT_ID = "metadata_validation"
    PHASE = "VALIDATING"

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        # Pass config to BaseAgent per the standard interface contract.
        # Also cache csv_dir directly on the instance — some BaseAgent builds
        # use self.config as a live-config store that may not mirror the
        # constructor dict verbatim; caching ensures reliable access.
        super().__init__(self.AGENT_ID, config or {})
        self._default_csv_dir: Optional[str] = (config or {}).get("csv_dir")

    # ── BaseAgent contract ─────────────────────────────────────────────────

    def validate_input(self, state: Dict[str, Any]) -> List[str]:
        errors: List[str] = []
        project_spec = state.get("project_spec")
        if not project_spec:
            errors.append("'project_spec' is missing from workflow state.")
        csv_dir = state.get("csv_dir") or self._default_csv_dir
        if not csv_dir:
            errors.append("'csv_dir' must be present in state or agent config.")
        return errors

    def run(self, state: Dict[str, Any]) -> AgentResult:
        start = time.monotonic()
        self.log_start()

        pre_errors = self.validate_input(state)
        if pre_errors:
            result = AgentResult(
                agent_id=self.AGENT_ID,
                phase=self.PHASE,
                status="failed",
                errors=pre_errors,
                duration_ms=(time.monotonic() - start) * 1000,
            )
            self.log_complete(result)
            return result

        csv_dir = Path(state.get("csv_dir") or self._default_csv_dir or "csv_inputs")
        project_spec = state.get("project_spec", {})
        project_id = project_spec.get("project_id", "unknown")

        report = self._run_validation(csv_dir, project_id)
        report.duration_ms = (time.monotonic() - start) * 1000

        status = "success" if report.is_valid else "failed"
        result = AgentResult(
            agent_id=self.AGENT_ID,
            phase=self.PHASE,
            status=status,
            output={"validation_report": report.to_dict()},
            errors=[i.message for i in report.criticals],
            warnings=[i.message for i in report.warnings],
            duration_ms=report.duration_ms,
        )
        self.log_complete(result)
        return result

    # ── Orchestration entry-point (LangGraph node signature) ──────────────

    def __call__(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """LangGraph node wrapper — returns state delta."""
        result = self.run(state)
        return {
            "validation_report": result.output.get("validation_report", {}),
            "errors": result.errors,
            "phase": self.PHASE if result.status == "success" else "FAILED",
        }

    # ── Internal validation pipeline ──────────────────────────────────────

    def _run_validation(self, csv_dir: Path, project_id: str) -> ValidationReport:
        report = ValidationReport(project_id=project_id)
        dataframes: Dict[str, pd.DataFrame] = {}

        # 1. Load & validate each expected file
        for file_name, schema_def in FILE_SCHEMAS.items():
            path = csv_dir / file_name
            if not path.exists():
                # Non-critical files: WARNING; core files: CRITICAL
                core_files = {
                    "project_config.csv", "data_sources.csv", "connections.csv",
                    "auth.csv", "tables.csv", "columns.csv", "dashboard_requirements.csv",
                }
                sev = Severity.CRITICAL if file_name in core_files else Severity.WARNING
                report.issues.append(ValidationIssue(
                    severity=sev,
                    rule="file_missing",
                    file=file_name,
                    column=None,
                    row_index=None,
                    message=f"Required file '{file_name}' not found in '{csv_dir}'.",
                ))
                continue

            # Load
            try:
                df = pd.read_csv(path, dtype=str, keep_default_na=False)
                # Normalise column names
                df.columns = [c.strip().lower() for c in df.columns]
            except Exception as exc:
                report.issues.append(ValidationIssue(
                    severity=Severity.CRITICAL,
                    rule="file_parse_error",
                    file=file_name,
                    column=None,
                    row_index=None,
                    message=f"Failed to parse '{file_name}': {exc}",
                ))
                continue

            dataframes[file_name] = df
            report.files_checked.append(file_name)

            # 2. Empty file check
            if df.empty:
                report.issues.append(ValidationIssue(
                    severity=Severity.WARNING,
                    rule="file_empty",
                    file=file_name,
                    column=None,
                    row_index=None,
                    message=f"File '{file_name}' is empty (0 data rows).",
                ))

            # 3. Column presence
            for col_name, dtype_hint, required in schema_def:
                col_lower = col_name.lower()
                if col_lower not in df.columns:
                    sev = Severity.CRITICAL if required else Severity.INFO
                    report.issues.append(ValidationIssue(
                        severity=sev,
                        rule="column_missing",
                        file=file_name,
                        column=col_name,
                        row_index=None,
                        message=f"Column '{col_name}' missing from '{file_name}'.",
                    ))
                    continue

                # 4. Required field: no blanks
                if required:
                    blank_mask = df[col_lower].str.strip().eq("") | df[col_lower].str.lower().isin(["nan", "none"])
                    for idx in df[blank_mask].index:
                        report.issues.append(ValidationIssue(
                            severity=Severity.CRITICAL,
                            rule="required_field_blank",
                            file=file_name,
                            column=col_name,
                            row_index=int(idx),  # type: ignore[arg-type]
                            message=f"Required column '{col_name}' is blank at row {idx}.",
                        ))

                # 5. Enum validation
                if dtype_hint.startswith("enum:") and col_lower in df.columns:
                    allowed = {v.strip().lower() for v in dtype_hint[5:].split(",")}
                    for idx, row in df.iterrows():
                        val = str(row[col_lower]).strip()
                        if val and val.lower() not in ("nan", "none", "") and val.lower() not in allowed:
                            report.issues.append(ValidationIssue(
                                severity=Severity.CRITICAL,
                                rule="enum_value_invalid",
                                file=file_name,
                                column=col_name,
                                row_index=int(idx),  # type: ignore[arg-type]
                                message=(
                                    f"Invalid value '{val}' for '{col_name}'. "
                                    f"Allowed: {sorted(allowed)}"
                                ),
                                value=val,
                            ))

                # 6. Integer type check
                if dtype_hint == "integer" and col_lower in df.columns:
                    for idx, row in df.iterrows():
                        val = str(row[col_lower]).strip()
                        if val and val.lower() not in ("nan", "none", ""):
                            try:
                                int(float(val))
                            except (ValueError, OverflowError):
                                report.issues.append(ValidationIssue(
                                    severity=Severity.WARNING,
                                    rule="type_mismatch_integer",
                                    file=file_name,
                                    column=col_name,
                                    row_index=int(idx),  # type: ignore[arg-type]
                                    message=f"Column '{col_name}' expects integer, got '{val}'.",
                                    value=val,
                                ))

                # 7. Boolean type check
                if dtype_hint == "boolean" and col_lower in df.columns:
                    valid_bools = {"true", "false", "1", "0", "yes", "no", "nan", "none", ""}
                    for idx, row in df.iterrows():
                        val = str(row[col_lower]).strip().lower()
                        if val not in valid_bools:
                            report.issues.append(ValidationIssue(
                                severity=Severity.WARNING,
                                rule="type_mismatch_boolean",
                                file=file_name,
                                column=col_name,
                                row_index=int(idx),  # type: ignore[arg-type]
                                message=f"Column '{col_name}' expects boolean, got '{val}'.",
                                value=val,
                            ))

            # 8. Duplicate primary key check
            pk_col = _PK_MAP.get(file_name)
            if pk_col and pk_col.lower() in df.columns:
                report.issues.extend(_check_duplicate_ids(df, file_name, pk_col.lower()))

            # 9. Business rule checks per file
            report.issues.extend(_check_hex_colors(df, file_name))
            report.issues.extend(_check_tableau_server_url(df, file_name))
            report.issues.extend(_check_metric_formula_brackets(df, file_name))
            report.issues.extend(_check_lod_flag_consistency(df, file_name))
            report.issues.extend(_check_snowflake_warehouse_required(df, file_name))
            report.issues.extend(_check_auth_credential_format(df, file_name))
            report.issues.extend(_check_dashboard_dimension_requirements(df, file_name))

        # 10. Cross-file checks
        # Dashboard → worksheet referential check
        if "dashboard_requirements.csv" in dataframes:
            dr = dataframes["dashboard_requirements.csv"]
            report.issues.extend(
                _check_dashboard_has_worksheets(dr, dr, "dashboard_requirements.csv")
            )

        # 11. FK referential integrity
        for child_file, child_col, parent_file, parent_col, severity in FK_RULES:
            child_df = dataframes.get(child_file)
            parent_df = dataframes.get(parent_file)
            if child_df is None or parent_df is None:
                continue
            c_col = child_col.lower()
            p_col = parent_col.lower()
            if c_col not in child_df.columns or p_col not in parent_df.columns:
                continue
            parent_ids: set = set(
                parent_df[p_col].str.strip().dropna().tolist()
            )
            for idx, row in child_df.iterrows():
                val = str(row[c_col]).strip()
                if val and val.lower() not in ("nan", "none", "") and val not in parent_ids:
                    report.issues.append(ValidationIssue(
                        severity=severity,
                        rule="referential_integrity",
                        file=child_file,
                        column=child_col,
                        row_index=int(idx),  # type: ignore[arg-type]
                        message=(
                            f"FK violation: '{child_file}'.{child_col}='{val}' "
                            f"not found in '{parent_file}'.{parent_col}."
                        ),
                        value=val,
                    ))

        # 12. INFO: project-level completeness hints
        self._emit_completeness_hints(dataframes, report)

        self.logger.info(
            "validation_complete",
            project_id=project_id,
            critical=len(report.criticals),
            warning=len(report.warnings),
            info=len(report.infos),
            files_checked=len(report.files_checked),
        )
        return report

    # ── Completeness hints ─────────────────────────────────────────────────

    def _emit_completeness_hints(
        self,
        dataframes: Dict[str, pd.DataFrame],
        report: ValidationReport,
    ) -> None:
        # Metrics file absent
        if "metrics.csv" not in dataframes:
            report.issues.append(ValidationIssue(
                severity=Severity.INFO,
                rule="completeness_hint",
                file="metrics.csv",
                column=None,
                row_index=None,
                message="No metrics.csv found. Workbook will contain no calculated fields.",
            ))
        # Figma layout absent
        if "figma_layout.csv" not in dataframes:
            report.issues.append(ValidationIssue(
                severity=Severity.INFO,
                rule="completeness_hint",
                file="figma_layout.csv",
                column=None,
                row_index=None,
                message=(
                    "No figma_layout.csv found. Dashboard layout will use default "
                    "grid-2x2 template."
                ),
            ))
        # Warn if only one datasource but multiple connection classes
        conn_df = dataframes.get("connections.csv")
        if conn_df is not None and "class" in conn_df.columns:
            classes = conn_df["class"].str.strip().str.lower().unique()
            if len(classes) > 1:
                report.issues.append(ValidationIssue(
                    severity=Severity.INFO,
                    rule="completeness_hint",
                    file="connections.csv",
                    column="class",
                    row_index=None,
                    message=(
                        f"Multiple connection classes detected: {list(classes)}. "
                        "Verify each connection has correct credentials in auth.csv."
                    ),
                ))


# ---------------------------------------------------------------------------
# Public export alias (matches cli.py import pattern)
# ---------------------------------------------------------------------------
ValidationAgent = MetadataValidationAgent
