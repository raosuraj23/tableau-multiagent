# models/project_spec.py
"""
ProjectSpec — Canonical Internal Representation of All CSV Inputs
==================================================================

Every CSV file maps to a Pydantic model here.
IntakeAgent reads the CSVs and produces a ProjectSpec.
All downstream agents receive ProjectSpec as their input.

Design principles:
  - All fields Optional where the CSV marks them optional
  - Validators normalize types (string booleans → bool, etc.)
  - No database connections here — pure data model
  - ProjectSpec.from_csv_dir() is the single entry point for loading

Usage:
    spec = ProjectSpec.from_csv_dir(Path("csv_inputs/"))
    print(spec.project_config.project_name)
    print(len(spec.columns))  # 25
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from pydantic import BaseModel, Field, field_validator, model_validator


# ── Helpers ────────────────────────────────────────────────────────────────────

def _to_bool(v: Any) -> bool:
    """Normalize string booleans from CSV ('true'/'false'/'1'/'0') to bool."""
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "yes")
    return bool(v)


def _clean(v: Any) -> Optional[str]:
    """Return None for empty/NaN strings from CSV."""
    if v is None:
        return None
    s = str(v).strip()
    return s if s and s.lower() not in ("nan", "none", "") else None


# ── CSV row models ─────────────────────────────────────────────────────────────

class ProjectConfig(BaseModel):
    """project_config.csv — one row per project."""
    project_id:         str
    project_name:       str
    environment:        str                     = "dev"
    tableau_site:       str
    tableau_server_url: str
    target_project:     str
    workbook_name:      str
    tableau_version:    str                     = "18.1"
    figma_file_id:      Optional[str]           = None
    enable_extract:     bool                    = False
    description:        Optional[str]           = None

    @field_validator("environment")
    @classmethod
    def validate_environment(cls, v: str) -> str:
        allowed = {"dev", "staging", "prod"}
        if v.lower() not in allowed:
            raise ValueError(f"environment must be one of {allowed}, got '{v}'")
        return v.lower()

    @field_validator("enable_extract", mode="before")
    @classmethod
    def parse_enable_extract(cls, v: Any) -> bool:
        return _to_bool(v)


class ConnectionConfig(BaseModel):
    """connections.csv — one row per database connection."""
    connection_id:  str
    class_:         str    = Field(alias="class")       # 'snowflake', 'postgres', etc.
    server:         str
    dbname:         str
    schema_:        str    = Field(alias="schema")
    warehouse:      Optional[str]   = None
    port:           Optional[int]   = None
    role:           Optional[str]   = None
    auth_method:    str             = "Username Password"
    auth_id:        str

    model_config = {"populate_by_name": True}

    @field_validator("port", mode="before")
    @classmethod
    def parse_port(cls, v: Any) -> Optional[int]:
        if v is None or str(v).strip() in ("", "nan", "None"):
            return None
        try:
            return int(v)
        except (ValueError, TypeError):
            return None


class AuthConfig(BaseModel):
    """auth.csv — credential env var references (never actual secrets)."""
    auth_id:          str
    username_env:     str
    password_env:     str
    pat_name_env:     Optional[str] = None
    pat_secret_env:   Optional[str] = None
    oauth_token_env:  Optional[str] = None
    description:      Optional[str] = None


class DataSourceConfig(BaseModel):
    """data_sources.csv — one row per logical data source."""
    datasource_id:      str
    datasource_name:    str
    connection_id:      str
    datasource_type:    str     = "live"    # live | extract | published
    published_ds_name:  Optional[str] = None
    default_schema:     Optional[str] = None
    primary_table:      str
    is_primary:         bool    = True

    @field_validator("is_primary", mode="before")
    @classmethod
    def parse_is_primary(cls, v: Any) -> bool:
        return _to_bool(v)

    @field_validator("datasource_type")
    @classmethod
    def validate_datasource_type(cls, v: str) -> str:
        allowed = {"live", "extract", "published"}
        if v.lower() not in allowed:
            raise ValueError(f"datasource_type must be one of {allowed}")
        return v.lower()


class TableConfig(BaseModel):
    """tables.csv — one row per database table."""
    table_id:       str
    datasource_id:  str
    table_name:     str
    schema_:        Optional[str]   = Field(None, alias="schema")
    alias:          Optional[str]   = None
    is_custom_sql:  bool            = False
    custom_sql:     Optional[str]   = None

    model_config = {"populate_by_name": True}

    @field_validator("is_custom_sql", mode="before")
    @classmethod
    def parse_is_custom_sql(cls, v: Any) -> bool:
        return _to_bool(v)


class ColumnConfig(BaseModel):
    """columns.csv — one row per table column."""
    column_id:      str
    table_id:       str
    column_name:    str
    display_name:   Optional[str]   = None
    datatype:       str             = "string"  # string|integer|real|date|datetime|boolean
    role:           str             = "dimension"  # dimension | measure
    aggregation:    Optional[str]   = None          # Sum|Avg|Count|Min|Max
    description:    Optional[str]   = None
    hidden:         bool            = False
    group:          Optional[str]   = None

    @field_validator("hidden", mode="before")
    @classmethod
    def parse_hidden(cls, v: Any) -> bool:
        return _to_bool(v)

    @field_validator("datatype")
    @classmethod
    def validate_datatype(cls, v: str) -> str:
        allowed = {"string", "integer", "real", "date", "datetime", "boolean"}
        if v.lower() not in allowed:
            raise ValueError(f"datatype '{v}' not in {allowed}")
        return v.lower()

    @field_validator("role")
    @classmethod
    def validate_role(cls, v: str) -> str:
        allowed = {"dimension", "measure"}
        if v.lower() not in allowed:
            raise ValueError(f"role '{v}' not in {allowed}")
        return v.lower()


class RelationshipConfig(BaseModel):
    """relationships.csv — join/relationship between two tables."""
    relationship_id:    str
    datasource_id:      str
    left_table_id:      str
    right_table_id:     str
    left_key:           str
    right_key:          str
    join_type:          str     = "inner"   # inner|left|right|full
    relationship_type:  str     = "join"    # join | relationship

    @field_validator("join_type")
    @classmethod
    def validate_join_type(cls, v: str) -> str:
        allowed = {"inner", "left", "right", "full"}
        if v.lower() not in allowed:
            raise ValueError(f"join_type '{v}' not in {allowed}")
        return v.lower()


class MetricConfig(BaseModel):
    """metrics.csv — calculated field / business metric definition."""
    metric_id:      str
    datasource_id:  str
    metric_name:    str
    formula:        str
    datatype:       str             = "real"
    format_string:  Optional[str]   = None
    description:    Optional[str]   = None
    is_lod:         bool            = False
    calculation_id: Optional[str]   = None

    @field_validator("is_lod", mode="before")
    @classmethod
    def parse_is_lod(cls, v: Any) -> bool:
        return _to_bool(v)


class DimensionConfig(BaseModel):
    """dimensions.csv — hierarchy, group, set, or calculated dimension."""
    dimension_id:   str
    datasource_id:  str
    dimension_name: str
    dimension_type: str     # hierarchy | group | set | calc
    columns:        str     # pipe-separated column_ids
    description:    Optional[str] = None

    @property
    def column_ids(self) -> List[str]:
        """Parse pipe-separated column IDs into a list."""
        return [c.strip() for c in self.columns.split("|") if c.strip()]


class DashboardRequirement(BaseModel):
    """dashboard_requirements.csv — worksheet or dashboard specification."""
    view_id:            str
    view_name:          str
    view_type:          str     # worksheet | dashboard
    datasource_id:      str
    chart_type:         Optional[str]   = None  # Bar|Line|Pie|Text|Map — None for dashboard rows
    rows:               Optional[str]   = None  # pipe-sep field names
    columns:            Optional[str]   = None  # pipe-sep field names
    color:              Optional[str]   = None
    size:               Optional[str]   = None
    label:              Optional[str]   = None
    filter_fields:      Optional[str]   = None
    sort_by:            Optional[str]   = None
    sort_direction:     Optional[str]   = "desc"
    dashboard_id:       Optional[str]   = None
    dashboard_layout:   Optional[str]   = None  # grid-2x2 | grid-3x2 | etc.
    views_in_dashboard: Optional[str]   = None  # pipe-sep view_ids
    width_px:           int             = 1366
    height_px:          int             = 768
    description:        Optional[str]   = None

    @field_validator("width_px", "height_px", mode="before")
    @classmethod
    def parse_int_or_default(cls, v: Any) -> int:
        try:
            return int(v)
        except (ValueError, TypeError):
            return 1366 if "width" else 768

    @property
    def row_fields(self) -> List[str]:
        return [f.strip() for f in (self.rows or "").split("|") if f.strip()]

    @property
    def column_fields(self) -> List[str]:
        return [f.strip() for f in (self.columns or "").split("|") if f.strip()]

    @property
    def filter_field_list(self) -> List[str]:
        return [f.strip() for f in (self.filter_fields or "").split("|") if f.strip()]

    @property
    def dashboard_view_ids(self) -> List[str]:
        return [v.strip() for v in (self.views_in_dashboard or "").split("|") if v.strip()]


class FigmaLayout(BaseModel):
    """figma_layout.csv — design token or zone definition."""
    element_id:     str
    element_type:   str     # color | font | zone | spacing | component
    name:           str
    value:          str
    category:       Optional[str]   = None
    zone_view_id:   Optional[str]   = None
    x_px:           int             = 0
    y_px:           int             = 0
    w_px:           int             = 0
    h_px:           int             = 0

    @field_validator("x_px", "y_px", "w_px", "h_px", mode="before")
    @classmethod
    def parse_int_zero(cls, v: Any) -> int:
        try:
            return int(v)
        except (ValueError, TypeError):
            return 0


class MstrAttribute(BaseModel):
    """mstr_attributes.csv — MicroStrategy attribute with Tableau mapping."""
    mstr_attribute_id:  str
    mstr_attribute_name: str
    mstr_object_type:   str
    mstr_form_name:     Optional[str]   = None
    mstr_datatype:      str             = "TEXT"
    mstr_description:   Optional[str]   = None
    tableau_field_name: Optional[str]   = None
    tableau_datatype:   Optional[str]   = None
    tableau_role:       Optional[str]   = None
    tableau_type:       Optional[str]   = None
    tableau_column_id:  Optional[str]   = None
    conversion_status:  str             = "pending"
    conversion_notes:   Optional[str]   = None


class MstrMetric(BaseModel):
    """mstr_metrics.csv — MicroStrategy metric with Tableau conversion."""
    mstr_metric_id:     str
    mstr_metric_name:   str
    mstr_formula:       str
    mstr_datatype:      str             = "NUMBER"
    mstr_format:        Optional[str]   = None
    mstr_description:   Optional[str]   = None
    mstr_complexity:    str             = "simple"
    tableau_metric_id:  Optional[str]   = None
    tableau_formula:    Optional[str]   = None
    tableau_datatype:   Optional[str]   = None
    tableau_format:     Optional[str]   = None
    conversion_status:  str             = "pending"
    conversion_notes:   Optional[str]   = None


# ── ProjectSpec (root model) ───────────────────────────────────────────────────

class ProjectSpec(BaseModel):
    """
    The canonical internal representation of a complete project configuration.
    Produced by IntakeAgent from all 12 CSV files.
    Consumed by every downstream agent.

    Fields mirror the 12 CSV files exactly.
    FK relationships are pre-validated by IntakeAgent.
    """
    project_config:         ProjectConfig
    connections:            List[ConnectionConfig]
    auth_configs:           List[AuthConfig]
    data_sources:           List[DataSourceConfig]
    tables:                 List[TableConfig]
    columns:                List[ColumnConfig]
    relationships:          List[RelationshipConfig]
    metrics:                List[MetricConfig]
    dimensions:             List[DimensionConfig]
    dashboard_requirements: List[DashboardRequirement]
    figma_layouts:          List[FigmaLayout]           = Field(default_factory=list)
    mstr_attributes:        List[MstrAttribute]         = Field(default_factory=list)
    mstr_metrics:           List[MstrMetric]            = Field(default_factory=list)

    # ── Convenience lookups ──────────────────────────────────────────────────

    def get_connection(self, connection_id: str) -> Optional[ConnectionConfig]:
        return next((c for c in self.connections if c.connection_id == connection_id), None)

    def get_datasource(self, datasource_id: str) -> Optional[DataSourceConfig]:
        return next((d for d in self.data_sources if d.datasource_id == datasource_id), None)

    def get_primary_datasource(self) -> Optional[DataSourceConfig]:
        return next((d for d in self.data_sources if d.is_primary), None)

    def get_tables_for_datasource(self, datasource_id: str) -> List[TableConfig]:
        return [t for t in self.tables if t.datasource_id == datasource_id]

    def get_columns_for_table(self, table_id: str) -> List[ColumnConfig]:
        return [c for c in self.columns if c.table_id == table_id]

    def get_metrics_for_datasource(self, datasource_id: str) -> List[MetricConfig]:
        return [m for m in self.metrics if m.datasource_id == datasource_id]

    def get_worksheets(self) -> List[DashboardRequirement]:
        return [v for v in self.dashboard_requirements if v.view_type == "worksheet"]

    def get_dashboards(self) -> List[DashboardRequirement]:
        return [v for v in self.dashboard_requirements if v.view_type == "dashboard"]

    def get_color_tokens(self) -> List[FigmaLayout]:
        return [f for f in self.figma_layouts if f.element_type == "color"]

    def get_zone_tokens(self) -> List[FigmaLayout]:
        return [f for f in self.figma_layouts if f.element_type == "zone"]

    def summary(self) -> Dict[str, Any]:
        """One-dict summary for logging."""
        return {
            "project_id":        self.project_config.project_id,
            "project_name":      self.project_config.project_name,
            "environment":       self.project_config.environment,
            "data_sources":      len(self.data_sources),
            "tables":            len(self.tables),
            "columns":           len(self.columns),
            "metrics":           len(self.metrics),
            "dimensions":        len(self.dimensions),
            "worksheets":        len(self.get_worksheets()),
            "dashboards":        len(self.get_dashboards()),
            "mstr_attributes":   len(self.mstr_attributes),
            "mstr_metrics":      len(self.mstr_metrics),
        }

    # ── Factory ──────────────────────────────────────────────────────────────

    @classmethod
    def from_csv_dir(cls, csv_dir: Path) -> "ProjectSpec":
        """
        Load all CSV files from csv_dir and return a validated ProjectSpec.
        Raises ValueError with a clear message if any required file is missing.
        """
        csv_dir = Path(csv_dir)
        required = [
            "project_config.csv", "connections.csv", "auth.csv",
            "data_sources.csv", "tables.csv", "columns.csv",
            "relationships.csv", "metrics.csv", "dimensions.csv",
            "dashboard_requirements.csv",
        ]
        missing = [f for f in required if not (csv_dir / f).exists()]
        if missing:
            raise ValueError(f"Missing required CSV files: {missing}")

        def load(filename: str) -> pd.DataFrame:
            return pd.read_csv(csv_dir / filename, dtype=str).fillna("")

        def row_to_dict(row: pd.Series) -> Dict[str, Any]:
            """Convert a DataFrame row to a dict, cleaning empty strings to None."""
            return {k: (_clean(v) if isinstance(v, str) else v)
                    for k, v in row.items()}

        # Load project_config (single row)
        pc_df   = load("project_config.csv")
        pc_row  = row_to_dict(pc_df.iloc[0])

        # Rename 'class' column to avoid Python keyword clash
        conn_df = load("connections.csv")
        conn_df = conn_df.rename(columns={"schema": "schema_"})

        # Build ProjectSpec
        return cls(
            project_config=ProjectConfig(**pc_row),
            connections=[
                ConnectionConfig(**{
                    **row_to_dict(r),
                    "class_": r.get("class", ""),
                    "schema_": r.get("schema_", r.get("schema", "")),
                })
                for _, r in conn_df.iterrows()
            ],
            auth_configs=[
                AuthConfig(**row_to_dict(r))
                for _, r in load("auth.csv").iterrows()
            ],
            data_sources=[
                DataSourceConfig(**row_to_dict(r))
                for _, r in load("data_sources.csv").iterrows()
            ],
            tables=[
                TableConfig(**{
                    **row_to_dict(r),
                    "schema_": r.get("schema", ""),
                })
                for _, r in load("tables.csv").rename(
                    columns={"schema": "schema_"}
                ).iterrows()
            ],
            columns=[
                ColumnConfig(**row_to_dict(r))
                for _, r in load("columns.csv").iterrows()
            ],
            relationships=[
                RelationshipConfig(**row_to_dict(r))
                for _, r in load("relationships.csv").iterrows()
            ],
            metrics=[
                MetricConfig(**row_to_dict(r))
                for _, r in load("metrics.csv").iterrows()
            ],
            dimensions=[
                DimensionConfig(**row_to_dict(r))
                for _, r in load("dimensions.csv").iterrows()
            ],
            dashboard_requirements=[
                DashboardRequirement(**row_to_dict(r))
                for _, r in load("dashboard_requirements.csv").iterrows()
            ],
            figma_layouts=(
                [FigmaLayout(**row_to_dict(r))
                 for _, r in load("figma_layout.csv").iterrows()]
                if (csv_dir / "figma_layout.csv").exists() else []
            ),
            mstr_attributes=(
                [MstrAttribute(**row_to_dict(r))
                 for _, r in load("mstr_attributes.csv").iterrows()]
                if (csv_dir / "mstr_attributes.csv").exists() else []
            ),
            mstr_metrics=(
                [MstrMetric(**row_to_dict(r))
                 for _, r in load("mstr_metrics.csv").iterrows()]
                if (csv_dir / "mstr_metrics.csv").exists() else []
            ),
        )
