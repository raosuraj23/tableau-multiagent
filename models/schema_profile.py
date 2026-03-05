# models/schema_profile.py
"""
SchemaProfile — Structured Output of the Source Schema Profiler Agent
======================================================================

The SchemaProfile is the gating artifact between Phase 04 (Profiling)
and Phase 05 (Data Conversion). It captures the *actual* database schema
as observed at runtime, compared against what was declared in columns.csv.

Gate condition for Phase 05:
    profile.can_proceed  → True if at least one table was profiled successfully

Structure:
    SchemaProfile
      └── datasource_id  → one profile per data source
      └── tables[]
            └── TableProfile
                  └── columns[]
                        └── ColumnProfile

ColumnProfile carries:
    - physical_name     (from DB)
    - declared_name     (from columns.csv, may differ by case)
    - physical_type     (DB-native type string, e.g. "NUMBER(18,2)")
    - tableau_datatype  (mapped Tableau type: string/integer/real/date/datetime/boolean)
    - declared_datatype (from columns.csv)
    - type_match        (physical maps to same Tableau type as declared)
    - nullable          (from DB INFORMATION_SCHEMA)
    - row_count / null_count / sample_values  (optional, from profiling)

Usage:
    profile = SchemaProfile(datasource_id="ds_001")
    tp = TableProfile(table_id="tbl_001", table_name="ORDERS",
                      schema="PUBLIC", row_count=125000)
    tp.add_column(ColumnProfile(
        physical_name="REVENUE",
        physical_type="NUMBER(18,2)",
        tableau_datatype="real",
        declared_datatype="real",
        nullable=False,
    ))
    profile.add_table(tp)
    if profile.can_proceed:
        # move to Phase 05
"""

from __future__ import annotations

from dataclasses import dataclass, field as dc_field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


# ── Snowflake / Postgres / MySQL → Tableau type mapping ───────────────────────

PHYSICAL_TO_TABLEAU: Dict[str, str] = {
    # Strings
    "TEXT": "string", "VARCHAR": "string", "STRING": "string",
    "CHAR": "string", "CHARACTER": "string", "NVARCHAR": "string",
    "NCHAR": "string", "VARIANT": "string", "OBJECT": "string",
    "ARRAY": "string", "JSONB": "string", "JSON": "string",
    "UUID": "string", "BYTEA": "string", "INET": "string",
    "CIDR": "string", "TIME": "string",
    # Real / float
    "NUMBER": "real", "DECIMAL": "real", "NUMERIC": "real",
    "FLOAT": "real", "FLOAT4": "real", "FLOAT8": "real",
    "DOUBLE": "real", "DOUBLE PRECISION": "real", "REAL": "real",
    # Integer
    "INTEGER": "integer", "INT": "integer", "BIGINT": "integer",
    "SMALLINT": "integer", "TINYINT": "integer", "BYTEINT": "integer",
    "SERIAL": "integer", "BIGSERIAL": "integer",
    # Boolean
    "BOOLEAN": "boolean", "BOOL": "boolean",
    # Date
    "DATE": "date",
    # Datetime / timestamp
    "TIMESTAMP": "datetime", "TIMESTAMP_NTZ": "datetime",
    "TIMESTAMP_LTZ": "datetime", "TIMESTAMP_TZ": "datetime",
    "DATETIME": "datetime",
}


def map_physical_to_tableau(physical_type: str) -> str:
    """
    Map a DB-native type string to a Tableau datatype.
    Strips precision/scale: 'NUMBER(18,2)' → 'NUMBER' before lookup.
    Falls back to 'string' for unknown types.
    """
    base = physical_type.upper().split("(")[0].strip()
    return PHYSICAL_TO_TABLEAU.get(base, "string")


# ── ColumnProfile ──────────────────────────────────────────────────────────────

@dataclass
class ColumnProfile:
    physical_name:     str
    physical_type:     str                    # Raw DB type, e.g. "NUMBER(18,2)"
    tableau_datatype:  str                    # Mapped Tableau type
    declared_name:     str            = ""    # From columns.csv (empty = undeclared)
    declared_datatype: str            = ""    # From columns.csv
    nullable:          bool           = True
    row_count:         Optional[int]  = None
    null_count:        Optional[int]  = None
    sample_values:     List[str]      = dc_field(default_factory=list)

    @property
    def type_match(self) -> bool:
        """True if physical type maps to same Tableau type as declared."""
        if not self.declared_datatype:
            return True  # not declared → no mismatch to report
        return self.tableau_datatype == self.declared_datatype.lower()

    @property
    def null_rate(self) -> Optional[float]:
        if (self.row_count is not None and self.row_count > 0
                and self.null_count is not None):
            return round(self.null_count / self.row_count, 4)
        return None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "physical_name":     self.physical_name,
            "declared_name":     self.declared_name,
            "physical_type":     self.physical_type,
            "tableau_datatype":  self.tableau_datatype,
            "declared_datatype": self.declared_datatype,
            "type_match":        self.type_match,
            "nullable":          self.nullable,
            "row_count":         self.row_count,
            "null_count":        self.null_count,
            "null_rate":         self.null_rate,
            "sample_values":     self.sample_values[:5],
        }


# ── TableProfile ───────────────────────────────────────────────────────────────

@dataclass
class TableProfile:
    table_id:   str
    table_name: str
    schema:     str                  = "PUBLIC"
    row_count:  Optional[int]        = None
    columns:    List[ColumnProfile]  = dc_field(default_factory=list)
    profiled:   bool                 = False   # False = table was unreachable
    error:      Optional[str]        = None

    def add_column(self, col: ColumnProfile) -> "TableProfile":
        self.columns.append(col)
        return self

    @property
    def type_mismatches(self) -> List[ColumnProfile]:
        return [c for c in self.columns if not c.type_match and c.declared_datatype]

    @property
    def undeclared_columns(self) -> List[ColumnProfile]:
        """Columns in DB that were not declared in columns.csv."""
        return [c for c in self.columns if not c.declared_name]

    @property
    def column_names(self) -> List[str]:
        return [c.physical_name for c in self.columns]

    def get_column(self, name: str) -> Optional[ColumnProfile]:
        return next(
            (c for c in self.columns if c.physical_name.upper() == name.upper()),
            None,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table_id":        self.table_id,
            "table_name":      self.table_name,
            "schema":          self.schema,
            "row_count":       self.row_count,
            "column_count":    len(self.columns),
            "profiled":        self.profiled,
            "error":           self.error,
            "type_mismatches": len(self.type_mismatches),
            "columns":         [c.to_dict() for c in self.columns],
        }


# ── SchemaProfile ──────────────────────────────────────────────────────────────

@dataclass
class SchemaProfile:
    """
    Complete schema profile for one data source.
    Produced by ProfilerAgent (Phase 04).
    Consumed by DataConversionAgent (Phase 05) and SemanticModelAgent (Phase 06).
    """
    datasource_id: str
    project_id:    str               = ""
    run_id:        str               = ""
    tables:        List[TableProfile] = dc_field(default_factory=list)
    timestamp:     str               = dc_field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def add_table(self, table: TableProfile) -> "SchemaProfile":
        self.tables.append(table)
        return self

    def get_table(self, table_name: str) -> Optional[TableProfile]:
        return next(
            (t for t in self.tables if t.table_name.upper() == table_name.upper()),
            None,
        )

    # ── Aggregate metrics ────────────────────────────────────────────────────

    @property
    def total_columns(self) -> int:
        return sum(len(t.columns) for t in self.tables)

    @property
    def total_type_mismatches(self) -> int:
        return sum(len(t.type_mismatches) for t in self.tables)

    @property
    def unprofiled_tables(self) -> List[TableProfile]:
        return [t for t in self.tables if not t.profiled]

    @property
    def profiled_tables(self) -> List[TableProfile]:
        return [t for t in self.tables if t.profiled]

    @property
    def can_proceed(self) -> bool:
        """
        True if at least one table was profiled, OR no tables exist (published ds).
        """
        if not self.tables:
            return True  # published datasource — nothing to profile
        return bool(self.profiled_tables)

    # ── Serialization ────────────────────────────────────────────────────────

    def summary(self) -> Dict[str, Any]:
        return {
            "datasource_id":     self.datasource_id,
            "project_id":        self.project_id,
            "can_proceed":       self.can_proceed,
            "total_tables":      len(self.tables),
            "profiled_tables":   len(self.profiled_tables),
            "unprofiled_tables": len(self.unprofiled_tables),
            "total_columns":     self.total_columns,
            "type_mismatches":   self.total_type_mismatches,
            "timestamp":         self.timestamp,
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            **self.summary(),
            "run_id": self.run_id,
            "tables": [t.to_dict() for t in self.tables],
        }

    def __repr__(self) -> str:
        return (
            f"SchemaProfile(datasource='{self.datasource_id}', "
            f"can_proceed={self.can_proceed}, "
            f"tables={len(self.tables)}, "
            f"mismatches={self.total_type_mismatches})"
        )
