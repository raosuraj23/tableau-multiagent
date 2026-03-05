# models/field_mapping.py
"""
TableauFieldMapping — Structured Output of the DataConversionAgent
===================================================================

This model is the "semantic bridge" between the raw schema profile and the
Tableau XML generators. Every column, calculated metric, and dimension that
will appear in the TWB file is represented here with fully-resolved Tableau
attributes.

Consumed by:
  - Phase 06 (SemanticModelAgent)    → builds TDS XML <column> elements
  - Phase 07 (MetricDefinitionAgent) → builds <calculation> elements
  - Phase 08 (TableauModelAgent)     → builds worksheet shelf references

Key types:
  FieldMapping    — one field (dimension or measure), fully resolved
  MetricMapping   — one calculated field (from metrics.csv)
  FieldMappingReport — root output, keyed by datasource_id

Resolution priority for datatype / role:
  1. Explicit override in columns.csv (spec_datatype / spec_role)
  2. DB-derived type from SchemaProfile
  3. MSTR type from mstr_attributes.csv / mstr_metrics.csv
  4. Default: string / dimension

Gate condition for Phase 06:
  mapping.can_proceed == True
  (all required fields have been successfully mapped)
"""

from __future__ import annotations

from dataclasses import dataclass, field as dc_field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional


class MappingStatus(str, Enum):
    MAPPED       = "mapped"       # Fully resolved, ready for XML
    OVERRIDDEN   = "overridden"   # Spec override applied (type / role)
    INFERRED     = "inferred"     # No spec — derived from DB or MSTR
    CONFLICT     = "conflict"     # Spec and DB types disagree (flagged)
    MISSING      = "missing"      # Referenced in spec but not found in DB
    SKIPPED      = "skipped"      # Explicitly hidden or excluded


class MappingSource(str, Enum):
    SPEC         = "spec"         # columns.csv explicit definition
    DB           = "db"           # DB INFORMATION_SCHEMA
    MSTR         = "mstr"         # mstr_attributes.csv / mstr_metrics.csv
    CALCULATED   = "calculated"   # metrics.csv formula


# ── FieldMapping ───────────────────────────────────────────────────────────────

@dataclass
class FieldMapping:
    """
    A single field (dimension or measure) mapped for use in Tableau XML.

    column_id     → FK to columns.csv (may be None for DB-only fields)
    tableau_name  → [FieldName] as it appears in Tableau shelf references
    xml_name      → [FieldName] in the <column name='...'> attribute
    caption       → display name shown in Tableau UI
    datatype      → string | integer | real | date | datetime | boolean
    role          → dimension | measure
    type_         → nominal | ordinal | quantitative
    aggregation   → SUM | AVG | COUNT | MIN | MAX | NONE
    """
    field_id:       str                         # Unique within this mapping
    column_id:      Optional[str]               = None
    table_name:     str                         = ""
    datasource_id:  str                         = ""

    # Resolved Tableau attributes
    tableau_name:   str                         = ""   # [FieldName]
    xml_name:       str                         = ""   # [FieldName] in XML attr
    caption:        str                         = ""   # display name
    datatype:       str                         = "string"
    role:           str                         = "dimension"
    type_:          str                         = "nominal"
    aggregation:    str                         = "NONE"
    hidden:         bool                        = False
    folder_group:   Optional[str]               = None
    description:    Optional[str]               = None
    format_string:  Optional[str]               = None

    # Provenance
    status:         MappingStatus               = MappingStatus.MAPPED
    source:         MappingSource               = MappingSource.SPEC
    conflict_note:  Optional[str]               = None

    @property
    def is_measure(self) -> bool:
        return self.role == "measure"

    @property
    def is_dimension(self) -> bool:
        return self.role == "dimension"

    @property
    def tableau_ref(self) -> str:
        """
        Full qualified shelf reference: [DatasourceName].[agg:FieldName:qualifier]
        Used when building worksheet rows/columns shelf XML.
        Datasource prefix is filled in by SemanticModelAgent.
        """
        if self.is_measure:
            agg = (self.aggregation or "SUM").lower()
            return f"[{agg}:{self.caption}:qk]"
        else:
            return f"[none:{self.caption}:nk]"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "field_id":      self.field_id,
            "column_id":     self.column_id,
            "table_name":    self.table_name,
            "datasource_id": self.datasource_id,
            "tableau_name":  self.tableau_name,
            "xml_name":      self.xml_name,
            "caption":       self.caption,
            "datatype":      self.datatype,
            "role":          self.role,
            "type":          self.type_,
            "aggregation":   self.aggregation,
            "hidden":        self.hidden,
            "folder_group":  self.folder_group,
            "description":   self.description,
            "format_string": self.format_string,
            "status":        self.status.value,
            "source":        self.source.value,
            "conflict_note": self.conflict_note,
            "is_measure":    self.is_measure,
            "tableau_ref":   self.tableau_ref,
        }


# ── MetricMapping ──────────────────────────────────────────────────────────────

@dataclass
class MetricMapping:
    """
    A calculated field derived from metrics.csv.
    Wraps the Tableau formula and all XML attributes for the <calculation> element.
    """
    metric_id:       str
    metric_name:     str
    datasource_id:   str
    tableau_formula: str
    datatype:        str                        = "real"
    role:            str                        = "measure"
    type_:           str                        = "quantitative"
    is_lod:          bool                       = False
    format_string:   Optional[str]              = None
    description:     Optional[str]              = None

    # Generated XML attributes
    calc_id:         str                        = ""   # [Calculation_NNN...] name
    caption:         str                        = ""   # display name

    # Validation flags
    formula_valid:   bool                       = True
    formula_errors:  List[str]                  = dc_field(default_factory=list)

    @property
    def xml_name(self) -> str:
        """The <column name='...'> attribute value."""
        return self.calc_id or f"[Calculation_{self.metric_id}]"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "metric_id":       self.metric_id,
            "metric_name":     self.metric_name,
            "datasource_id":   self.datasource_id,
            "tableau_formula": self.tableau_formula,
            "datatype":        self.datatype,
            "role":            self.role,
            "type":            self.type_,
            "is_lod":          self.is_lod,
            "format_string":   self.format_string,
            "description":     self.description,
            "calc_id":         self.calc_id,
            "caption":         self.caption,
            "xml_name":        self.xml_name,
            "formula_valid":   self.formula_valid,
            "formula_errors":  self.formula_errors,
        }


# ── TableauFieldMapping ────────────────────────────────────────────────────────

@dataclass
class TableauFieldMapping:
    """
    Root output of the DataConversionAgent.
    Contains fully-resolved field and metric mappings for every datasource.
    """
    project_id:      str                        = ""
    run_id:          str                        = ""
    fields:          List[FieldMapping]         = dc_field(default_factory=list)
    metrics:         List[MetricMapping]        = dc_field(default_factory=list)
    timestamp:       str                        = dc_field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    # ── Lookups ──────────────────────────────────────────────────────────────

    def get_field(self, field_id: str) -> Optional[FieldMapping]:
        return next((f for f in self.fields if f.field_id == field_id), None)

    def get_metric(self, metric_id: str) -> Optional[MetricMapping]:
        return next((m for m in self.metrics if m.metric_id == metric_id), None)

    def fields_for_datasource(self, datasource_id: str) -> List[FieldMapping]:
        return [f for f in self.fields if f.datasource_id == datasource_id]

    def metrics_for_datasource(self, datasource_id: str) -> List[MetricMapping]:
        return [m for m in self.metrics if m.datasource_id == datasource_id]

    def dimensions(self, datasource_id: Optional[str] = None) -> List[FieldMapping]:
        fields = self.fields_for_datasource(datasource_id) if datasource_id else self.fields
        return [f for f in fields if f.is_dimension]

    def measures(self, datasource_id: Optional[str] = None) -> List[FieldMapping]:
        fields = self.fields_for_datasource(datasource_id) if datasource_id else self.fields
        return [f for f in fields if f.is_measure]

    # ── Quality aggregation ──────────────────────────────────────────────────

    @property
    def total_fields(self) -> int:
        return len(self.fields)

    @property
    def mapped_count(self) -> int:
        return sum(1 for f in self.fields
                   if f.status in (MappingStatus.MAPPED, MappingStatus.OVERRIDDEN,
                                   MappingStatus.INFERRED))

    @property
    def conflict_count(self) -> int:
        return sum(1 for f in self.fields if f.status == MappingStatus.CONFLICT)

    @property
    def missing_count(self) -> int:
        return sum(1 for f in self.fields if f.status == MappingStatus.MISSING)

    @property
    def invalid_metric_count(self) -> int:
        return sum(1 for m in self.metrics if not m.formula_valid)

    @property
    def can_proceed(self) -> bool:
        """
        True if:
          - No MISSING fields (all spec fields found in DB)
          - No invalid metric formulas
        Conflicts are warnings only — XML can still be generated with a note.
        """
        return self.missing_count == 0 and self.invalid_metric_count == 0

    # ── Serialization ─────────────────────────────────────────────────────

    def summary(self) -> Dict[str, Any]:
        return {
            "project_id":           self.project_id,
            "can_proceed":          self.can_proceed,
            "total_fields":         self.total_fields,
            "mapped":               self.mapped_count,
            "conflicts":            self.conflict_count,
            "missing":              self.missing_count,
            "total_metrics":        len(self.metrics),
            "invalid_metrics":      self.invalid_metric_count,
            "timestamp":            self.timestamp,
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            **self.summary(),
            "run_id":   self.run_id,
            "fields":   [f.to_dict() for f in self.fields],
            "metrics":  [m.to_dict() for m in self.metrics],
        }

    def __repr__(self) -> str:
        return (
            f"TableauFieldMapping(can_proceed={self.can_proceed}, "
            f"fields={self.total_fields}, metrics={len(self.metrics)}, "
            f"conflicts={self.conflict_count}, missing={self.missing_count})"
        )
