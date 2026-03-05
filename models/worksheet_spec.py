# models/worksheet_spec.py
"""
WorksheetSpec — field resolver and <worksheet> XML builder for Phase 08.

Core responsibility: translate human-readable field references from
dashboard_requirements.csv into Tableau's internal shelf reference format
and emit valid <worksheet> XML blocks.

Shelf reference format (from Tableau spec):
  [DatasourceName].[aggregation:FieldName:qualifier]

  Qualifiers:
    nk  = nominal key   (string/bool dimension, discrete)
    ok  = ordinal key   (date/datetime dimension, ordinal)
    qk  = quantitative  (measure, continuous)

  Aggregation prefixes:
    none:  raw dimension placement
    sum:   SUM(field)
    avg:   AVG(field)
    cnt:   COUNT(field)
    cntd:  COUNTD(field)
    min:   MIN(field)
    max:   MAX(field)
    mnth:  MONTH(date) — discrete date part
    yr:    YEAR(date)
    qtr:   QUARTER(date)
    day:   DAY(date)

Input field name formats accepted from dashboard_requirements.csv:
  "SUB_CATEGORY"           → bare column name
  "SUM(SALES)"             → aggregated measure
  "MONTH(ORDER_DATE)"      → date part
  "Profit Ratio"           → calculated field (matched by caption)

All field lookups are case-insensitive.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


# ── Constants ──────────────────────────────────────────────────────────────────

VALID_MARK_CLASSES = {
    "Bar", "Line", "Area", "Circle", "Square",
    "Automatic", "Text", "Pie", "Shape", "Map",
}

# Maps aggregation function name (upper) → Tableau shelf prefix
_AGG_PREFIX_MAP: Dict[str, str] = {
    "SUM":    "sum",
    "AVG":    "avg",
    "COUNT":  "cnt",
    "COUNTD": "cntd",
    "MIN":    "min",
    "MAX":    "max",
    "MEDIAN": "median",
    "STDEV":  "stdev",
    "ATTR":   "attr",
    # Date parts
    "MONTH":   "mnth",
    "YEAR":    "yr",
    "QUARTER": "qtr",
    "DAY":     "day",
    "WEEK":    "wk",
    "HOUR":    "hr",
    "MINUTE":  "min",
}

# Maps (role, type_) → qualifier
_QUALIFIER_MAP: Dict[Tuple[str, str], str] = {
    ("dimension", "nominal"):      "nk",
    ("dimension", "ordinal"):      "ok",
    ("dimension", "quantitative"): "qk",   # rare
    ("measure",   "quantitative"): "qk",
    ("measure",   "ordinal"):      "qk",   # rare
    ("measure",   "nominal"):      "nk",   # rare
}

# Date part functions produce ordinal key qualifiers
_DATE_PART_FUNCS = {"MONTH", "YEAR", "QUARTER", "DAY", "WEEK", "HOUR", "MINUTE"}

# Aggregation functions that produce quantitative measures
_AGG_FUNCS = {"SUM", "AVG", "COUNT", "COUNTD", "MIN", "MAX", "MEDIAN", "STDEV", "ATTR"}


# ── Column registry ─────────────────────────────────────────────────────────────

class ColumnRegistry:
    """
    Index of columns from a TdsDocument for fast lookups by name or caption.

    Accepts the 'xml' string from TdsDocument.to_dict(), parses it, and
    builds lookup tables for field resolution.
    """

    def __init__(self, ds_name: str) -> None:
        self.ds_name = ds_name
        # key → (role, type_, is_calculated, bracketed_name, caption)
        self._by_name:    Dict[str, dict] = {}   # lowercase bracketed name
        self._by_caption: Dict[str, dict] = {}   # lowercase caption

    def register(
        self,
        name:          str,   # bracketed: [FIELD_NAME]
        caption:       str,
        role:          str,   # dimension | measure
        type_:         str,   # nominal | ordinal | quantitative
        is_calculated: bool  = False,
    ) -> None:
        entry = {
            "name":          name,
            "caption":       caption,
            "role":          role,
            "type_":         type_,
            "is_calculated": is_calculated,
        }
        self._by_name[name.strip("[]").lower()] = entry
        if caption:
            self._by_caption[caption.lower()] = entry

    def lookup(self, raw: str) -> Optional[dict]:
        """Lookup by column name (no brackets) or caption, case-insensitive."""
        key = raw.strip().strip("[]").lower()
        return self._by_name.get(key) or self._by_caption.get(key)

    @classmethod
    def from_tds_xml(cls, ds_name: str, tds_xml: str) -> "ColumnRegistry":
        """Parse a TDS XML string and populate the registry."""
        reg = cls(ds_name=ds_name)
        try:
            root = ET.fromstring(tds_xml)
        except ET.ParseError:
            return reg

        for col_el in root.iter("column"):
            name     = col_el.get("name", "")
            caption  = col_el.get("caption", name.strip("[]"))
            role     = col_el.get("role", "dimension")
            type_    = col_el.get("type", "nominal")
            is_calc  = col_el.find("calculation") is not None
            if name:
                reg.register(name, caption, role, type_, is_calc)
        return reg


# ── Field reference parser ─────────────────────────────────────────────────────

# Matches: FUNC(FIELD)  e.g. SUM(SALES), MONTH(ORDER_DATE)
_FUNC_RE = re.compile(r"^([A-Z_]+)\((.+)\)$", re.IGNORECASE)


def resolve_field_ref(
    raw:      str,
    registry: ColumnRegistry,
    ds_name:  str,
    *,
    force_agg: Optional[str] = None,   # e.g. "sum" for measures on sort
) -> str:
    """
    Translate a raw field reference string to a Tableau shelf reference.

    Returns the full bracketed reference:
        [ds_name].[agg:FIELD_NAME:qualifier]

    Unknown fields (not in registry) are treated as dimensions.
    """
    raw = raw.strip()
    if not raw:
        return ""

    # ── 1. Parse aggregation wrapper ─────────────────────────────────────────
    agg_prefix:  str  = "none"
    field_name:  str  = raw
    is_date_part: bool = False

    m = _FUNC_RE.match(raw)
    if m:
        func_name  = m.group(1).upper()
        field_name = m.group(2).strip()
        if func_name in _AGG_FUNCS:
            agg_prefix = _AGG_PREFIX_MAP.get(func_name, "sum")
        elif func_name in _DATE_PART_FUNCS:
            agg_prefix   = _AGG_PREFIX_MAP.get(func_name, "yr")
            is_date_part = True

    # ── 2. Registry lookup ────────────────────────────────────────────────────
    entry = registry.lookup(field_name)

    if entry:
        role  = entry["role"]
        type_ = entry["type_"]
        name  = entry["name"].strip("[]")   # bare name without brackets

        # Auto-detect agg for unprefixed fields
        if agg_prefix == "none":
            if force_agg:
                agg_prefix = force_agg
            elif role == "measure":
                agg_prefix = "sum"      # default aggregation for measures

        if is_date_part:
            qualifier = "ok"
        else:
            qualifier = _QUALIFIER_MAP.get((role, type_), "nk")
            if agg_prefix != "none":
                qualifier = "qk" if role == "measure" else qualifier
    else:
        # Unknown field — treat as bare dimension
        name      = field_name.strip("[]")
        qualifier = "nk"
        if agg_prefix in {v for k, v in _AGG_PREFIX_MAP.items() if k in _AGG_FUNCS}:
            qualifier = "qk"

    return f"[{ds_name}].[{agg_prefix}:{name}:{qualifier}]"


# ── WorksheetDocument ──────────────────────────────────────────────────────────

@dataclass
class SortSpec:
    field_ref: str          # full Tableau shelf ref
    direction: str = "descending"   # ascending | descending


@dataclass
class FilterSpec:
    field_ref:  str
    field_role: str = "dimension"   # dimension | measure


@dataclass
class WorksheetDocument:
    """
    Full structured representation of a <worksheet> XML block.

    Serialises to XML via to_xml().

    Gate condition for Phase 09 (DashboardGenAgent):
        doc.is_valid → True when name and ds_name are non-empty
    """
    name:       str              # worksheet display name
    ds_name:    str              # internal datasource name (federated.abc)
    mark_class: str = "Bar"      # Bar | Line | Pie | Text | ...

    row_refs:    List[str] = field(default_factory=list)   # shelf references
    col_refs:    List[str] = field(default_factory=list)
    color_ref:   Optional[str] = None
    size_ref:    Optional[str] = None
    label_ref:   Optional[str] = None
    text_ref:    Optional[str] = None   # alias for label in Text marks

    sorts:   List[SortSpec]   = field(default_factory=list)
    filters: List[FilterSpec] = field(default_factory=list)

    # Metadata
    datasource_id: str = ""
    view_id:       str = ""

    @property
    def is_valid(self) -> bool:
        return bool(self.name) and bool(self.ds_name)

    def to_xml(self) -> str:
        """Serialise to a <worksheet> XML string."""
        ws = ET.Element("worksheet", {"name": self.name})
        table = ET.SubElement(ws, "table")

        # ── <view> ──────────────────────────────────────────────────────────
        view = ET.SubElement(table, "view")
        ds_el = ET.SubElement(view, "datasources")
        ET.SubElement(ds_el, "datasource", {"name": self.ds_name})

        # Rows shelf: pipe-joined refs (or empty)
        rows_text = " + " .join(self.row_refs) if self.row_refs else ""
        ET.SubElement(view, "rows").text = rows_text

        # Cols shelf
        cols_text = " + ".join(self.col_refs) if self.col_refs else ""
        ET.SubElement(view, "cols").text = cols_text

        # Filters (listed under <view> per TWB structure)
        for f in self.filters:
            _emit_filter(view, f)

        # ── <panes> ──────────────────────────────────────────────────────────
        panes = ET.SubElement(table, "panes")
        pane  = ET.SubElement(panes, "pane")

        mark_name = self.mark_class if self.mark_class in VALID_MARK_CLASSES else "Automatic"
        ET.SubElement(pane, "mark", {"class": mark_name})

        enc = ET.SubElement(pane, "encodings")
        if self.color_ref:
            ET.SubElement(enc, "color", {"column": self.color_ref})
        if self.size_ref:
            ET.SubElement(enc, "size",  {"column": self.size_ref})

        # label / text mark
        label = self.label_ref or self.text_ref
        if label:
            ET.SubElement(enc, "text",  {"column": label})

        # ── <sorts> ──────────────────────────────────────────────────────────
        if self.sorts:
            sorts_el = ET.SubElement(table, "sorts")
            for s in self.sorts:
                ET.SubElement(sorts_el, "sort", {
                    "class":     "computed",
                    "column":    s.field_ref,
                    "direction": s.direction,
                })

        ET.indent(ws, space="  ")
        return ET.tostring(ws, encoding="unicode")

    def to_dict(self) -> dict:
        return {
            "view_id":        self.view_id,
            "name":           self.name,
            "datasource_id":  self.datasource_id,
            "ds_name":        self.ds_name,
            "mark_class":     self.mark_class,
            "row_count":      len(self.row_refs),
            "col_count":      len(self.col_refs),
            "has_color":      self.color_ref is not None,
            "has_sort":       bool(self.sorts),
            "has_filter":     bool(self.filters),
            "is_valid":       self.is_valid,
            "xml":            self.to_xml(),
        }

    def summary(self) -> dict:
        return {
            "view_id":   self.view_id,
            "name":      self.name,
            "mark":      self.mark_class,
            "rows":      len(self.row_refs),
            "cols":      len(self.col_refs),
            "is_valid":  self.is_valid,
        }

    def __repr__(self) -> str:
        return (
            f"<WorksheetDocument name={self.name!r} "
            f"mark={self.mark_class} valid={self.is_valid}>"
        )


def _emit_filter(parent: ET.Element, f: FilterSpec) -> None:
    """Emit a minimal <filter> element (categorical placeholder)."""
    cls = "quantitative" if f.field_role == "measure" else "categorical"
    ET.SubElement(parent, "filter", {
        "class":  cls,
        "column": f.field_ref,
    })


# ── Chart-type → mark class mapping ───────────────────────────────────────────

_CHART_TO_MARK: Dict[str, str] = {
    "bar":       "Bar",
    "line":      "Line",
    "area":      "Area",
    "pie":       "Pie",
    "text":      "Text",
    "scatter":   "Circle",
    "map":       "Map",
    "shape":     "Shape",
    "square":    "Square",
    "automatic": "Automatic",
    "circle":    "Circle",
}


def chart_type_to_mark(chart_type: str) -> str:
    return _CHART_TO_MARK.get(chart_type.lower().strip(), "Automatic")
