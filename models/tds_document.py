# models/tds_document.py
"""
TdsDocument — Structured Representation of a Tableau Datasource (TDS) XML Block
=================================================================================

Produced by SemanticModelAgent (Phase 06).
Consumed by:
  - MetricDefinitionAgent (Phase 07): appends <calculation> elements
  - TableauModelAgent    (Phase 08): uses field references for shelf XML
  - TWB assembler        (Phase 11): embeds the <datasource> block into <workbook>

Structure mirrors Tableau's XML hierarchy:
  <datasource>
    <connection class='federated'>
      <named-connections>
        <named-connection>
          <connection class='snowflake' .../>
        </named-connection>
      </named-connections>
      <relation name='{primary_table}' table='[{schema}].[{table}]' type='table'/>
      [<relation type='join'> ... </relation>]
      <cols>
        <map key='[ColName]' value='[TableName].[ColName]'/>
      </cols>
    </connection>
    <aliases enabled='yes'/>
    <column name='[FieldName]' datatype='real' role='measure' type='quantitative'
            caption='Caption' hidden='false'>
      [<calculation class='tableau' formula='...'/>]  ← for calculated fields
    </column>
    ...
  </datasource>

Usage:
    doc = TdsDocument(datasource_id='ds_001', caption='Orders',
                      ds_name='federated.abc123')
    doc.add_connection(ConnectionSpec(...))
    doc.add_relation(RelationSpec(...))
    doc.add_column(ColumnSpec(...))
    xml_str = doc.to_xml()
    if doc.is_valid:
        # move to Phase 07
"""

from __future__ import annotations

import hashlib
import re
import uuid
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field as dc_field
from datetime import datetime, timezone
from typing import List, Optional


# ── Constants ──────────────────────────────────────────────────────────────────

TABLEAU_VERSION = "18.1"
TABLEAU_XMLNS   = "http://www.tableausoftware.com/xml/user"
VALID_MARK_CLASSES = {
    "Bar", "Line", "Area", "Circle", "Square", "Automatic",
    "Text", "Pie", "Shape", "Map",
}
VALID_JOIN_TYPES = {"inner", "left", "right", "full"}

# HTML-escape map for formula attribute values
_HTML_ESC = {
    '"': "&quot;",
    "<": "&lt;",
    ">": "&gt;",
    "&": "&amp;",
}


def _escape_formula(formula: str) -> str:
    """Escape special chars in a Tableau formula for XML attribute embedding."""
    # & must be first to avoid double-escaping
    result = formula.replace("&", "&amp;")
    result = result.replace('"', "&quot;")
    result = result.replace("<", "&lt;")
    result = result.replace(">", "&gt;")
    return result


def _make_ds_name(seed: str = "") -> str:
    """Generate a unique federated datasource name like Tableau does."""
    h = hashlib.sha256((seed or uuid.uuid4().hex).encode()).hexdigest()
    return f"federated.{h[:20]}"


# ── Sub-models ─────────────────────────────────────────────────────────────────

@dataclass
class ConnectionSpec:
    """
    Represents the inner <connection class='snowflake' .../> element
    plus the containing <named-connection>.
    """
    connection_id:  str
    conn_class:     str          # snowflake | postgres | mysql | excel-direct
    server:         str
    dbname:         str
    schema:         str          = "PUBLIC"
    warehouse:      Optional[str] = None
    port:           Optional[int] = None
    role:           Optional[str] = None
    username:       str           = ""
    auth_method:    str           = "Username Password"
    odbc_extras:    str           = ""
    one_time_sql:   str           = ""

    @property
    def named_connection_name(self) -> str:
        h = hashlib.sha256(self.connection_id.encode()).hexdigest()
        return f"{self.conn_class}.{h[:12]}"

    @property
    def caption(self) -> str:
        return self.server


@dataclass
class RelationSpec:
    """
    One <relation> element — either a raw table reference or a join.
    For joins: left_table + right_table + join_type + left_key + right_key.
    For raw tables: just table_name + schema.
    """
    relation_id:    str
    table_name:     str
    schema:         str          = "PUBLIC"
    is_join:        bool         = False
    join_type:      str          = "inner"       # inner|left|right|full
    left_table:     str          = ""
    right_table:    str          = ""
    left_key:       str          = ""
    right_key:      str          = ""
    custom_sql:     str          = ""            # set for type='text' relations

    @property
    def is_custom_sql(self) -> bool:
        return bool(self.custom_sql.strip())


@dataclass
class ColumnSpec:
    """
    One <column> element in the datasource.
    Calculated fields include a nested <calculation> element.
    """
    name:           str          # [BracketedFieldName]
    caption:        str          # display name
    datatype:       str          # string|integer|real|date|datetime|boolean
    role:           str          # dimension|measure
    type_:          str          # nominal|ordinal|quantitative
    hidden:         bool         = False
    aggregation:    str          = "Sum"
    description:    Optional[str] = None
    format_string:  Optional[str] = None
    folder_group:   Optional[str] = None

    # Calculated field support
    is_calculated:  bool         = False
    formula:        str          = ""
    is_lod:         bool         = False

    @property
    def xml_name(self) -> str:
        """Ensure name has square brackets."""
        n = self.name.strip()
        if not n.startswith("["):
            n = f"[{n}]"
        return n

    @property
    def escaped_formula(self) -> str:
        return _escape_formula(self.formula)


# ── TdsDocument ────────────────────────────────────────────────────────────────

@dataclass
class TdsDocument:
    """
    Full structured representation of a <datasource> block.

    Serialises to XML via to_xml() — the string is what gets embedded
    in the workbook's <datasources> section.

    Gate condition for Phase 07:
        doc.is_valid  → True when connections and at least one column exist
    """
    datasource_id:  str
    caption:        str
    ds_name:        str          = ""   # federated.abc123 — auto-generated if empty

    connections:    List[ConnectionSpec]  = dc_field(default_factory=list)
    relations:      List[RelationSpec]    = dc_field(default_factory=list)
    columns:        List[ColumnSpec]      = dc_field(default_factory=list)
    col_maps:       List[tuple]           = dc_field(default_factory=list)
    # col_maps: list of (key, value) tuples → <map key='[K]' value='[T].[K]'/>

    timestamp:      str = dc_field(
                        default_factory=lambda: datetime.now(timezone.utc).isoformat()
                    )

    def __post_init__(self) -> None:
        if not self.ds_name:
            self.ds_name = _make_ds_name(self.datasource_id)

    # ── mutation ──────────────────────────────────────────────────────────────

    def add_connection(self, c: ConnectionSpec) -> "TdsDocument":
        self.connections.append(c)
        return self

    def add_relation(self, r: RelationSpec) -> "TdsDocument":
        self.relations.append(r)
        return self

    def add_column(self, c: ColumnSpec) -> "TdsDocument":
        self.columns.append(c)
        return self

    def add_col_map(self, key: str, value: str) -> "TdsDocument":
        self.col_maps.append((key, value))
        return self

    # ── quality ───────────────────────────────────────────────────────────────

    @property
    def is_valid(self) -> bool:
        return bool(self.connections) and bool(self.columns)

    @property
    def calculated_columns(self) -> List[ColumnSpec]:
        return [c for c in self.columns if c.is_calculated]

    @property
    def raw_columns(self) -> List[ColumnSpec]:
        return [c for c in self.columns if not c.is_calculated]

    # ── XML generation ────────────────────────────────────────────────────────

    def to_xml(self, indent: bool = True) -> str:
        """
        Produce the full <datasource> XML string.
        Always returns well-formed XML regardless of indent setting.
        """
        root = self._build_datasource_element()
        tree = ET.ElementTree(root)
        if indent:
            ET.indent(tree, space="  ")
        return ET.tostring(root, encoding="unicode")

    def _build_datasource_element(self) -> ET.Element:
        ds = ET.Element("datasource", {
            "caption":  self.caption,
            "inline":   "true",
            "name":     self.ds_name,
            "version":  TABLEAU_VERSION,
            "xmlns:user": TABLEAU_XMLNS,
        })

        # ── <connection class='federated'> ────────────────────────────────
        conn_el = ET.SubElement(ds, "connection", {"class": "federated"})
        nc_container = ET.SubElement(conn_el, "named-connections")

        for cs in self.connections:
            nc = ET.SubElement(nc_container, "named-connection", {
                "caption": cs.caption,
                "name":    cs.named_connection_name,
            })
            attrs: dict = {
                "class":          cs.conn_class,
                "server":         cs.server,
                "dbname":         cs.dbname,
                "schema":         cs.schema,
                "username":       cs.username,
                "authentication": cs.auth_method,
                "odbc-connect-string-extras": cs.odbc_extras,
                "one-time-sql":   cs.one_time_sql,
            }
            if cs.warehouse:
                attrs["warehouse"] = cs.warehouse
            if cs.role:
                attrs["role"] = cs.role
            ET.SubElement(nc, "connection", attrs)

        # ── <relation> elements ───────────────────────────────────────────
        for rel in self.relations:
            self._append_relation(conn_el, rel)

        # ── <cols> map ────────────────────────────────────────────────────
        if self.col_maps:
            cols_el = ET.SubElement(conn_el, "cols")
            for key, value in self.col_maps:
                k = key if key.startswith("[") else f"[{key}]"
                ET.SubElement(cols_el, "map", {"key": k, "value": value})

        # ── <aliases> ────────────────────────────────────────────────────
        ET.SubElement(ds, "aliases", {"enabled": "yes"})

        # ── <column> elements ─────────────────────────────────────────────
        for col in self.columns:
            self._append_column(ds, col)

        return ds

    def _append_relation(self, parent: ET.Element, rel: RelationSpec) -> None:
        if rel.is_custom_sql:
            r = ET.SubElement(parent, "relation", {
                "name": rel.table_name,
                "type": "text",
            })
            r.text = rel.custom_sql
            return

        if not rel.is_join:
            ET.SubElement(parent, "relation", {
                "name":  rel.table_name,
                "table": f"[{rel.schema}].[{rel.table_name}]",
                "type":  "table",
            })
            return

        # Join relation
        jtype = rel.join_type.lower()
        if jtype not in VALID_JOIN_TYPES:
            jtype = "inner"
        join_el = ET.SubElement(parent, "relation", {
            "join": jtype,
            "type": "join",
        })
        clause = ET.SubElement(join_el, "clause", {"type": "join"})
        expr   = ET.SubElement(clause, "expression", {"op": "="})
        ET.SubElement(expr, "expression", {
            "op": f"[{rel.left_table}].[{rel.left_key}]"
        })
        ET.SubElement(expr, "expression", {
            "op": f"[{rel.right_table}].[{rel.right_key}]"
        })
        ET.SubElement(join_el, "relation", {
            "name":  rel.left_table,
            "table": f"[{rel.schema}].[{rel.left_table}]",
            "type":  "table",
        })
        ET.SubElement(join_el, "relation", {
            "name":  rel.right_table,
            "table": f"[{rel.schema}].[{rel.right_table}]",
            "type":  "table",
        })

    def _append_column(self, parent: ET.Element, col: ColumnSpec) -> None:
        attrs: dict = {
            "datatype": col.datatype,
            "name":     col.xml_name,
            "role":     col.role,
            "type":     col.type_,
        }
        if col.caption:
            attrs["caption"] = col.caption
        if col.hidden:
            attrs["hidden"] = "true"
        if col.aggregation and col.role == "measure":
            attrs["default-role"]        = "measure"
            attrs["type"]                = col.type_
        if col.format_string:
            attrs["default-format"] = col.format_string
        if col.folder_group:
            attrs["folder"]         = col.folder_group

        col_el = ET.SubElement(parent, "column", attrs)

        if col.is_calculated and col.formula:
            ET.SubElement(col_el, "calculation", {
                "class":   "tableau",
                "formula": col.formula,   # ElementTree handles XML escaping
            })

    # ── summary / serialisation ───────────────────────────────────────────────

    def summary(self) -> dict:
        return {
            "datasource_id":   self.datasource_id,
            "caption":         self.caption,
            "ds_name":         self.ds_name,
            "connections":     len(self.connections),
            "relations":       len(self.relations),
            "raw_columns":     len(self.raw_columns),
            "calculated_cols": len(self.calculated_columns),
            "total_columns":   len(self.columns),
            "col_maps":        len(self.col_maps),
            "is_valid":        self.is_valid,
            "timestamp":       self.timestamp,
        }

    def to_dict(self) -> dict:
        return {
            **self.summary(),
            "xml": self.to_xml(),
        }

    def __repr__(self) -> str:
        return (
            f"<TdsDocument ds={self.datasource_id} "
            f"cols={len(self.columns)} valid={self.is_valid}>"
        )
