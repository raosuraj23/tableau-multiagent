"""
TWB Generator Agent
===================
Phase 3 of the pipeline.

Responsibilities:
- Assembles a complete, valid Tableau workbook XML (.twb) from:
    • Published datasource connection (sqlproxy pattern — Metadata API discovered)
    • Calculated fields from MSTR Metric Mapper Agent
    • Worksheet XML per dashboard_requirements.csv
    • Dashboard zone layout from Figma Design Agent tokens
    • Color palette from Figma design tokens
- Validates the generated XML for well-formedness
- Writes the .twb file to models/twb/<workbook_name>.twb

All XML is generated deterministically in Python — the LLM is NOT used here.
"""

from __future__ import annotations

import html
from pathlib import Path
from typing import Any, Dict, List, Optional
import xml.etree.ElementTree as ET

from agents.base_agent import AgentResult, BaseAgent


# ─────────────────────────────────────────────
# Chart type → Tableau mark class
# ─────────────────────────────────────────────
CHART_TYPE_MAP: Dict[str, str] = {
    "bar":      "Bar",
    "line":     "Line",
    "area":     "Area",
    "pie":      "Pie",
    "scatter":  "Circle",
    "text":     "Text",
    "map":      "Map",
    "shape":    "Shape",
    "auto":     "Automatic",
}

# Tableau zone type-v2 for container vs worksheet
CONTAINER_ZONE = "layout-basic"
WORKSHEET_ZONE = "worksheet"


class TwbGeneratorAgent(BaseAgent):
    """
    Deterministically generates a complete Tableau workbook XML (.twb)
    from the field mapping and design tokens produced by upstream agents.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__("twb_generator_agent", config)
        self.output_dir = Path(config.get("twb_output_dir", "models/twb"))
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.tableau_version: str = config.get("tableau_version", "18.1")

    # ──────────────────────────────────────────
    # BaseAgent interface
    # ──────────────────────────────────────────

    def validate_input(self, state: Dict[str, Any]) -> List[str]:
        errors: List[str] = []
        if not state.get("field_mapping"):
            errors.append("field_mapping missing — run MstrMetricMapperAgent first")
        if not state.get("design_tokens"):
            errors.append("design_tokens missing — run FigmaDesignAgent first")
        spec = state.get("project_spec", {})
        if not spec.get("workbook_name"):
            errors.append("project_spec.workbook_name is empty")
        if not spec.get("published_datasource_name"):
            errors.append(
                "project_spec.published_datasource_name is empty — "
                "required for published datasource connection"
            )
        return errors

    def run(self, state: Dict[str, Any]) -> AgentResult:
        spec: Dict[str, Any] = state["project_spec"]
        field_mapping: Dict[str, Any] = state["field_mapping"]
        design_tokens: Dict[str, Any] = state["design_tokens"]
        warnings: List[str] = []

        # Derive internal datasource name (used for field references)
        ds_internal_name = f"sqlproxy.{_short_hash(spec['published_datasource_name'])}"

        # ── Build XML sections ──
        palette_xml = self._build_color_palette(design_tokens)
        datasource_xml = self._build_published_datasource(
            spec, ds_internal_name, field_mapping
        )
        worksheet_xmls = self._build_worksheets(
            spec["dashboard_requirements"], ds_internal_name, field_mapping
        )
        dashboard_reqs = [
            r for r in spec["dashboard_requirements"]
            if r.get("view_type", "").lower() == "dashboard"
        ]
        dashboard_xml = self._build_dashboards(
            dashboard_reqs, spec["dashboard_requirements"], design_tokens
        )
        if not dashboard_reqs:
            warnings.append(
                "No rows with view_type='dashboard' found in "
                "dashboard_requirements.csv — generating worksheets only."
            )

        # ── Assemble final TWB ──
        twb_content = self._assemble_twb(
            spec=spec,
            palette_xml=palette_xml,
            datasource_xml=datasource_xml,
            worksheet_xmls=worksheet_xmls,
            dashboard_xml=dashboard_xml,
        )

        # ── Validate XML well-formedness ──
        xml_errors = self._validate_xml(twb_content)
        if xml_errors:
            return AgentResult(
                agent_id=self.agent_id,
                phase="twb_generation",
                status="failed",
                errors=xml_errors,
                warnings=warnings,
            )

        # ── Write to disk ──
        workbook_name = spec["workbook_name"].replace(" ", "_")
        twb_path = self.output_dir / f"{workbook_name}.twb"
        twb_path.write_text(twb_content, encoding="utf-8")
        self.logger.info("twb_written", path=str(twb_path), size_bytes=len(twb_content))

        status = "warning" if warnings else "success"
        return AgentResult(
            agent_id=self.agent_id,
            phase="twb_generation",
            status=status,
            output={"twb_path": str(twb_path)},
            warnings=warnings,
        )

    # ──────────────────────────────────────────
    # Color palette
    # ──────────────────────────────────────────

    def _build_color_palette(self, design_tokens: Dict[str, Any]) -> str:
        colors = design_tokens.get("colors", [])
        if not colors:
            return ""
        color_els = "\n    ".join(
            f"<color>{c['hex']}</color>" for c in colors
        )
        return (
            f'<color-palette name="Figma Brand Colors" type="regular">\n'
            f"    {color_els}\n"
            f"  </color-palette>"
        )

    # ──────────────────────────────────────────
    # Published datasource connection (sqlproxy)
    # ──────────────────────────────────────────

    def _build_published_datasource(
        self,
        spec: Dict[str, Any],
        ds_internal_name: str,
        field_mapping: Dict[str, Any],
    ) -> str:
        """
        Generate the <datasource> element using the sqlproxy pattern.
        This references an already-published Tableau datasource on Tableau Cloud.
        No direct DB credentials are embedded — auth is handled by Tableau Cloud.
        """
        ds_name = spec["published_datasource_name"]
        server_url = spec["tableau_server_url"].rstrip("/")
        site = spec["tableau_site"]
        version = self.tableau_version

        # Build <column> elements for calculated fields
        calc_columns = "\n  ".join(
            self._calc_field_xml(cf) for cf in field_mapping.get("calculated_fields", [])
        )

        # Build <column> elements for mapped dimensions
        dim_columns = "\n  ".join(
            self._dim_column_xml(dc) for dc in field_mapping.get("dimension_columns", [])
        )

        return f"""<datasource caption='{_esc(ds_name)}' inline='true'
              name='{ds_internal_name}' version='{version}'>
  <repository-location id='{_esc(ds_name)}' path='/datasources'
                       revision='1.0' site='{_esc(site)}' />
  <connection channel='http' class='sqlproxy'
              dbname='{_esc(ds_name)}'
              directory='/dataserver'
              port='443'
              server='{_esc(server_url)}'
              username='' />
  <aliases enabled='yes' />
  {dim_columns}
  {calc_columns}
</datasource>"""

    def _calc_field_xml(self, cf: Dict[str, str]) -> str:
        formula = html.escape(cf.get("formula", ""), quote=True)
        caption_attr = f"caption='{_esc(cf['caption'])}' " if cf.get("caption") else ""
        fmt = f" default-format='{_esc(cf['format_string'])}'" if cf.get("format_string") else ""
        return (
            f"<column {caption_attr}"
            f"datatype='{cf['datatype']}' "
            f"name='{cf['calc_id']}' "
            f"role='{cf['role']}' "
            f"type='{cf['type']}'"
            f"{fmt}>\n"
            f"  <calculation class='tableau' formula='{formula}' />\n"
            f"</column>"
        )

    def _dim_column_xml(self, dc: Dict[str, str]) -> str:
        hidden_attr = " hidden='true'" if dc.get("hidden", "false") == "true" else ""
        caption_attr = f"caption='{_esc(dc['caption'])}' " if dc.get("caption") else ""
        return (
            f"<column {caption_attr}"
            f"datatype='{dc['datatype']}' "
            f"name='{_esc(dc['name'])}' "
            f"role='{dc['role']}' "
            f"type='{dc['type']}'"
            f"{hidden_attr} />"
        )

    # ──────────────────────────────────────────
    # Worksheets
    # ──────────────────────────────────────────

    def _build_worksheets(
        self,
        requirements: List[Dict[str, Any]],
        ds_name: str,
        field_mapping: Dict[str, Any],
    ) -> List[str]:
        worksheets: List[str] = []
        for req in requirements:
            if req.get("view_type", "").lower() != "worksheet":
                continue
            worksheets.append(
                self._worksheet_xml(req, ds_name, field_mapping)
            )
        return worksheets

    def _worksheet_xml(
        self,
        req: Dict[str, Any],
        ds_name: str,
        field_mapping: Dict[str, Any],
    ) -> str:
        name = req.get("view_name", "Sheet")
        chart_type = CHART_TYPE_MAP.get(
            req.get("chart_type", "bar").lower(), "Bar"
        )

        rows_shelf = self._field_ref(req.get("rows", ""), ds_name, field_mapping)
        cols_shelf = self._field_ref(req.get("columns", ""), ds_name, field_mapping)
        color_enc = self._encoding_xml(
            "color", req.get("color", ""), ds_name, field_mapping
        )
        size_enc = self._encoding_xml(
            "size", req.get("size", ""), ds_name, field_mapping
        )
        label_enc = self._encoding_xml(
            "label", req.get("label", ""), ds_name, field_mapping
        )

        # Sort
        sort_xml = ""
        if req.get("sort_by"):
            direction = req.get("sort_direction", "desc")
            sort_field = self._field_ref(req["sort_by"], ds_name, field_mapping)
            sort_xml = (
                f"<sort class='computed' direction='{direction}' "
                f"using='{sort_field}' />"
            )

        encodings_block = "\n          ".join(
            filter(None, [color_enc, size_enc, label_enc])
        )
        if encodings_block:
            encodings_block = f"<encodings>\n          {encodings_block}\n        </encodings>"

        return f"""<worksheet name='{_esc(name)}'>
  <table>
    <view>
      <datasources>
        <datasource name='{_esc(ds_name)}' />
      </datasources>
      <rows>{rows_shelf}</rows>
      <cols>{cols_shelf}</cols>
      {sort_xml}
    </view>
    <panes>
      <pane>
        <mark class='{chart_type}' />
        {encodings_block}
      </pane>
    </panes>
  </table>
</worksheet>"""

    def _field_ref(
        self,
        field_spec: str,
        ds_name: str,
        field_mapping: Dict[str, Any],
    ) -> str:
        """Convert a field spec string into a Tableau shelf field reference."""
        if not field_spec:
            return ""
        field_spec = field_spec.strip()
        if not field_spec:
            return ""

        # Detect aggregation prefix e.g. SUM(Sales), MONTH(Order Date)
        import re
        agg_match = re.match(r"^(SUM|AVG|COUNT|MIN|MAX|MONTH|YEAR|DAY)\((.+)\)$",
                             field_spec, re.IGNORECASE)
        if agg_match:
            agg = agg_match.group(1).lower()
            inner = agg_match.group(2).strip()
            qualifier = "qk"  # quantitative key
            return f"[{ds_name}].[{agg}:{inner}:{qualifier}]"

        # Check if it's a calculated field caption
        for cf in field_mapping.get("calculated_fields", []):
            if cf.get("caption", "").lower() == field_spec.lower():
                qualifier = "qk" if cf.get("role") == "measure" else "nk"
                return f"[{ds_name}].[none:{cf['calc_id']}:{qualifier}]"

        # Dimension (nominal key)
        return f"[{ds_name}].[none:{field_spec}:nk]"

    def _encoding_xml(
        self,
        enc_type: str,
        field_spec: str,
        ds_name: str,
        field_mapping: Dict[str, Any],
    ) -> str:
        if not field_spec:
            return ""
        ref = self._field_ref(field_spec, ds_name, field_mapping)
        if not ref:
            return ""
        return f"<{enc_type} column='{ref}' />"

    # ──────────────────────────────────────────
    # Dashboards
    # ──────────────────────────────────────────

    def _build_dashboards(
        self,
        dashboard_reqs: List[Dict[str, Any]],
        all_reqs: List[Dict[str, Any]],
        design_tokens: Dict[str, Any],
    ) -> str:
        if not dashboard_reqs:
            return ""
        xmls = []
        for db_req in dashboard_reqs:
            xmls.append(
                self._dashboard_xml(db_req, all_reqs, design_tokens)
            )
        return "\n".join(xmls)

    def _dashboard_xml(
        self,
        db_req: Dict[str, Any],
        all_reqs: List[Dict[str, Any]],
        design_tokens: Dict[str, Any],
    ) -> str:
        name = db_req.get("view_name", "Dashboard")
        width = db_req.get("width_px", "1366")
        height = db_req.get("height_px", "768")

        # Worksheets included in this dashboard
        included_ids = [
            v.strip()
            for v in db_req.get("views_in_dashboard", "").split("|")
            if v.strip()
        ]
        included_views = [
            r for r in all_reqs
            if r.get("view_id") in included_ids
            and r.get("view_type", "").lower() == "worksheet"
        ]

        zones = self._build_zones(included_views, design_tokens, int(width), int(height))

        return f"""<dashboard name='{_esc(name)}'>
  <size maxheight='{height}' maxwidth='{width}'
        minheight='{height}' minwidth='{width}' />
  <zones>
    <zone h='100000' id='1' type-v2='{CONTAINER_ZONE}' w='100000' x='0' y='0'>
      {zones}
    </zone>
  </zones>
</dashboard>"""

    def _build_zones(
        self,
        views: List[Dict[str, Any]],
        design_tokens: Dict[str, Any],
        dashboard_w: int,
        dashboard_h: int,
    ) -> str:
        layouts = design_tokens.get("layouts", [])
        # layouts[0] is the parent dashboard frame; subsequent are child frames
        parent = layouts[0] if layouts else {"x": 0, "y": 0, "width": dashboard_w, "height": dashboard_h}
        child_layouts = layouts[1:]

        zone_xmls: List[str] = []
        for i, view in enumerate(views):
            zone_id = i + 5
            if i < len(child_layouts):
                frame = child_layouts[i]
                # Floating zones use pixel coordinates
                x = round(frame["x"] - parent["x"])
                y = round(frame["y"] - parent["y"])
                w = round(frame["width"])
                h = round(frame["height"])
                is_fixed = "true"
            else:
                # Fallback: compute tiled grid position
                cols = 2
                col_idx = i % cols
                row_idx = i // cols
                rows_count = (len(views) + cols - 1) // cols
                w_unit = 100000 // cols
                h_unit = 100000 // rows_count
                x = col_idx * w_unit
                y = row_idx * h_unit
                w = w_unit
                h = h_unit
                is_fixed = "false"

            view_name = view.get("view_name", "")
            zone_xmls.append(
                f"<zone h='{h}' id='{zone_id}' is-fixed='{is_fixed}' "
                f"name='{_esc(view_name)}' "
                f"type-v2='{WORKSHEET_ZONE}' "
                f"w='{w}' x='{x}' y='{y}' />"
            )

        return "\n      ".join(zone_xmls)

    # ──────────────────────────────────────────
    # Final TWB assembly
    # ──────────────────────────────────────────

    def _assemble_twb(
        self,
        spec: Dict[str, Any],
        palette_xml: str,
        datasource_xml: str,
        worksheet_xmls: List[str],
        dashboard_xml: str,
    ) -> str:
        version = self.tableau_version
        worksheets_block = "\n".join(worksheet_xmls)
        preferences_block = f"<preferences>\n  {palette_xml}\n</preferences>" if palette_xml else "<preferences />"

        return f"""<?xml version='1.0' encoding='utf-8' ?>
<workbook source-build='2024.1.0 (20241.24.0312.1234)'
          source-platform='win'
          version='{version}'
          xmlns:user='http://www.tableausoftware.com/xml/user'>
  {preferences_block}
  <datasources>
    {datasource_xml}
  </datasources>
  <worksheets>
    {worksheets_block}
  </worksheets>
  <dashboards>
    {dashboard_xml}
  </dashboards>
</workbook>"""

    # ──────────────────────────────────────────
    # XML validation
    # ──────────────────────────────────────────

    def _validate_xml(self, content: str) -> List[str]:
        try:
            ET.fromstring(content)
            return []
        except ET.ParseError as exc:
            return [f"Generated TWB XML is not well-formed: {exc}"]


# ──────────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────────

def _esc(value: str) -> str:
    """Escape a string for use in an XML attribute value."""
    return html.escape(str(value), quote=True)


def _short_hash(value: str) -> str:
    """Generate a short deterministic hex string from a value."""
    import hashlib
    return hashlib.md5(value.encode()).hexdigest()[:16]  # noqa: S324
