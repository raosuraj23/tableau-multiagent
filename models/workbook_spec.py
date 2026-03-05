# models/workbook_spec.py
"""
WorkbookDocument - Final TWB XML assembler for Phase 10/11.

Assembles a complete Tableau workbook XML file from the parts produced
by previous pipeline phases:
  - tds_documents   (SemanticModelAgent, Phase 06)
  - worksheets_xml  (TableauModelAgent,  Phase 08)
  - dashboards_xml  (DashboardGenAgent,  Phase 09)

TWB root structure assembled here:
  <?xml version='1.0' encoding='utf-8' ?>
  <workbook source-build='...' source-platform='win' version='18.1' ...>
    <preferences>           -- color palettes (optional)
    <datasources>           -- Parameters datasource + all TDS blocks
    <worksheets>            -- all worksheet blocks
    <dashboards>            -- all dashboard blocks
    <windows>               -- window state (minimal stub)
  </workbook>

Validation checks (before write):
  - Root tag is <workbook>
  - <datasources> has at least one <datasource>
  - <worksheets> has at least one <worksheet>
  - <dashboards> has at least one <dashboard>
  - All worksheet names referenced in dashboards exist in worksheets
  - XML is well-formed (parseable by lxml / ET)

Output: .twb file written to tableau/output/<workbook_name>.twb
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

TABLEAU_VERSION   = "18.1"
SOURCE_BUILD      = "2024.1.0 (20241.24.0312.0000)"
SOURCE_PLATFORM   = "win"
XML_NAMESPACE_USER = "http://www.tableausoftware.com/xml/user"

# Minimal Parameters datasource that Tableau requires even if no parameters defined
_PARAMETERS_DS = """<datasource hasconnection='false' inline='true' name='Parameters' version='18.1'>
</datasource>"""


@dataclass
class WorkbookValidationResult:
    is_valid:  bool              = True
    errors:    List[str]         = field(default_factory=list)
    warnings:  List[str]         = field(default_factory=list)

    def add_error(self, msg: str) -> "WorkbookValidationResult":
        self.errors.append(msg)
        self.is_valid = False
        return self

    def add_warning(self, msg: str) -> "WorkbookValidationResult":
        self.warnings.append(msg)
        return self


@dataclass
class WorkbookDocument:
    """
    Assembled TWB workbook document.

    Attributes set after build():
      name         -- workbook display name
      twb_xml      -- full XML string
      twb_path     -- Path where file was written (or None if dry_run / not written)
      validation   -- WorkbookValidationResult
    """
    name:            str
    tableau_version: str = TABLEAU_VERSION
    source_platform: str = SOURCE_PLATFORM
    source_build:    str = SOURCE_BUILD

    # Fragment lists (XML strings)
    tds_xml_list:       List[str] = field(default_factory=list)
    worksheet_xml_list: List[str] = field(default_factory=list)
    dashboard_xml_list: List[str] = field(default_factory=list)
    palette_xml_list:   List[str] = field(default_factory=list)

    # Output
    twb_xml:    Optional[str]  = None
    twb_path:   Optional[Path] = None
    validation: WorkbookValidationResult = field(
        default_factory=WorkbookValidationResult
    )

    @property
    def is_valid(self) -> bool:
        return self.validation.is_valid

    def build(self) -> "WorkbookDocument":
        """Assemble all fragments into twb_xml. Populates self.twb_xml."""
        root = ET.Element("workbook", {
            "source-build":    self.source_build,
            "source-platform": self.source_platform,
            "version":         self.tableau_version,
            "xmlns:user":      XML_NAMESPACE_USER,
        })

        # <preferences>
        prefs = ET.SubElement(root, "preferences")
        for palette_xml in self.palette_xml_list:
            try:
                el = ET.fromstring(palette_xml)
                prefs.append(el)
            except ET.ParseError:
                pass

        # <datasources>
        ds_container = ET.SubElement(root, "datasources")
        # Always prepend Parameters datasource
        try:
            ds_container.append(ET.fromstring(_PARAMETERS_DS))
        except ET.ParseError:
            pass
        for tds_xml in self.tds_xml_list:
            try:
                el = ET.fromstring(tds_xml)
                ds_container.append(el)
            except ET.ParseError as e:
                self.validation.add_error(f"Unparseable TDS XML: {e}")

        # <worksheets>
        ws_container = ET.SubElement(root, "worksheets")
        for ws_xml in self.worksheet_xml_list:
            try:
                el = ET.fromstring(ws_xml)
                ws_container.append(el)
            except ET.ParseError as e:
                self.validation.add_error(f"Unparseable worksheet XML: {e}")

        # <dashboards>
        db_container = ET.SubElement(root, "dashboards")
        for db_xml in self.dashboard_xml_list:
            try:
                el = ET.fromstring(db_xml)
                db_container.append(el)
            except ET.ParseError as e:
                self.validation.add_error(f"Unparseable dashboard XML: {e}")

        # <windows> stub — Tableau expects this element to be present
        ET.SubElement(root, "windows")

        ET.indent(root, space="  ")
        self.twb_xml = (
            "<?xml version='1.0' encoding='utf-8' ?>\n"
            + ET.tostring(root, encoding="unicode")
        )
        return self

    def validate(self) -> WorkbookValidationResult:
        """Run structural validation checks against twb_xml."""
        if not self.twb_xml:
            self.validation.add_error("twb_xml is empty — call build() first.")
            return self.validation

        try:
            root = ET.fromstring(
                self.twb_xml.replace("<?xml version='1.0' encoding='utf-8' ?>", "", 1)
            )
        except ET.ParseError as e:
            self.validation.add_error(f"XML is not well-formed: {e}")
            return self.validation

        # Root tag
        if root.tag != "workbook":
            self.validation.add_error(
                f"Root element must be <workbook>, got <{root.tag}>"
            )

        # Required version attribute
        if not root.attrib.get("version"):
            self.validation.add_warning("Workbook missing version attribute.")

        # Datasources
        ds = root.find("datasources")
        if ds is None or len(ds) == 0:
            self.validation.add_error("<datasources> is missing or empty.")
        elif len(ds) < 2:
            # At minimum Parameters + 1 real datasource
            self.validation.add_warning(
                "Only Parameters datasource found — no data source connected."
            )

        # Worksheets
        ws = root.find("worksheets")
        if ws is None or len(ws) == 0:
            self.validation.add_error("<worksheets> is missing or empty.")

        # Dashboards
        db = root.find("dashboards")
        if db is None or len(db) == 0:
            self.validation.add_warning(
                "<dashboards> is missing or empty — workbook has no dashboards."
            )

        # Cross-reference: dashboard worksheet names must exist in worksheets
        if ws is not None and db is not None:
            ws_names = {el.attrib.get("name", "") for el in ws}
            for db_el in db:
                for ws_ref in db_el.findall("worksheets/worksheet"):
                    ref_name = ws_ref.attrib.get("name", "")
                    if ref_name and ref_name not in ws_names:
                        self.validation.add_warning(
                            f"Dashboard '{db_el.attrib.get('name')}' references "
                            f"worksheet '{ref_name}' which is not in <worksheets>."
                        )

        return self.validation

    def write(self, output_dir: Path, dry_run: bool = False) -> Optional[Path]:
        """Write twb_xml to <output_dir>/<name>.twb. Returns path or None."""
        if not self.twb_xml:
            return None
        if dry_run:
            return None
        safe_name = _safe_filename(self.name) + ".twb"
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / safe_name
        path.write_text(self.twb_xml, encoding="utf-8")
        self.twb_path = path
        return path

    def to_dict(self) -> dict:
        return {
            "name":              self.name,
            "tableau_version":   self.tableau_version,
            "datasource_count":  len(self.tds_xml_list),
            "worksheet_count":   len(self.worksheet_xml_list),
            "dashboard_count":   len(self.dashboard_xml_list),
            "is_valid":          self.is_valid,
            "twb_path":          str(self.twb_path) if self.twb_path else None,
            "xml_length":        len(self.twb_xml) if self.twb_xml else 0,
            "validation_errors": self.validation.errors,
            "validation_warnings": self.validation.warnings,
        }

    def __repr__(self) -> str:
        return (
            f"<WorkbookDocument name={self.name!r} "
            f"valid={self.is_valid} "
            f"ds={len(self.tds_xml_list)} "
            f"ws={len(self.worksheet_xml_list)} "
            f"db={len(self.dashboard_xml_list)}>"
        )


# ── Helpers ────────────────────────────────────────────────────────────────────

def _safe_filename(name: str) -> str:
    """Replace filesystem-unsafe characters with underscores."""
    unsafe = r'<>:"/\\|?* '
    result = "".join("_" if c in unsafe else c for c in name)
    return result.strip("_") or "workbook"
