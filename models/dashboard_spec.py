# models/dashboard_spec.py
"""
DashboardDocument — zone layout model and <dashboard> XML builder for Phase 09.

Tableau dashboard zones use two coordinate systems:
  - Tiled zones:    relative units 0-100,000 (0% to 100% of canvas)
  - Floating zones: absolute pixel values

The primary layouts are computed grids (grid-2x2, grid-3x2, etc.)
which use the tiled coordinate system. figma_layout.csv can override
individual zone positions with pixel values (floating).

Dashboard XML structure:
  <dashboard name="...">
    <size maxheight="768" maxwidth="1366" minheight="768" minwidth="1366" />
    <zones>
      <zone h="100000" id="1" type-v2="layout-basic" w="100000" x="0" y="0">
        <zone h="50000" id="2" name="Sheet 1" type-v2="worksheet" w="50000" x="0" y="0" />
        <zone h="50000" id="3" name="Sheet 2" type-v2="worksheet" w="50000" x="50000" y="0" />
        ...
      </zone>
    </zones>
    <devicelayouts />
    <filters ... />
  </dashboard>

Grid templates (all values in tiled units 0-100,000):
  grid-2x2: 2 cols × 2 rows  — each cell 50,000 × 50,000
  grid-3x2: 3 cols × 2 rows  — each cell 33,333 × 50,000
  grid-2x3: 2 cols × 3 rows  — each cell 50,000 × 33,333
  grid-1x2: 1 col  × 2 rows  — each cell 100,000 × 50,000
  grid-2x1: 2 cols × 1 row   — each cell 50,000 × 100,000
  horizontal: all sheets side-by-side in a single row
  vertical:   all sheets stacked in a single column
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


# ── Zone definitions ───────────────────────────────────────────────────────────

TILED_MAX = 100_000   # Tableau's 0-100,000 unit system

VALID_ZONE_TYPES = {
    "worksheet", "layout-basic", "layout-flow",
    "text", "bitmap", "filter", "paramctrl", "color",
}

# Grid layout → (cols, rows)
GRID_LAYOUTS: Dict[str, Tuple[int, int]] = {
    "grid-2x2":   (2, 2),
    "grid-3x2":   (3, 2),
    "grid-2x3":   (2, 3),
    "grid-3x3":   (3, 3),
    "grid-4x2":   (4, 2),
    "grid-1x2":   (1, 2),
    "grid-2x1":   (2, 1),
    "grid-1x3":   (1, 3),
    "grid-3x1":   (3, 1),
    "horizontal": (0, 1),   # special: auto-cols, 1 row
    "vertical":   (1, 0),   # special: 1 col, auto-rows
}


@dataclass
class ZoneSpec:
    """Represents one <zone> element in a dashboard layout."""
    zone_id:   int
    name:      str             # worksheet name (empty for containers)
    zone_type: str = "worksheet"   # worksheet | layout-basic | text | ...
    x:         int = 0
    y:         int = 0
    w:         int = TILED_MAX
    h:         int = TILED_MAX
    is_fixed:  bool = False    # True → floating zone with pixel coords
    children:  List["ZoneSpec"] = field(default_factory=list)

    @property
    def is_container(self) -> bool:
        return self.zone_type in ("layout-basic", "layout-flow")

    def to_element(self) -> ET.Element:
        attrs: Dict[str, str] = {
            "h":       str(self.h),
            "id":      str(self.zone_id),
            "type-v2": self.zone_type,
            "w":       str(self.w),
            "x":       str(self.x),
            "y":       str(self.y),
        }
        if self.name and not self.is_container:
            attrs["name"] = self.name
        if self.is_fixed:
            attrs["is-fixed"] = "true"

        el = ET.Element("zone", attrs)
        for child in self.children:
            el.append(child.to_element())
        return el


# ── DashboardDocument ──────────────────────────────────────────────────────────

@dataclass
class DashboardDocument:
    """
    Full structured representation of a <dashboard> XML block.

    Serialises to XML via to_xml().

    Gate condition for Phase 10 (TWB Assembler):
        doc.is_valid → True when name is non-empty and zones present
    """
    name:          str
    layout:        str = "grid-2x2"   # grid-NxM | horizontal | vertical
    width_px:      int = 1366
    height_px:     int = 768
    view_id:       str = ""

    zones:         List[ZoneSpec] = field(default_factory=list)
    filter_fields: List[str]     = field(default_factory=list)   # quick-filter refs

    @property
    def is_valid(self) -> bool:
        return bool(self.name) and bool(self.zones)

    def to_xml(self) -> str:
        """Serialise to a <dashboard> XML string."""
        dash = ET.Element("dashboard", {"name": self.name})

        # Canvas size
        ET.SubElement(dash, "size", {
            "maxheight": str(self.height_px),
            "maxwidth":  str(self.width_px),
            "minheight": str(self.height_px),
            "minwidth":  str(self.width_px),
        })

        # Outer container zone (wraps all worksheet zones)
        zones_el = ET.SubElement(dash, "zones")
        outer = ZoneSpec(
            zone_id=1,
            name="",
            zone_type="layout-basic",
            x=0, y=0,
            w=TILED_MAX, h=TILED_MAX,
            children=self.zones,
        )
        zones_el.append(outer.to_element())

        # Device layouts placeholder (required by Tableau schema)
        ET.SubElement(dash, "devicelayouts")

        # Quick-filter zone placeholders
        if self.filter_fields:
            for i, field_ref in enumerate(self.filter_fields, start=100):
                ET.SubElement(dash, "filter", {
                    "column":    field_ref,
                    "filter-id": str(i),
                })

        ET.indent(dash, space="  ")
        return ET.tostring(dash, encoding="unicode")

    def to_dict(self) -> dict:
        return {
            "view_id":      self.view_id,
            "name":         self.name,
            "layout":       self.layout,
            "width_px":     self.width_px,
            "height_px":    self.height_px,
            "zone_count":   len(self.zones),
            "is_valid":     self.is_valid,
            "xml":          self.to_xml(),
        }

    def summary(self) -> dict:
        return {
            "view_id":   self.view_id,
            "name":      self.name,
            "layout":    self.layout,
            "zones":     len(self.zones),
            "is_valid":  self.is_valid,
        }

    def __repr__(self) -> str:
        return (
            f"<DashboardDocument name={self.name!r} "
            f"layout={self.layout!r} zones={len(self.zones)} valid={self.is_valid}>"
        )


# ── Zone grid builders ─────────────────────────────────────────────────────────

def build_grid_zones(
    sheet_names: List[str],
    layout:      str,
    *,
    start_id:    int = 2,
) -> List[ZoneSpec]:
    """
    Compute tiled zone coordinates for a named grid layout.

    Returns a flat list of ZoneSpec (one per worksheet).
    Excess capacity is filled with empty placeholder zones.
    If sheet_names exceed grid capacity, remaining sheets are appended
    to the last row with equal distribution.
    """
    cols, rows = GRID_LAYOUTS.get(layout.lower(), (2, 2))

    n = len(sheet_names)

    # Handle special auto-dimension layouts
    if cols == 0:      # horizontal
        cols = n or 1
        rows = 1
    elif rows == 0:    # vertical
        rows = n or 1
        cols = 1

    # Ensure grid is large enough for all sheets
    capacity = cols * rows
    while capacity < n:
        rows += 1
        capacity = cols * rows

    cell_w = TILED_MAX // cols
    cell_h = TILED_MAX // rows

    zones: List[ZoneSpec] = []
    zone_id = start_id

    for idx, name in enumerate(sheet_names):
        col = idx % cols
        row = idx // cols
        zones.append(ZoneSpec(
            zone_id=zone_id,
            name=name,
            zone_type="worksheet",
            x=col * cell_w,
            y=row * cell_h,
            w=cell_w,
            h=cell_h,
        ))
        zone_id += 1

    return zones


def build_figma_zones(
    sheet_names:   List[str],
    figma_tokens:  List[dict],
    *,
    start_id:      int = 2,
) -> List[ZoneSpec]:
    """
    Build floating zones from figma_layout.csv tokens.

    figma_tokens is a list of dicts with keys:
      element_id, element_type, name, value, zone_view_id,
      x_px, y_px, w_px, h_px

    Only tokens with element_type='zone' and a matching zone_view_id
    (matched against sheet name or view_id) are used.
    Unmatched sheets fall back to default grid positioning.
    """
    zone_tokens = [t for t in figma_tokens if t.get("element_type") == "zone"]

    # Build lookup: zone_view_id → token
    by_view_id: Dict[str, dict] = {}
    by_name:    Dict[str, dict] = {}
    for tok in zone_tokens:
        vid = (tok.get("zone_view_id") or "").strip()
        nm  = (tok.get("name") or "").strip().lower()
        if vid:
            by_view_id[vid] = tok
        if nm:
            by_name[nm] = tok

    zones: List[ZoneSpec] = []
    zone_id = start_id

    # Build fallback grid for unmatched sheets
    fallback_grid = build_grid_zones(sheet_names, "grid-2x2", start_id=start_id + 1000)
    fallback_map  = {z.name: z for z in fallback_grid}

    for name in sheet_names:
        tok = by_view_id.get(name) or by_name.get(name.lower())

        if tok and _has_valid_coords(tok):
            zones.append(ZoneSpec(
                zone_id=zone_id,
                name=name,
                zone_type="worksheet",
                x=int(tok.get("x_px", 0) or 0),
                y=int(tok.get("y_px", 0) or 0),
                w=int(tok.get("w_px", 683) or 683),
                h=int(tok.get("h_px", 384) or 384),
                is_fixed=True,
            ))
        else:
            # Use fallback tiled position
            fb = fallback_map.get(name)
            if fb:
                zones.append(ZoneSpec(
                    zone_id=zone_id,
                    name=name,
                    zone_type="worksheet",
                    x=fb.x, y=fb.y, w=fb.w, h=fb.h,
                ))
            else:
                # Last resort: full canvas
                zones.append(ZoneSpec(
                    zone_id=zone_id,
                    name=name,
                    zone_type="worksheet",
                ))
        zone_id += 1

    return zones


def _has_valid_coords(tok: dict) -> bool:
    for k in ("x_px", "y_px", "w_px", "h_px"):
        try:
            if not tok.get(k) and tok.get(k) != 0:
                return False
            int(tok[k])
        except (TypeError, ValueError):
            return False
    w = int(tok.get("w_px", 0) or 0)
    h = int(tok.get("h_px", 0) or 0)
    return w > 0 and h > 0
