# tests/unit/test_dashboard_agent.py
"""
Unit tests for DashboardGenAgent (Phase 09) and DashboardSpec models.

Test count target: ~70 tests
Coverage: ZoneSpec, DashboardDocument, build_grid_zones, build_figma_zones,
          DashboardGenAgent.validate_input, run(), pipeline integration.
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any, Dict, List
import pytest

from agents.base_agent import AgentStatus, PhaseContext
from agents.dashboard_agent import (
    DashboardGenAgent,
    _find_ws_name_for_view_id,
    _resolve_sheet_names,
)
from models.dashboard_spec import (
    GRID_LAYOUTS,
    TILED_MAX,
    DashboardDocument,
    ZoneSpec,
    build_figma_zones,
    build_grid_zones,
)


# ── Fixtures ───────────────────────────────────────────────────────────────────

FOUR_SHEETS  = ["Sales by Sub-Category", "Monthly Sales Trend",
                "Profit by Region", "Sales by Segment"]
SAMPLE_WS_XML = [{"name": s, "xml": f"<worksheet name='{s}'/>", "is_valid": True}
                 for s in FOUR_SHEETS]


def _agent(cfg: dict = None) -> DashboardGenAgent:
    return DashboardGenAgent(config=cfg or {})


def parse_xml(xml_str: str) -> ET.Element:
    return ET.fromstring(xml_str)


def _pipeline_state(extra: dict = None) -> Dict[str, Any]:
    """Build full Phase 01→09 pipeline state via dry-run."""
    from agents.base_agent import PhaseContext
    from agents.intake_agent import IntakeAgent
    from agents.validation_agent import ValidationAgent
    from agents.connectivity_agent import ConnectivityAgent
    from agents.profiler_agent import ProfilerAgent
    from agents.conversion_agent import DataConversionAgent
    from agents.semantic_agent import SemanticModelAgent
    from agents.tableau_model_agent import TableauModelAgent

    dry_ctx = PhaseContext(project_id="proj_001", run_id="test", dry_run=True)

    state: dict = {}
    state.update(IntakeAgent(config={"csv_dir": "csv_inputs"}).execute({}).output)
    state.update(ValidationAgent().execute(state).output)
    state.update(ConnectivityAgent(context=dry_ctx).execute(state).output)
    state.update(ProfilerAgent(context=dry_ctx).execute(state).output)
    state.update(DataConversionAgent().execute(state).output)
    state.update(SemanticModelAgent().execute(state).output)
    state.update(TableauModelAgent().execute(state).output)

    if extra:
        state.update(extra)
    return state


# ── TestZoneSpec ───────────────────────────────────────────────────────────────

class TestZoneSpec:

    def test_worksheet_zone_element(self):
        z = ZoneSpec(zone_id=2, name="Sheet 1", zone_type="worksheet",
                     x=0, y=0, w=50000, h=50000)
        el = z.to_element()
        assert el.tag == "zone"
        assert el.attrib["name"] == "Sheet 1"
        assert el.attrib["type-v2"] == "worksheet"
        assert el.attrib["id"] == "2"

    def test_zone_coordinates_in_element(self):
        z = ZoneSpec(zone_id=3, name="S", zone_type="worksheet",
                     x=50000, y=0, w=50000, h=50000)
        el = z.to_element()
        assert el.attrib["x"] == "50000"
        assert el.attrib["w"] == "50000"

    def test_container_zone_no_name_attribute(self):
        z = ZoneSpec(zone_id=1, name="", zone_type="layout-basic",
                     x=0, y=0, w=TILED_MAX, h=TILED_MAX)
        el = z.to_element()
        assert "name" not in el.attrib

    def test_floating_zone_has_is_fixed(self):
        z = ZoneSpec(zone_id=5, name="KPI", zone_type="worksheet",
                     x=10, y=10, w=200, h=120, is_fixed=True)
        el = z.to_element()
        assert el.attrib.get("is-fixed") == "true"

    def test_tiled_zone_no_is_fixed(self):
        z = ZoneSpec(zone_id=2, name="Sheet", zone_type="worksheet")
        el = z.to_element()
        assert "is-fixed" not in el.attrib

    def test_container_with_children(self):
        child = ZoneSpec(zone_id=2, name="Sheet 1", zone_type="worksheet",
                         x=0, y=0, w=50000, h=100000)
        parent = ZoneSpec(zone_id=1, name="", zone_type="layout-basic",
                          x=0, y=0, w=TILED_MAX, h=TILED_MAX, children=[child])
        el = parent.to_element()
        assert len(list(el)) == 1
        assert list(el)[0].attrib["name"] == "Sheet 1"

    def test_is_container_for_layout_basic(self):
        z = ZoneSpec(zone_id=1, name="", zone_type="layout-basic")
        assert z.is_container is True

    def test_is_container_false_for_worksheet(self):
        z = ZoneSpec(zone_id=1, name="Sheet", zone_type="worksheet")
        assert z.is_container is False


# ── TestBuildGridZones ────────────────────────────────────────────────────────

class TestBuildGridZones:

    def test_grid_2x2_four_zones(self):
        zones = build_grid_zones(FOUR_SHEETS, "grid-2x2")
        assert len(zones) == 4

    def test_grid_2x2_cell_dimensions(self):
        zones = build_grid_zones(FOUR_SHEETS, "grid-2x2")
        for z in zones:
            assert z.w == 50000
            assert z.h == 50000

    def test_grid_2x2_positions(self):
        zones = build_grid_zones(FOUR_SHEETS, "grid-2x2")
        coords = {(z.x, z.y) for z in zones}
        expected = {(0, 0), (50000, 0), (0, 50000), (50000, 50000)}
        assert coords == expected

    def test_grid_2x2_names_preserved(self):
        zones = build_grid_zones(FOUR_SHEETS, "grid-2x2")
        names = [z.name for z in zones]
        assert names == FOUR_SHEETS

    def test_grid_3x2_cell_dimensions(self):
        zones = build_grid_zones(["A", "B", "C", "D", "E", "F"], "grid-3x2")
        for z in zones:
            assert z.w == 33333   # 100000 // 3
            assert z.h == 50000

    def test_grid_1x2_full_width(self):
        zones = build_grid_zones(["A", "B"], "grid-1x2")
        for z in zones:
            assert z.w == TILED_MAX

    def test_horizontal_layout(self):
        zones = build_grid_zones(["A", "B", "C"], "horizontal")
        assert len(zones) == 3
        for z in zones:
            assert z.y == 0   # all in one row

    def test_vertical_layout(self):
        zones = build_grid_zones(["A", "B", "C"], "vertical")
        assert len(zones) == 3
        for z in zones:
            assert z.x == 0   # all in one column

    def test_overflow_adds_rows(self):
        # 5 sheets in a 2x2 grid → expands to 2x3
        zones = build_grid_zones(["A", "B", "C", "D", "E"], "grid-2x2")
        assert len(zones) == 5

    def test_start_id_applied(self):
        zones = build_grid_zones(["A", "B"], "grid-2x1", start_id=10)
        ids = [z.zone_id for z in zones]
        assert ids[0] == 10
        assert ids[1] == 11

    def test_all_zones_are_worksheet_type(self):
        zones = build_grid_zones(FOUR_SHEETS, "grid-2x2")
        for z in zones:
            assert z.zone_type == "worksheet"

    def test_empty_sheet_list(self):
        zones = build_grid_zones([], "grid-2x2")
        assert zones == []

    def test_single_sheet(self):
        zones = build_grid_zones(["Only Sheet"], "grid-2x2")
        assert len(zones) == 1
        assert zones[0].name == "Only Sheet"


# ── TestBuildFigmaZones ───────────────────────────────────────────────────────

class TestBuildFigmaZones:

    def _tokens(self) -> List[dict]:
        return [
            {"element_type": "zone", "name": "Sales by Sub-Category",
             "zone_view_id": "view_001", "x_px": 0, "y_px": 0, "w_px": 683, "h_px": 384},
            {"element_type": "zone", "name": "Monthly Sales Trend",
             "zone_view_id": "view_002", "x_px": 683, "y_px": 0, "w_px": 683, "h_px": 384},
        ]

    def test_matched_zones_are_floating(self):
        tokens = self._tokens()
        zones = build_figma_zones(["Sales by Sub-Category"], tokens)
        assert zones[0].is_fixed is True

    def test_matched_zone_pixel_coords(self):
        tokens = self._tokens()
        zones = build_figma_zones(["Sales by Sub-Category"], tokens)
        assert zones[0].x == 0
        assert zones[0].w == 683

    def test_unmatched_falls_back_to_grid(self):
        zones = build_figma_zones(["Unknown Sheet"], [])
        assert len(zones) == 1
        assert zones[0].is_fixed is False

    def test_mixed_matched_and_unmatched(self):
        tokens = self._tokens()
        names = ["Sales by Sub-Category", "Unmatched Sheet"]
        zones = build_figma_zones(names, tokens)
        assert zones[0].is_fixed is True
        assert zones[1].is_fixed is False

    def test_empty_tokens_all_fallback(self):
        zones = build_figma_zones(FOUR_SHEETS, [])
        assert all(not z.is_fixed for z in zones)


# ── TestDashboardDocument ─────────────────────────────────────────────────────

class TestDashboardDocument:

    def _doc(self) -> DashboardDocument:
        zones = build_grid_zones(FOUR_SHEETS, "grid-2x2")
        return DashboardDocument(name="Sales Overview Dashboard", zones=zones)

    def test_is_valid_with_name_and_zones(self):
        assert self._doc().is_valid is True

    def test_is_invalid_without_name(self):
        assert DashboardDocument(name="", zones=[ZoneSpec(1,"S","worksheet")]).is_valid is False

    def test_is_invalid_without_zones(self):
        assert DashboardDocument(name="Dash").is_valid is False

    def test_to_xml_parseable(self):
        root = parse_xml(self._doc().to_xml())
        assert root.tag == "dashboard"

    def test_name_attribute(self):
        root = parse_xml(self._doc().to_xml())
        assert root.attrib["name"] == "Sales Overview Dashboard"

    def test_size_element_present(self):
        root = parse_xml(self._doc().to_xml())
        size = root.find("size")
        assert size is not None
        assert size.attrib["maxwidth"] == "1366"
        assert size.attrib["maxheight"] == "768"

    def test_custom_canvas_size(self):
        zones = build_grid_zones(FOUR_SHEETS, "grid-2x2")
        doc = DashboardDocument("Dash", zones=zones, width_px=1920, height_px=1080)
        root = parse_xml(doc.to_xml())
        assert root.find("size").attrib["maxwidth"] == "1920"

    def test_zones_element_present(self):
        root = parse_xml(self._doc().to_xml())
        assert root.find("zones") is not None

    def test_outer_container_zone_exists(self):
        root = parse_xml(self._doc().to_xml())
        outer = root.find("zones/zone")
        assert outer is not None
        assert outer.attrib["type-v2"] == "layout-basic"

    def test_worksheet_zones_are_children_of_container(self):
        root = parse_xml(self._doc().to_xml())
        children = list(root.findall("zones/zone/zone"))
        assert len(children) == 4

    def test_worksheet_zone_names_match_sheets(self):
        root = parse_xml(self._doc().to_xml())
        names = {z.attrib.get("name") for z in root.findall("zones/zone/zone")}
        assert "Sales by Sub-Category" in names

    def test_devicelayouts_element_present(self):
        root = parse_xml(self._doc().to_xml())
        assert root.find("devicelayouts") is not None

    def test_to_dict_has_required_keys(self):
        d = self._doc().to_dict()
        for key in ("view_id", "name", "layout", "width_px", "height_px",
                    "zone_count", "is_valid", "xml"):
            assert key in d, f"Missing: {key}"

    def test_zone_count_in_dict(self):
        assert self._doc().to_dict()["zone_count"] == 4

    def test_repr_contains_name(self):
        assert "Sales Overview Dashboard" in repr(self._doc())

    def test_filter_fields_emit_filter_elements(self):
        zones = build_grid_zones(FOUR_SHEETS, "grid-2x2")
        doc = DashboardDocument("Dash", zones=zones,
                                filter_fields=["[federated.x].[none:REGION:nk]"])
        root = parse_xml(doc.to_xml())
        filters = root.findall("filter")
        assert len(filters) == 1


# ── TestDashboardAgentValidateInput ───────────────────────────────────────────

class TestDashboardAgentValidateInput:

    def test_missing_project_spec_fails(self):
        errors = _agent().validate_input({"worksheets_xml": []})
        assert any("project_spec" in e for e in errors)

    def test_missing_worksheets_xml_fails(self):
        errors = _agent().validate_input({"project_spec": {"project_id": "x"}})
        assert any("worksheets_xml" in e for e in errors)

    def test_empty_worksheets_xml_passes(self):
        # Empty list is valid — agent handles missing sheets gracefully
        errors = _agent().validate_input({
            "project_spec":  {"project_id": "x"},
            "worksheets_xml": [],
        })
        assert errors == []

    def test_valid_state_passes(self):
        errors = _agent().validate_input({
            "project_spec":   {"project_id": "x"},
            "worksheets_xml": SAMPLE_WS_XML,
        })
        assert errors == []


# ── TestDashboardAgentExecution ───────────────────────────────────────────────

class TestDashboardAgentExecution:

    @pytest.fixture(scope="class")
    def pipeline_state(self):
        return _pipeline_state()

    @pytest.fixture(scope="class")
    def agent_result(self, pipeline_state):
        return _agent().execute(pipeline_state)

    def test_execute_succeeds(self, agent_result):
        assert agent_result.status in (AgentStatus.SUCCESS, AgentStatus.WARNING)

    def test_output_has_dashboards_xml(self, agent_result):
        assert "dashboards_xml" in agent_result.output

    def test_dashboards_xml_is_list(self, agent_result):
        assert isinstance(agent_result.output["dashboards_xml"], list)

    def test_at_least_one_dashboard(self, agent_result):
        assert len(agent_result.output["dashboards_xml"]) >= 1

    def test_each_dashboard_has_xml(self, agent_result):
        for d in agent_result.output["dashboards_xml"]:
            assert "xml" in d and len(d["xml"]) > 50

    def test_each_xml_is_parseable(self, agent_result):
        for d in agent_result.output["dashboards_xml"]:
            root = parse_xml(d["xml"])
            assert root.tag == "dashboard"

    def test_is_valid_flag_true(self, agent_result):
        for d in agent_result.output["dashboards_xml"]:
            assert d["is_valid"] is True

    def test_metadata_summary_present(self, agent_result):
        s = agent_result.metadata.get("dashboards_summary", {})
        assert "dashboards_generated" in s
        assert s["dashboards_generated"] > 0

    def test_timing_recorded(self, agent_result):
        assert agent_result.duration_ms > 0


# ── TestDashboardXmlContent ───────────────────────────────────────────────────

class TestDashboardXmlContent:

    @pytest.fixture(scope="class")
    def dashboard_root(self):
        state = _pipeline_state()
        result = _agent().execute(state)
        xml_str = result.output["dashboards_xml"][0]["xml"]
        return parse_xml(xml_str)

    def test_dashboard_name(self, dashboard_root):
        assert dashboard_root.attrib["name"] == "Sales Overview Dashboard"

    def test_size_element(self, dashboard_root):
        size = dashboard_root.find("size")
        assert size is not None
        assert int(size.attrib["maxwidth"]) == 1366
        assert int(size.attrib["maxheight"]) == 768

    def test_four_worksheet_zones(self, dashboard_root):
        ws_zones = dashboard_root.findall("zones/zone/zone[@type-v2='worksheet']")
        assert len(ws_zones) == 4

    def test_zone_coordinates_non_negative(self, dashboard_root):
        ws_zones = dashboard_root.findall("zones/zone/zone[@type-v2='worksheet']")
        for z in ws_zones:
            assert int(z.attrib["x"]) >= 0
            assert int(z.attrib["y"]) >= 0

    def test_zone_dimensions_positive(self, dashboard_root):
        ws_zones = dashboard_root.findall("zones/zone/zone[@type-v2='worksheet']")
        for z in ws_zones:
            assert int(z.attrib["w"]) > 0
            assert int(z.attrib["h"]) > 0

    def test_2x2_grid_50000_dimensions(self, dashboard_root):
        ws_zones = dashboard_root.findall("zones/zone/zone[@type-v2='worksheet']")
        for z in ws_zones:
            assert int(z.attrib["w"]) == 50000
            assert int(z.attrib["h"]) == 50000

    def test_zones_cover_full_canvas(self, dashboard_root):
        ws_zones = dashboard_root.findall("zones/zone/zone[@type-v2='worksheet']")
        # Sum of zone areas should equal TILED_MAX²
        total_area = sum(int(z.attrib["w"]) * int(z.attrib["h"]) for z in ws_zones)
        assert total_area == TILED_MAX * TILED_MAX

    def test_expected_worksheet_names_in_zones(self, dashboard_root):
        ws_zones = dashboard_root.findall("zones/zone/zone[@type-v2='worksheet']")
        names = {z.attrib.get("name") for z in ws_zones}
        assert "Sales by Sub-Category" in names

    def test_devicelayouts_present(self, dashboard_root):
        assert dashboard_root.find("devicelayouts") is not None


# ── TestHelpers ────────────────────────────────────────────────────────────────

class TestHelpers:

    @pytest.fixture(scope="class")
    def spec_and_ws(self):
        from models.project_spec import ProjectSpec
        spec = ProjectSpec.from_csv_dir("csv_inputs")
        ws_by_name = {ws["name"]: ws for ws in SAMPLE_WS_XML}
        return spec, ws_by_name

    def test_find_ws_name_resolves_view_id(self, spec_and_ws):
        spec, ws_by_name = spec_and_ws
        # The worksheets in SAMPLE_WS_XML match the sheet names in csv
        result = _find_ws_name_for_view_id("view_001", spec, ws_by_name)
        assert result == "Sales by Sub-Category"

    def test_find_ws_name_returns_none_for_unknown(self, spec_and_ws):
        spec, ws_by_name = spec_and_ws
        assert _find_ws_name_for_view_id("nonexistent", spec, ws_by_name) is None

    def test_resolve_sheet_names_ordered(self, spec_and_ws):
        spec, ws_by_name = spec_and_ws
        view_ids = ["view_001", "view_002", "view_003", "view_004"]
        names = _resolve_sheet_names(view_ids, ws_by_name, spec)
        assert len(names) == 4
        assert names[0] == "Sales by Sub-Category"


# ── TestPhase01To09Pipeline ───────────────────────────────────────────────────

class TestPhase01To09Pipeline:

    @pytest.fixture(scope="class")
    def full_state(self):
        return _pipeline_state()

    def test_full_pipeline_dry_run(self, full_state):
        result = _agent().execute(full_state)
        assert result.status in (AgentStatus.SUCCESS, AgentStatus.WARNING)

    def test_dashboards_feed_into_workbook_state(self, full_state):
        result = _agent().execute(full_state)
        state = {**full_state, **result.output}
        assert "dashboards_xml" in state
        assert len(state["dashboards_xml"]) > 0

    def test_phase_label(self, full_state):
        result = _agent().execute(full_state)
        assert result.phase == "GENERATING"
        assert result.agent_id == "dashboard_agent"

    def test_dashboard_count_matches_spec(self, full_state):
        from models.project_spec import ProjectSpec
        spec = ProjectSpec.model_validate(full_state["project_spec"])
        result = _agent().execute(full_state)
        assert len(result.output["dashboards_xml"]) == len(spec.get_dashboards())

    def test_all_dashboards_have_valid_xml(self, full_state):
        result = _agent().execute(full_state)
        for d in result.output["dashboards_xml"]:
            root = parse_xml(d["xml"])
            assert root.tag == "dashboard"
