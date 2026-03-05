# tests/unit/test_tableau_model_agent.py
"""
Unit tests for TableauModelAgent (Phase 08) and WorksheetSpec models.

Test counts target: ~75 tests
Coverage: ColumnRegistry, resolve_field_ref, WorksheetDocument,
          TableauModelAgent.validate_input, run(), pipeline integration.
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any, Dict
import pytest

from agents.base_agent import AgentStatus
from agents.tableau_model_agent import TableauModelAgent, _build_registries, _find_tds
from models.worksheet_spec import (
    ColumnRegistry,
    FilterSpec,
    SortSpec,
    WorksheetDocument,
    chart_type_to_mark,
    resolve_field_ref,
)


# ── Fixtures ───────────────────────────────────────────────────────────────────

SAMPLE_TDS_XML = """
<datasource caption="Superstore Orders" inline="true"
            name="federated.abc123" version="18.1"
            xmlns:user="http://www.tableausoftware.com/xml/user">
  <connection class="federated" />
  <aliases enabled="yes" />
  <column datatype="string"   name="[SUB_CATEGORY]" role="dimension" type="nominal"  caption="Sub-Category" />
  <column datatype="string"   name="[CATEGORY]"     role="dimension" type="nominal"  caption="Category" />
  <column datatype="string"   name="[REGION]"       role="dimension" type="nominal"  caption="Region" />
  <column datatype="string"   name="[SEGMENT]"      role="dimension" type="nominal"  caption="Segment" />
  <column datatype="real"     name="[SALES]"        role="measure"   type="quantitative" caption="Sales" />
  <column datatype="real"     name="[PROFIT]"       role="measure"   type="quantitative" caption="Profit" />
  <column datatype="integer"  name="[QUANTITY]"     role="measure"   type="quantitative" caption="Quantity" />
  <column datatype="datetime" name="[ORDER_DATE]"   role="dimension" type="ordinal"  caption="Order Date" />
  <column datatype="real"     name="[Calculation_1234567890123456789]" role="measure"
          type="quantitative" caption="Profit Ratio">
    <calculation class="tableau" formula="SUM([PROFIT]) / SUM([SALES])" />
  </column>
  <column datatype="integer"  name="[Calculation_9876543210987654321]" role="measure"
          type="quantitative" caption="Orders Count">
    <calculation class="tableau" formula="COUNTD([ORDER_ID])" />
  </column>
</datasource>
""".strip()

SAMPLE_TDS_DOC = {
    "datasource_id": "ds_001",
    "ds_name": "federated.abc123",
    "caption": "Superstore Orders",
    "xml": SAMPLE_TDS_XML,
    "is_valid": True,
    "connections": 1,
    "raw_columns": 8,
    "calculated_cols": 2,
    "total_columns": 10,
    "col_maps": 0,
    "relations": 0,
    "timestamp": "2026-01-01T00:00:00+00:00",
}


def _registry() -> ColumnRegistry:
    return ColumnRegistry.from_tds_xml("federated.abc123", SAMPLE_TDS_XML)


def _agent(cfg: dict = None) -> TableauModelAgent:
    return TableauModelAgent(config=cfg or {})


def _pipeline_state(extra: dict = None) -> Dict[str, Any]:
    """Build a minimal state that passes validation."""
    from agents.base_agent import PhaseContext
    dry_ctx = PhaseContext(project_id="proj_001", run_id="test", dry_run=True)

    from agents.intake_agent import IntakeAgent
    intake = IntakeAgent(config={"csv_dir": "csv_inputs"})
    intake_result = intake.execute({})
    state: dict = {**intake_result.output}

    from agents.validation_agent import ValidationAgent
    val_result = ValidationAgent().execute(state)
    state.update(val_result.output)

    from agents.connectivity_agent import ConnectivityAgent
    conn_result = ConnectivityAgent(context=dry_ctx).execute(state)
    state.update(conn_result.output)

    from agents.profiler_agent import ProfilerAgent
    prof_result = ProfilerAgent(context=dry_ctx).execute(state)
    state.update(prof_result.output)

    from agents.conversion_agent import DataConversionAgent
    conv_result = DataConversionAgent().execute(state)
    state.update(conv_result.output)

    from agents.semantic_agent import SemanticModelAgent
    sem_result = SemanticModelAgent().execute(state)
    state.update(sem_result.output)

    if extra:
        state.update(extra)
    return state


def parse_xml(xml_str: str) -> ET.Element:
    return ET.fromstring(xml_str)


# ── TestColumnRegistry ─────────────────────────────────────────────────────────

class TestColumnRegistry:

    def test_register_and_lookup_by_name(self):
        reg = ColumnRegistry("federated.x")
        reg.register("[SALES]", "Sales", "measure", "quantitative")
        entry = reg.lookup("SALES")
        assert entry is not None
        assert entry["role"] == "measure"

    def test_lookup_brackets_stripped(self):
        reg = ColumnRegistry("federated.x")
        reg.register("[SALES]", "Sales", "measure", "quantitative")
        assert reg.lookup("[SALES]") is not None

    def test_lookup_by_caption(self):
        reg = ColumnRegistry("federated.x")
        reg.register("[Calculation_123]", "Profit Ratio", "measure", "quantitative", True)
        entry = reg.lookup("Profit Ratio")
        assert entry is not None
        assert entry["is_calculated"] is True

    def test_lookup_case_insensitive(self):
        reg = ColumnRegistry("federated.x")
        reg.register("[REGION]", "Region", "dimension", "nominal")
        assert reg.lookup("region") is not None
        assert reg.lookup("REGION") is not None

    def test_lookup_missing_returns_none(self):
        reg = ColumnRegistry("federated.x")
        assert reg.lookup("NONEXISTENT") is None

    def test_from_tds_xml_parses_columns(self):
        reg = _registry()
        assert reg.lookup("SALES") is not None
        assert reg.lookup("SUB_CATEGORY") is not None

    def test_from_tds_xml_parses_calculated_field(self):
        reg = _registry()
        entry = reg.lookup("Profit Ratio")
        assert entry is not None
        assert entry["is_calculated"] is True

    def test_from_tds_xml_role_correct(self):
        reg = _registry()
        assert reg.lookup("SALES")["role"] == "measure"
        assert reg.lookup("REGION")["role"] == "dimension"

    def test_from_tds_xml_type_correct(self):
        reg = _registry()
        assert reg.lookup("ORDER_DATE")["type_"] == "ordinal"
        assert reg.lookup("SALES")["type_"] == "quantitative"

    def test_from_invalid_xml_returns_empty_registry(self):
        reg = ColumnRegistry.from_tds_xml("federated.x", "NOT VALID XML <<<")
        assert reg.lookup("ANYTHING") is None


# ── TestResolveFieldRef ────────────────────────────────────────────────────────

class TestResolveFieldRef:

    def setup_method(self):
        self.reg = _registry()
        self.ds  = "federated.abc123"

    def test_bare_measure_gets_sum(self):
        ref = resolve_field_ref("SALES", self.reg, self.ds)
        assert ref == "[federated.abc123].[sum:SALES:qk]"

    def test_sum_wrapper_measure(self):
        ref = resolve_field_ref("SUM(SALES)", self.reg, self.ds)
        assert ref == "[federated.abc123].[sum:SALES:qk]"

    def test_bare_dimension_gets_none(self):
        ref = resolve_field_ref("SUB_CATEGORY", self.reg, self.ds)
        assert ref == "[federated.abc123].[none:SUB_CATEGORY:nk]"

    def test_date_month_part(self):
        ref = resolve_field_ref("MONTH(ORDER_DATE)", self.reg, self.ds)
        assert ref == "[federated.abc123].[mnth:ORDER_DATE:ok]"

    def test_date_year_part(self):
        ref = resolve_field_ref("YEAR(ORDER_DATE)", self.reg, self.ds)
        assert ref == "[federated.abc123].[yr:ORDER_DATE:ok]"

    def test_calculated_field_by_caption(self):
        ref = resolve_field_ref("Profit Ratio", self.reg, self.ds)
        # Caption lookup → Calculation_NNN → measure → sum:...:qk
        assert "sum" in ref
        assert "qk" in ref
        assert "Calculation_1234567890123456789" in ref

    def test_avg_aggregation(self):
        ref = resolve_field_ref("AVG(PROFIT)", self.reg, self.ds)
        assert ref == "[federated.abc123].[avg:PROFIT:qk]"

    def test_count_aggregation(self):
        ref = resolve_field_ref("COUNT(SALES)", self.reg, self.ds)
        assert ref == "[federated.abc123].[cnt:SALES:qk]"

    def test_ordinal_dimension_qualifier(self):
        # ORDER_DATE is datetime → ok qualifier
        ref = resolve_field_ref("ORDER_DATE", self.reg, self.ds)
        assert ":ok]" in ref

    def test_nominal_dimension_qualifier(self):
        ref = resolve_field_ref("REGION", self.reg, self.ds)
        assert ":nk]" in ref

    def test_unknown_field_treated_as_dimension(self):
        ref = resolve_field_ref("UNKNOWN_FIELD", self.reg, self.ds)
        assert "UNKNOWN_FIELD" in ref
        assert ":nk]" in ref

    def test_empty_string_returns_empty(self):
        ref = resolve_field_ref("", self.reg, self.ds)
        assert ref == ""

    def test_force_agg_overrides(self):
        # Even for a dimension, force_agg applies
        ref = resolve_field_ref("REGION", self.reg, self.ds, force_agg="sum")
        assert ref.startswith("[federated.abc123].[sum:REGION:")

    def test_ds_name_embedded_in_ref(self):
        ref = resolve_field_ref("SALES", self.reg, "federated.xyz999")
        assert ref.startswith("[federated.xyz999].")

    def test_sum_measure_quantitative_qualifier(self):
        ref = resolve_field_ref("SUM(PROFIT)", self.reg, self.ds)
        assert ref.endswith(":qk]")


# ── TestChartTypeToMark ────────────────────────────────────────────────────────

class TestChartTypeToMark:

    def test_bar(self):
        assert chart_type_to_mark("Bar") == "Bar"

    def test_bar_lowercase(self):
        assert chart_type_to_mark("bar") == "Bar"

    def test_line(self):
        assert chart_type_to_mark("Line") == "Line"

    def test_pie(self):
        assert chart_type_to_mark("Pie") == "Pie"

    def test_text(self):
        assert chart_type_to_mark("Text") == "Text"

    def test_scatter_maps_to_circle(self):
        assert chart_type_to_mark("Scatter") == "Circle"

    def test_unknown_maps_to_automatic(self):
        assert chart_type_to_mark("Unknown123") == "Automatic"


# ── TestWorksheetDocument ──────────────────────────────────────────────────────

class TestWorksheetDocument:

    def _basic_doc(self) -> WorksheetDocument:
        return WorksheetDocument(
            name="Sales by Region",
            ds_name="federated.abc123",
            mark_class="Bar",
            row_refs=["[federated.abc123].[none:REGION:nk]"],
            col_refs=["[federated.abc123].[sum:SALES:qk]"],
        )

    def test_is_valid_with_name_and_ds_name(self):
        doc = self._basic_doc()
        assert doc.is_valid is True

    def test_is_invalid_without_name(self):
        doc = WorksheetDocument(name="", ds_name="federated.x")
        assert doc.is_valid is False

    def test_is_invalid_without_ds_name(self):
        doc = WorksheetDocument(name="Sheet", ds_name="")
        assert doc.is_valid is False

    def test_to_xml_parseable(self):
        root = parse_xml(self._basic_doc().to_xml())
        assert root.tag == "worksheet"

    def test_worksheet_name_attribute(self):
        root = parse_xml(self._basic_doc().to_xml())
        assert root.attrib["name"] == "Sales by Region"

    def test_table_element_present(self):
        root = parse_xml(self._basic_doc().to_xml())
        assert root.find("table") is not None

    def test_view_element_present(self):
        root = parse_xml(self._basic_doc().to_xml())
        assert root.find("table/view") is not None

    def test_datasource_element_in_view(self):
        root = parse_xml(self._basic_doc().to_xml())
        ds_el = root.find("table/view/datasources/datasource")
        assert ds_el is not None
        assert ds_el.attrib["name"] == "federated.abc123"

    def test_rows_shelf_content(self):
        root = parse_xml(self._basic_doc().to_xml())
        rows = root.find("table/view/rows")
        assert rows is not None
        assert "REGION" in rows.text

    def test_cols_shelf_content(self):
        root = parse_xml(self._basic_doc().to_xml())
        cols = root.find("table/view/cols")
        assert cols is not None
        assert "SALES" in cols.text

    def test_panes_element_present(self):
        root = parse_xml(self._basic_doc().to_xml())
        assert root.find("table/panes") is not None

    def test_mark_class_set(self):
        root = parse_xml(self._basic_doc().to_xml())
        mark = root.find("table/panes/pane/mark")
        assert mark is not None
        assert mark.attrib["class"] == "Bar"

    def test_color_encoding_set(self):
        doc = self._basic_doc()
        doc.color_ref = "[federated.abc123].[none:CATEGORY:nk]"
        root = parse_xml(doc.to_xml())
        color = root.find("table/panes/pane/encodings/color")
        assert color is not None
        assert "CATEGORY" in color.attrib["column"]

    def test_size_encoding_set(self):
        doc = self._basic_doc()
        doc.size_ref = "[federated.abc123].[sum:SALES:qk]"
        root = parse_xml(doc.to_xml())
        size = root.find("table/panes/pane/encodings/size")
        assert size is not None

    def test_sort_element_emitted(self):
        doc = self._basic_doc()
        doc.sorts.append(SortSpec(
            field_ref="[federated.abc123].[sum:SALES:qk]",
            direction="descending"
        ))
        root = parse_xml(doc.to_xml())
        sort = root.find("table/sorts/sort")
        assert sort is not None
        assert sort.attrib["direction"] == "descending"

    def test_filter_element_emitted(self):
        doc = self._basic_doc()
        doc.filters.append(FilterSpec(
            field_ref="[federated.abc123].[none:REGION:nk]",
            field_role="dimension"
        ))
        root = parse_xml(doc.to_xml())
        f = root.find("table/view/filter")
        assert f is not None
        assert f.attrib["class"] == "categorical"

    def test_measure_filter_is_quantitative(self):
        doc = self._basic_doc()
        doc.filters.append(FilterSpec(
            field_ref="[federated.abc123].[sum:SALES:qk]",
            field_role="measure"
        ))
        root = parse_xml(doc.to_xml())
        f = root.find("table/view/filter")
        assert f.attrib["class"] == "quantitative"

    def test_text_mark_label_is_text_encoding(self):
        doc = WorksheetDocument(
            name="KPI", ds_name="federated.abc",
            mark_class="Text",
            col_refs=["[federated.abc].[sum:SALES:qk]"],
        )
        doc.text_ref = "[federated.abc].[sum:SALES:qk]"
        root = parse_xml(doc.to_xml())
        text_enc = root.find("table/panes/pane/encodings/text")
        assert text_enc is not None

    def test_to_dict_has_required_keys(self):
        d = self._basic_doc().to_dict()
        for key in ("view_id", "name", "ds_name", "mark_class", "is_valid", "xml"):
            assert key in d, f"Missing key: {key}"

    def test_to_dict_xml_is_string(self):
        assert isinstance(self._basic_doc().to_dict()["xml"], str)

    def test_repr_contains_name(self):
        assert "Sales by Region" in repr(self._basic_doc())

    def test_multiple_row_refs_joined(self):
        doc = WorksheetDocument(
            name="Sheet", ds_name="federated.x",
            row_refs=[
                "[federated.x].[sum:SALES:qk]",
                "[federated.x].[sum:PROFIT:qk]",
            ],
        )
        xml_str = doc.to_xml()
        root = parse_xml(xml_str)
        rows = root.find("table/view/rows")
        assert " + " in rows.text


# ── TestTableauModelAgentValidateInput ────────────────────────────────────────

class TestTableauModelAgentValidateInput:

    def test_missing_project_spec_fails(self):
        agent = _agent()
        errors = agent.validate_input({"tds_documents": [SAMPLE_TDS_DOC]})
        assert any("project_spec" in e for e in errors)

    def test_missing_tds_documents_fails(self):
        agent = _agent()
        errors = agent.validate_input({"project_spec": {"project_id": "x"}})
        assert any("tds_documents" in e for e in errors)

    def test_valid_state_passes(self):
        agent = _agent()
        errors = agent.validate_input({
            "project_spec":  {"project_id": "x"},
            "tds_documents": [SAMPLE_TDS_DOC],
        })
        assert errors == []


# ── TestTableauModelAgentExecution ────────────────────────────────────────────

class TestTableauModelAgentExecution:

    @pytest.fixture(scope="class")
    def pipeline_state(self):
        return _pipeline_state()

    @pytest.fixture(scope="class")
    def agent_result(self, pipeline_state):
        return _agent().execute(pipeline_state)

    def test_execute_succeeds(self, agent_result):
        assert agent_result.status in (AgentStatus.SUCCESS, AgentStatus.WARNING)

    def test_output_has_worksheets_xml(self, agent_result):
        assert "worksheets_xml" in agent_result.output

    def test_worksheets_xml_is_list(self, agent_result):
        assert isinstance(agent_result.output["worksheets_xml"], list)

    def test_at_least_one_worksheet(self, agent_result):
        assert len(agent_result.output["worksheets_xml"]) >= 1

    def test_each_worksheet_has_xml(self, agent_result):
        for ws in agent_result.output["worksheets_xml"]:
            assert "xml" in ws
            assert len(ws["xml"]) > 50

    def test_each_xml_is_parseable(self, agent_result):
        for ws in agent_result.output["worksheets_xml"]:
            root = parse_xml(ws["xml"])
            assert root.tag == "worksheet"

    def test_is_valid_flag_true(self, agent_result):
        for ws in agent_result.output["worksheets_xml"]:
            assert ws["is_valid"] is True

    def test_metadata_summary_present(self, agent_result):
        assert "worksheets_summary" in agent_result.metadata
        s = agent_result.metadata["worksheets_summary"]
        assert "worksheets_generated" in s
        assert s["worksheets_generated"] > 0

    def test_timing_recorded(self, agent_result):
        assert agent_result.duration_ms > 0

    def test_no_dashboards_in_output(self, agent_result):
        """Dashboards are handled by Phase 09, not here."""
        for ws in agent_result.output["worksheets_xml"]:
            assert ws["name"] != "Sales Overview Dashboard"


# ── TestWorksheetXmlContent ───────────────────────────────────────────────────

class TestWorksheetXmlContent:
    """Verify specific Tableau XML structure of generated worksheets."""

    @pytest.fixture(scope="class")
    def worksheets(self):
        state = _pipeline_state()
        result = _agent().execute(state)
        return {ws["name"]: ws for ws in result.output["worksheets_xml"]}

    def test_bar_chart_has_bar_mark(self, worksheets):
        ws = worksheets.get("Sales by Sub-Category")
        assert ws is not None
        root = parse_xml(ws["xml"])
        mark = root.find("table/panes/pane/mark")
        assert mark.attrib["class"] == "Bar"

    def test_bar_chart_rows_has_dimension(self, worksheets):
        ws = worksheets.get("Sales by Sub-Category")
        root = parse_xml(ws["xml"])
        rows = root.find("table/view/rows").text
        assert "SUB_CATEGORY" in rows

    def test_bar_chart_cols_has_measure(self, worksheets):
        ws = worksheets.get("Sales by Sub-Category")
        root = parse_xml(ws["xml"])
        cols = root.find("table/view/cols").text
        assert "sum" in cols.lower()

    def test_line_chart_mark(self, worksheets):
        ws = worksheets.get("Monthly Sales Trend")
        assert ws is not None
        root = parse_xml(ws["xml"])
        mark = root.find("table/panes/pane/mark")
        assert mark.attrib["class"] == "Line"

    def test_pie_chart_mark(self, worksheets):
        ws = worksheets.get("Sales by Segment")
        assert ws is not None
        root = parse_xml(ws["xml"])
        mark = root.find("table/panes/pane/mark")
        assert mark.attrib["class"] == "Pie"

    def test_pie_chart_has_color_encoding(self, worksheets):
        ws = worksheets.get("Sales by Segment")
        root = parse_xml(ws["xml"])
        color = root.find("table/panes/pane/encodings/color")
        assert color is not None

    def test_text_mark_for_kpi(self, worksheets):
        ws = worksheets.get("KPI Sales Total")
        assert ws is not None
        root = parse_xml(ws["xml"])
        mark = root.find("table/panes/pane/mark")
        assert mark.attrib["class"] == "Text"

    def test_sort_present_on_sorted_worksheet(self, worksheets):
        ws = worksheets.get("Sales by Sub-Category")
        root = parse_xml(ws["xml"])
        sort = root.find("table/sorts/sort")
        assert sort is not None
        assert sort.attrib["direction"] == "descending"

    def test_filter_present_when_filter_fields_set(self, worksheets):
        ws = worksheets.get("Sales by Sub-Category")
        root = parse_xml(ws["xml"])
        filters = root.findall("table/view/filter")
        assert len(filters) >= 1

    def test_datasource_reference_in_view(self, worksheets):
        for ws in worksheets.values():
            root = parse_xml(ws["xml"])
            ds = root.find("table/view/datasources/datasource")
            assert ds is not None
            assert ds.attrib["name"].startswith("federated.")


# ── TestBuildRegistriesHelper ─────────────────────────────────────────────────

class TestBuildRegistriesHelper:

    def test_builds_one_registry_per_tds(self):
        regs = _build_registries([SAMPLE_TDS_DOC])
        assert "ds_001" in regs

    def test_empty_tds_list_returns_empty_dict(self):
        regs = _build_registries([])
        assert regs == {}

    def test_registry_has_correct_ds_name(self):
        regs = _build_registries([SAMPLE_TDS_DOC])
        assert regs["ds_001"].ds_name == "federated.abc123"

    def test_registry_can_resolve_columns(self):
        regs = _build_registries([SAMPLE_TDS_DOC])
        reg = regs["ds_001"]
        assert reg.lookup("SALES") is not None


# ── TestFindTdsHelper ─────────────────────────────────────────────────────────

class TestFindTdsHelper:

    def test_finds_matching_doc(self):
        result = _find_tds([SAMPLE_TDS_DOC], "ds_001")
        assert result is not None
        assert result["ds_name"] == "federated.abc123"

    def test_returns_none_for_missing(self):
        result = _find_tds([SAMPLE_TDS_DOC], "ds_NONEXISTENT")
        assert result is None

    def test_empty_list_returns_none(self):
        assert _find_tds([], "ds_001") is None


# ── TestPhase01To08Pipeline ───────────────────────────────────────────────────

class TestPhase01To08Pipeline:

    @pytest.fixture(scope="class")
    def full_state(self):
        return _pipeline_state()

    def test_full_pipeline_dry_run(self, full_state):
        result = _agent().execute(full_state)
        assert result.status in (AgentStatus.SUCCESS, AgentStatus.WARNING)

    def test_worksheets_feed_into_workbook_state(self, full_state):
        result = _agent().execute(full_state)
        state = {**full_state, **result.output}
        assert "worksheets_xml" in state
        assert len(state["worksheets_xml"]) > 0

    def test_phase_labels_in_pipeline(self, full_state):
        result = _agent().execute(full_state)
        assert result.phase == "GENERATING"
        assert result.agent_id == "tableau_model_agent"

    def test_all_worksheets_have_valid_xml(self, full_state):
        result = _agent().execute(full_state)
        for ws in result.output["worksheets_xml"]:
            root = parse_xml(ws["xml"])
            assert root.tag == "worksheet"
            assert root.attrib.get("name")

    def test_worksheet_count_matches_spec(self, full_state):
        """Number of generated worksheets equals worksheet rows in dashboard_requirements.csv."""
        from models.project_spec import ProjectSpec
        spec = ProjectSpec.model_validate(full_state["project_spec"])
        result = _agent().execute(full_state)
        assert len(result.output["worksheets_xml"]) == len(spec.get_worksheets())
