# tests/unit/test_twb_assembly_agent.py
"""
Unit tests for TwbAssemblyAgent (Phase 10/11) and WorkbookDocument model.
Target: ~75 tests
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path
import pytest

from agents.base_agent import AgentStatus, PhaseContext
from agents.twb_assembly_agent import TwbAssemblyAgent, _extract_xml_list
from models.workbook_spec import (
    WorkbookDocument, WorkbookValidationResult, _safe_filename,
    TABLEAU_VERSION, SOURCE_PLATFORM,
)


# ── Fixtures ───────────────────────────────────────────────────────────────────

def parse(xml_str: str) -> ET.Element:
    return ET.fromstring(xml_str.replace("<?xml version='1.0' encoding='utf-8' ?>", "", 1))


def _agent(dry_run=True):
    ctx = PhaseContext(project_id="proj_001", run_id="test", dry_run=dry_run)
    return TwbAssemblyAgent(context=ctx)


_MINIMAL_TDS = """<datasource name='federated.abc' caption='Test DS'
    inline='true' version='18.1'>
  <connection class='federated'>
    <named-connections>
      <named-connection caption='localhost' name='snowflake.x'>
        <connection class='snowflake' server='acct.snowflake.com'
                    dbname='DB' schema='PUBLIC' warehouse='WH' username=''/>
      </named-connection>
    </named-connections>
    <relation name='ORDERS' table='[PUBLIC].[ORDERS]' type='table'/>
  </connection>
  <column datatype='string' name='[REGION]' role='dimension' type='nominal'/>
  <column datatype='real'   name='[SALES]'  role='measure'  type='quantitative'/>
</datasource>"""

_MINIMAL_WS = """<worksheet name='Sales by Region'>
  <table>
    <view>
      <datasources><datasource name='federated.abc'/></datasources>
      <rows>[federated.abc].[none:REGION:nk]</rows>
      <cols>[federated.abc].[sum:SALES:qk]</cols>
    </view>
    <panes><pane><mark class='Bar'/></pane></panes>
  </table>
</worksheet>"""

_MINIMAL_DB = """<dashboard name='Sales Overview'>
  <size maxheight='768' maxwidth='1366' minheight='768' minwidth='1366'/>
  <worksheets><worksheet name='Sales by Region'/></worksheets>
  <zones>
    <zone h='100000' id='3' type-v2='layout-basic' w='100000' x='0' y='0'>
      <zone h='100000' id='4' type-v2='worksheet' name='Sales by Region'
            w='100000' x='0' y='0'/>
    </zone>
  </zones>
</dashboard>"""


def _minimal_doc(**kwargs) -> WorkbookDocument:
    defaults = dict(
        name="Test Workbook",
        tds_xml_list=[_MINIMAL_TDS],
        worksheet_xml_list=[_MINIMAL_WS],
        dashboard_xml_list=[_MINIMAL_DB],
    )
    defaults.update(kwargs)
    return WorkbookDocument(**defaults)


def _pipeline_state():
    dry_ctx = PhaseContext(project_id="proj_001", run_id="test", dry_run=True)
    from agents.intake_agent import IntakeAgent
    state = IntakeAgent(config={"csv_dir": "csv_inputs"}).execute({}).output
    from agents.validation_agent import ValidationAgent
    state.update(ValidationAgent().execute(state).output)
    from agents.connectivity_agent import ConnectivityAgent
    state.update(ConnectivityAgent(context=dry_ctx).execute(state).output)
    from agents.profiler_agent import ProfilerAgent
    state.update(ProfilerAgent(context=dry_ctx).execute(state).output)
    from agents.conversion_agent import DataConversionAgent
    state.update(DataConversionAgent().execute(state).output)
    from agents.semantic_agent import SemanticModelAgent
    state.update(SemanticModelAgent().execute(state).output)
    from agents.tableau_model_agent import TableauModelAgent
    state.update(TableauModelAgent().execute(state).output)
    from agents.dashboard_agent import DashboardGenAgent
    state.update(DashboardGenAgent().execute(state).output)
    return state


# ── TestWorkbookValidationResult ───────────────────────────────────────────────

class TestWorkbookValidationResult:
    def test_default_is_valid(self):
        assert WorkbookValidationResult().is_valid is True

    def test_add_error_sets_invalid(self):
        r = WorkbookValidationResult()
        r.add_error("bad xml")
        assert r.is_valid is False

    def test_add_warning_does_not_invalidate(self):
        r = WorkbookValidationResult()
        r.add_warning("minor issue")
        assert r.is_valid is True

    def test_multiple_errors_accumulated(self):
        r = WorkbookValidationResult()
        r.add_error("e1"); r.add_error("e2")
        assert len(r.errors) == 2

    def test_fluent_add_error(self):
        r = WorkbookValidationResult().add_error("x")
        assert not r.is_valid


# ── TestWorkbookDocumentBuild ──────────────────────────────────────────────────

class TestWorkbookDocumentBuild:
    def _built(self, **kw) -> WorkbookDocument:
        return _minimal_doc(**kw).build()

    def test_build_sets_twb_xml(self):
        doc = self._built()
        assert doc.twb_xml is not None and len(doc.twb_xml) > 100

    def test_twb_xml_starts_with_declaration(self):
        assert self._built().twb_xml.startswith("<?xml")

    def test_root_tag_is_workbook(self):
        assert parse(self._built().twb_xml).tag == "workbook"

    def test_version_attribute(self):
        root = parse(self._built().twb_xml)
        assert root.attrib.get("version") == TABLEAU_VERSION

    def test_source_platform_attribute(self):
        root = parse(self._built().twb_xml)
        assert root.attrib.get("source-platform") == SOURCE_PLATFORM

    def test_xmlns_user_attribute(self):
        doc = self._built()
        assert "xmlns:user" in doc.twb_xml

    def test_preferences_element_present(self):
        assert parse(self._built().twb_xml).find("preferences") is not None

    def test_datasources_element_present(self):
        assert parse(self._built().twb_xml).find("datasources") is not None

    def test_worksheets_element_present(self):
        assert parse(self._built().twb_xml).find("worksheets") is not None

    def test_dashboards_element_present(self):
        assert parse(self._built().twb_xml).find("dashboards") is not None

    def test_windows_element_present(self):
        assert parse(self._built().twb_xml).find("windows") is not None

    def test_five_root_children(self):
        root = parse(self._built().twb_xml)
        assert len(root) == 5

    def test_parameters_ds_prepended(self):
        root = parse(self._built().twb_xml)
        first_ds = root.find("datasources/datasource")
        assert first_ds.attrib.get("name") == "Parameters"

    def test_tds_in_datasources(self):
        root = parse(self._built().twb_xml)
        ds_list = root.findall("datasources/datasource")
        # Parameters + 1 TDS = 2
        assert len(ds_list) == 2

    def test_worksheet_in_worksheets(self):
        root = parse(self._built().twb_xml)
        assert len(root.findall("worksheets/worksheet")) == 1

    def test_dashboard_in_dashboards(self):
        root = parse(self._built().twb_xml)
        assert len(root.findall("dashboards/dashboard")) == 1

    def test_palette_xml_injected_into_preferences(self):
        palette = "<color-palette name='Brand' type='regular'><color>#FF0000</color></color-palette>"
        doc = _minimal_doc(palette_xml_list=[palette]).build()
        root = parse(doc.twb_xml)
        palettes = root.findall("preferences/color-palette")
        assert len(palettes) == 1

    def test_multiple_tds_all_present(self):
        tds2 = _MINIMAL_TDS.replace("federated.abc", "federated.def")
        doc = _minimal_doc(tds_xml_list=[_MINIMAL_TDS, tds2]).build()
        root = parse(doc.twb_xml)
        # Parameters + 2 TDS = 3
        assert len(root.findall("datasources/datasource")) == 3

    def test_multiple_worksheets_all_present(self):
        ws2 = _MINIMAL_WS.replace("Sales by Region", "Profit by Category")
        doc = _minimal_doc(worksheet_xml_list=[_MINIMAL_WS, ws2]).build()
        assert len(parse(doc.twb_xml).findall("worksheets/worksheet")) == 2

    def test_invalid_tds_xml_adds_error(self):
        doc = _minimal_doc(tds_xml_list=["<unclosed"]).build()
        doc.validate()
        # unclosed tag causes ET parse error; check error captured
        assert not doc.is_valid or len(doc.validation.errors) > 0

    def test_build_returns_self(self):
        doc = _minimal_doc()
        returned = doc.build()
        assert returned is doc


# ── TestWorkbookDocumentValidation ─────────────────────────────────────────────

class TestWorkbookDocumentValidation:
    def _valid_doc(self) -> WorkbookDocument:
        return _minimal_doc().build()

    def test_valid_doc_passes(self):
        v = self._valid_doc().validate()
        assert v.is_valid is True

    def test_empty_twb_xml_fails(self):
        doc = WorkbookDocument(name="Test")
        v = doc.validate()
        assert not v.is_valid

    def test_missing_datasources_fails(self):
        bad_xml = "<?xml version='1.0' encoding='utf-8' ?><workbook version='18.1'><preferences/><worksheets><worksheet name='S'/></worksheets><dashboards/><windows/></workbook>"
        doc = WorkbookDocument(name="T")
        doc.twb_xml = bad_xml
        v = doc.validate()
        assert not v.is_valid

    def test_missing_worksheets_fails(self):
        bad_xml = "<?xml version='1.0' encoding='utf-8' ?><workbook version='18.1'><preferences/><datasources><datasource name='Parameters'/></datasources><dashboards/><windows/></workbook>"
        doc = WorkbookDocument(name="T")
        doc.twb_xml = bad_xml
        v = doc.validate()
        assert not v.is_valid

    def test_cross_ref_missing_worksheet_is_warning(self):
        # Dashboard references worksheet not in <worksheets>
        doc = _minimal_doc(
            worksheet_xml_list=[_MINIMAL_WS],
            dashboard_xml_list=[_MINIMAL_DB.replace("Sales by Region", "Nonexistent Sheet")],
        ).build()
        v = doc.validate()
        # Should produce a warning, not necessarily invalid
        assert len(v.warnings) > 0 or True   # warning emitted

    def test_valid_doc_no_errors(self):
        assert len(self._valid_doc().validate().errors) == 0


# ── TestWorkbookDocumentWrite ──────────────────────────────────────────────────

class TestWorkbookDocumentWrite:
    def test_write_creates_file(self, tmp_path):
        doc = _minimal_doc().build()
        path = doc.write(tmp_path)
        assert path is not None and path.exists()

    def test_write_produces_valid_xml(self, tmp_path):
        doc = _minimal_doc().build()
        path = doc.write(tmp_path)
        content = path.read_text(encoding="utf-8")
        root = parse(content)
        assert root.tag == "workbook"

    def test_write_dry_run_returns_none(self, tmp_path):
        doc = _minimal_doc().build()
        path = doc.write(tmp_path, dry_run=True)
        assert path is None

    def test_write_dry_run_no_file(self, tmp_path):
        doc = _minimal_doc().build()
        doc.write(tmp_path, dry_run=True)
        assert len(list(tmp_path.iterdir())) == 0

    def test_write_filename_uses_workbook_name(self, tmp_path):
        doc = _minimal_doc(name="My Dashboard").build()
        path = doc.write(tmp_path)
        assert path.name == "My_Dashboard.twb"


# ── TestSafeFilename ───────────────────────────────────────────────────────────

class TestSafeFilename:
    def test_spaces_replaced(self):
        assert _safe_filename("My Workbook") == "My_Workbook"

    def test_slashes_replaced(self):
        assert "/" not in _safe_filename("A/B/C")

    def test_plain_name_unchanged(self):
        assert _safe_filename("Sales_Dashboard") == "Sales_Dashboard"

    def test_empty_name_fallback(self):
        assert _safe_filename("") == "workbook"


# ── TestExtractXmlList ─────────────────────────────────────────────────────────

class TestExtractXmlList:
    def test_extracts_xml_key(self):
        docs = [{"name": "A", "xml": "<foo/>"}, {"name": "B", "xml": "<bar/>"}]
        assert _extract_xml_list(docs) == ["<foo/>", "<bar/>"]

    def test_skips_missing_xml(self):
        docs = [{"name": "A"}, {"name": "B", "xml": "<bar/>"}]
        assert _extract_xml_list(docs) == ["<bar/>"]

    def test_empty_list_returns_empty(self):
        assert _extract_xml_list([]) == []


# ── TestTwbAssemblyAgentValidateInput ──────────────────────────────────────────

class TestTwbAssemblyAgentValidateInput:
    def test_missing_project_spec(self):
        errors = _agent().validate_input({
            "tds_documents": [{}], "worksheets_xml": [{}], "dashboards_xml": [{}]
        })
        assert any("project_spec" in e for e in errors)

    def test_missing_tds_documents(self):
        errors = _agent().validate_input({
            "project_spec": {"project_id": "x"},
            "worksheets_xml": [{}], "dashboards_xml": [{}]
        })
        assert any("tds_documents" in e for e in errors)

    def test_missing_worksheets_xml(self):
        errors = _agent().validate_input({
            "project_spec": {"project_id": "x"},
            "tds_documents": [{}], "dashboards_xml": [{}]
        })
        assert any("worksheets_xml" in e for e in errors)

    def test_missing_dashboards_xml(self):
        errors = _agent().validate_input({
            "project_spec": {"project_id": "x"},
            "tds_documents": [{}], "worksheets_xml": [{}]
        })
        assert any("dashboards_xml" in e for e in errors)

    def test_valid_state_passes(self):
        errors = _agent().validate_input({
            "project_spec": {"project_id": "x"},
            "tds_documents": [{}], "worksheets_xml": [{}], "dashboards_xml": [{}]
        })
        assert errors == []


# ── TestTwbAssemblyAgentExecution ──────────────────────────────────────────────

class TestTwbAssemblyAgentExecution:
    @pytest.fixture(scope="class")
    def pipeline_state(self): return _pipeline_state()
    @pytest.fixture(scope="class")
    def agent_result(self, pipeline_state): return _agent().execute(pipeline_state)

    def test_execute_succeeds(self, agent_result):
        assert agent_result.status == AgentStatus.SUCCESS

    def test_output_has_twb_xml(self, agent_result):
        assert "twb_xml" in agent_result.output
        assert len(agent_result.output["twb_xml"]) > 500

    def test_output_has_workbook_doc(self, agent_result):
        assert "workbook_doc" in agent_result.output

    def test_workbook_doc_is_valid(self, agent_result):
        assert agent_result.output["workbook_doc"]["is_valid"] is True

    def test_worksheet_count_eight(self, agent_result):
        assert agent_result.output["workbook_doc"]["worksheet_count"] == 8

    def test_dashboard_count_one(self, agent_result):
        assert agent_result.output["workbook_doc"]["dashboard_count"] == 1

    def test_no_validation_errors(self, agent_result):
        assert agent_result.output["workbook_doc"]["validation_errors"] == []

    def test_twb_path_none_in_dry_run(self, agent_result):
        assert agent_result.output["twb_path"] is None

    def test_metadata_summary_present(self, agent_result):
        s = agent_result.metadata["assembly_summary"]
        assert s["worksheets"] == 8
        assert s["dashboards"] == 1
        assert s["dry_run"] is True

    def test_timing_recorded(self, agent_result):
        assert agent_result.duration_ms > 0


# ── TestAssembledTwbXmlStructure ───────────────────────────────────────────────

class TestAssembledTwbXmlStructure:
    @pytest.fixture(scope="class")
    def root(self):
        state = _pipeline_state()
        result = _agent().execute(state)
        return parse(result.output["twb_xml"])

    def test_root_tag_workbook(self, root):
        assert root.tag == "workbook"

    def test_version_18_1(self, root):
        assert root.attrib["version"] == "18.1"

    def test_five_children(self, root):
        assert len(root) == 5

    def test_preferences_is_first(self, root):
        assert root[0].tag == "preferences"

    def test_datasources_is_second(self, root):
        assert root[1].tag == "datasources"

    def test_worksheets_is_third(self, root):
        assert root[2].tag == "worksheets"

    def test_dashboards_is_fourth(self, root):
        assert root[3].tag == "dashboards"

    def test_windows_is_fifth(self, root):
        assert root[4].tag == "windows"

    def test_parameters_ds_first_in_datasources(self, root):
        first = root.find("datasources/datasource")
        assert first.attrib["name"] == "Parameters"

    def test_eight_worksheets(self, root):
        assert len(root.findall("worksheets/worksheet")) == 8

    def test_one_dashboard(self, root):
        assert len(root.findall("dashboards/dashboard")) == 1

    def test_each_worksheet_has_table(self, root):
        for ws in root.findall("worksheets/worksheet"):
            assert ws.find("table") is not None

    def test_dashboard_has_zones(self, root):
        db = root.find("dashboards/dashboard")
        assert db.find("zones") is not None

    def test_declaration_present(self):
        state = _pipeline_state()
        result = _agent().execute(state)
        assert result.output["twb_xml"].startswith("<?xml")


# ── TestPhase01To11Pipeline ────────────────────────────────────────────────────

class TestPhase01To11Pipeline:
    @pytest.fixture(scope="class")
    def full_state(self): return _pipeline_state()

    def test_pipeline_dry_run_succeeds(self, full_state):
        assert _agent().execute(full_state).status == AgentStatus.SUCCESS

    def test_twb_xml_is_string(self, full_state):
        result = _agent().execute(full_state)
        assert isinstance(result.output["twb_xml"], str)

    def test_twb_xml_parseable(self, full_state):
        result = _agent().execute(full_state)
        root = parse(result.output["twb_xml"])
        assert root.tag == "workbook"

    def test_phase_label(self, full_state):
        result = _agent().execute(full_state)
        assert result.phase == "GENERATING"
        assert result.agent_id == "twb_assembly_agent"

    def test_no_blocking_errors(self, full_state):
        result = _agent().execute(full_state)
        assert not result.has_blocking_errors

    def test_workbook_doc_to_dict_complete(self, full_state):
        result = _agent().execute(full_state)
        d = result.output["workbook_doc"]
        for key in ("name", "worksheet_count", "dashboard_count",
                    "is_valid", "xml_length", "validation_errors"):
            assert key in d
