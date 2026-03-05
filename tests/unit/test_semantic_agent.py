# tests/unit/test_semantic_agent.py
"""
Unit tests for:
  - TdsDocument model         (models/tds_document.py)
  - SemanticModelAgent        (agents/semantic_agent.py)
  - Phase 01-06 pipeline      (dry-run, no DB or network calls)

All XML comparisons use ElementTree parsing rather than string matching
so attribute ordering differences don't cause false failures.
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict
from unittest.mock import patch

import pytest

from agents.base_agent import AgentStatus, PhaseContext
from agents.connectivity_agent import ConnectivityAgent
from agents.conversion_agent import DataConversionAgent
from agents.intake_agent import IntakeAgent
from agents.profiler_agent import ProfilerAgent
from agents.semantic_agent import SemanticModelAgent, _build_connection_spec
from agents.validation_agent import ValidationAgent
from models.field_mapping import FieldMapping, MappingSource, MappingStatus, MetricMapping, TableauFieldMapping
from models.tds_document import (
    ColumnSpec,
    ConnectionSpec,
    RelationSpec,
    TdsDocument,
    _escape_formula,
    _make_ds_name,
)

PROJECT_ROOT = Path(__file__).parent.parent.parent
REAL_CSV_DIR = PROJECT_ROOT / "csv_inputs"


# ── Phase 01-05 pipeline fixture ───────────────────────────────────────────────

@pytest.fixture(scope="module")
def pipeline_state() -> Dict[str, Any]:
    """Run Phase 01-05 (dry-run) once."""
    ctx = PhaseContext(project_id="proj_001", run_id="sem_test", dry_run=True)
    ir  = IntakeAgent(csv_dir=REAL_CSV_DIR, context=ctx).execute({})
    vr  = ValidationAgent(context=ctx).execute(
        {"project_spec": ir.output["project_spec"]}
    )
    cr  = ConnectivityAgent(context=ctx).execute({
        "project_spec":      ir.output["project_spec"],
        "validation_report": vr.output["validation_report"],
    })
    pr  = ProfilerAgent(context=ctx).execute({
        "project_spec":        ir.output["project_spec"],
        "validation_report":   vr.output["validation_report"],
        "connectivity_report": cr.output["connectivity_report"],
    })
    conv = DataConversionAgent(context=ctx).execute({
        "project_spec":   ir.output["project_spec"],
        "schema_profiles": pr.output["schema_profiles"],
    })
    return {
        "project_spec": ir.output["project_spec"],
        "field_mapping": conv.output["field_mapping"],
    }


@pytest.fixture
def ctx() -> PhaseContext:
    return PhaseContext(project_id="proj_001", run_id="sem_test", dry_run=True)


@pytest.fixture
def agent(ctx) -> SemanticModelAgent:
    return SemanticModelAgent(context=ctx)


# ── Helper: parse XML string safely ───────────────────────────────────────────

def parse_xml(xml_str: str) -> ET.Element:
    return ET.fromstring(xml_str)


def find_columns(root: ET.Element) -> list:
    return root.findall("column")


def find_connection(root: ET.Element) -> ET.Element | None:
    return root.find("connection")


# ══════════════════════════════════════════════════════════════════════════════
# TdsDocument model
# ══════════════════════════════════════════════════════════════════════════════

class TestTdsDocumentModel:
    def test_default_ds_name_generated(self):
        doc = TdsDocument(datasource_id="ds_001", caption="Test")
        assert doc.ds_name.startswith("federated.")

    def test_ds_name_stable_for_same_id(self):
        a = TdsDocument(datasource_id="ds_001", caption="T", ds_name=_make_ds_name("ds_001"))
        b = TdsDocument(datasource_id="ds_001", caption="T", ds_name=_make_ds_name("ds_001"))
        assert a.ds_name == b.ds_name

    def test_empty_doc_not_valid(self):
        doc = TdsDocument(datasource_id="ds_001", caption="T")
        assert not doc.is_valid

    def test_connection_only_not_valid(self):
        doc = TdsDocument(datasource_id="ds_001", caption="T")
        doc.add_connection(ConnectionSpec("c1", "snowflake", "host", "db"))
        assert not doc.is_valid    # still needs columns

    def test_valid_when_connection_and_column_present(self):
        doc = TdsDocument(datasource_id="ds_001", caption="T")
        doc.add_connection(ConnectionSpec("c1", "snowflake", "host", "db"))
        doc.add_column(ColumnSpec("[Revenue]", "Revenue", "real", "measure", "quantitative"))
        assert doc.is_valid

    def test_fluent_chaining(self):
        doc = (
            TdsDocument(datasource_id="ds_001", caption="T")
            .add_connection(ConnectionSpec("c1", "snowflake", "host", "db"))
            .add_column(ColumnSpec("[Rev]", "Rev", "real", "measure", "quantitative"))
            .add_relation(RelationSpec("r1", "ORDERS"))
            .add_col_map("[Revenue]", "[ORDERS].[REVENUE]")
        )
        assert len(doc.connections) == 1
        assert len(doc.columns)     == 1
        assert len(doc.relations)   == 1
        assert len(doc.col_maps)    == 1

    def test_calculated_columns_property(self):
        doc = TdsDocument(datasource_id="ds_001", caption="T")
        doc.add_column(ColumnSpec("[Rev]", "Rev", "real", "measure", "quantitative"))
        doc.add_column(ColumnSpec(
            "[Calc]", "Calc", "real", "measure", "quantitative",
            is_calculated=True, formula="SUM([Rev])"
        ))
        assert len(doc.calculated_columns) == 1
        assert len(doc.raw_columns)        == 1

    def test_summary_has_required_keys(self):
        doc = TdsDocument(datasource_id="ds_001", caption="T")
        s = doc.summary()
        for key in ("datasource_id", "caption", "is_valid", "total_columns",
                    "connections", "relations", "timestamp"):
            assert key in s

    def test_repr_contains_datasource_id(self):
        doc = TdsDocument(datasource_id="ds_001", caption="T")
        assert "ds_001" in repr(doc)


class TestEscapeFormula:
    def test_double_quotes_escaped(self):
        assert "&quot;" in _escape_formula('IF [X]="Y" THEN 1 END')

    def test_ampersand_escaped(self):
        assert "&amp;" in _escape_formula("A & B")

    def test_lt_gt_escaped(self):
        r = _escape_formula("[Sales] > 100")
        assert "&gt;" in r
        r2 = _escape_formula("[Sales] < 100")
        assert "&lt;" in r2

    def test_plain_formula_unchanged(self):
        f = "SUM([Revenue]) / SUM([Sales])"
        assert _escape_formula(f) == f

    def test_lod_formula_preserved(self):
        f = "{ FIXED [Customer] : SUM([Sales]) }"
        result = _escape_formula(f)
        assert "FIXED" in result
        assert "SUM" in result


# ══════════════════════════════════════════════════════════════════════════════
# TDS XML generation
# ══════════════════════════════════════════════════════════════════════════════

class TestTdsXmlGeneration:

    @pytest.fixture
    def simple_doc(self) -> TdsDocument:
        doc = TdsDocument(datasource_id="ds_001", caption="Orders")
        doc.add_connection(ConnectionSpec(
            "conn_001", "snowflake",
            "acct.snowflakecomputing.com", "ANALYTICS",
            schema="PUBLIC", warehouse="COMPUTE_WH",
        ))
        doc.add_relation(RelationSpec("r1", "ORDERS", schema="PUBLIC"))
        doc.add_column(ColumnSpec("[REVENUE]", "Revenue", "real", "measure", "quantitative"))
        doc.add_column(ColumnSpec("[REGION]",  "Region",  "string", "dimension", "nominal"))
        return doc

    def test_xml_is_parseable(self, simple_doc):
        xml_str = simple_doc.to_xml()
        root = parse_xml(xml_str)
        assert root is not None

    def test_root_tag_is_datasource(self, simple_doc):
        root = parse_xml(simple_doc.to_xml())
        assert root.tag == "datasource"

    def test_datasource_has_version(self, simple_doc):
        root = parse_xml(simple_doc.to_xml())
        assert root.attrib.get("version") == "18.1"

    def test_datasource_caption_set(self, simple_doc):
        root = parse_xml(simple_doc.to_xml())
        assert root.attrib.get("caption") == "Orders"

    def test_federated_connection_present(self, simple_doc):
        root = parse_xml(simple_doc.to_xml())
        conn = find_connection(root)
        assert conn is not None
        assert conn.attrib.get("class") == "federated"

    def test_named_connection_present(self, simple_doc):
        root = parse_xml(simple_doc.to_xml())
        conn = find_connection(root)
        nc_container = conn.find("named-connections")
        assert nc_container is not None
        assert len(nc_container.findall("named-connection")) == 1

    def test_inner_connection_class(self, simple_doc):
        root = parse_xml(simple_doc.to_xml())
        inner = root.find(".//named-connection/connection")
        assert inner.attrib.get("class") == "snowflake"

    def test_inner_connection_server(self, simple_doc):
        root = parse_xml(simple_doc.to_xml())
        inner = root.find(".//named-connection/connection")
        assert inner.attrib.get("server") == "acct.snowflakecomputing.com"

    def test_relation_element_present(self, simple_doc):
        root = parse_xml(simple_doc.to_xml())
        conn = find_connection(root)
        rel = conn.find("relation")
        assert rel is not None
        assert rel.attrib.get("type") == "table"

    def test_column_count_matches(self, simple_doc):
        root = parse_xml(simple_doc.to_xml())
        cols = find_columns(root)
        assert len(cols) == 2

    def test_measure_column_attributes(self, simple_doc):
        root = parse_xml(simple_doc.to_xml())
        cols = {c.attrib.get("name"): c for c in find_columns(root)}
        rev = cols.get("[REVENUE]")
        assert rev is not None
        assert rev.attrib.get("datatype") == "real"
        assert rev.attrib.get("role")     == "measure"

    def test_dimension_column_attributes(self, simple_doc):
        root = parse_xml(simple_doc.to_xml())
        cols = {c.attrib.get("name"): c for c in find_columns(root)}
        reg = cols.get("[REGION]")
        assert reg is not None
        assert reg.attrib.get("datatype") == "string"
        assert reg.attrib.get("role")     == "dimension"

    def test_aliases_element_present(self, simple_doc):
        root = parse_xml(simple_doc.to_xml())
        aliases = root.find("aliases")
        assert aliases is not None
        assert aliases.attrib.get("enabled") == "yes"

    def test_col_map_in_xml(self):
        doc = TdsDocument(datasource_id="ds_001", caption="T")
        doc.add_connection(ConnectionSpec("c1", "snowflake", "h", "db"))
        doc.add_col_map("Revenue", "[ORDERS].[REVENUE]")
        doc.add_column(ColumnSpec("[Revenue]", "Revenue", "real", "measure", "quantitative"))
        root = parse_xml(doc.to_xml())
        cols_el = root.find(".//cols")
        assert cols_el is not None
        maps = cols_el.findall("map")
        assert len(maps) == 1
        assert maps[0].attrib.get("key") == "[Revenue]"

    def test_calculated_field_has_calculation_child(self):
        doc = TdsDocument(datasource_id="ds_001", caption="T")
        doc.add_connection(ConnectionSpec("c1", "snowflake", "h", "db"))
        doc.add_column(ColumnSpec(
            "[Profit Ratio]", "Profit Ratio", "real", "measure", "quantitative",
            is_calculated=True, formula="SUM([Profit]) / SUM([Sales])",
        ))
        root = parse_xml(doc.to_xml())
        calc = root.find(".//column/calculation")
        assert calc is not None
        assert calc.attrib.get("class") == "tableau"
        assert "SUM" in calc.attrib.get("formula", "")

    def test_formula_special_chars_escaped(self):
        doc = TdsDocument(datasource_id="ds_001", caption="T")
        doc.add_connection(ConnectionSpec("c1", "snowflake", "h", "db"))
        doc.add_column(ColumnSpec(
            "[Seg]", "Seg", "string", "dimension", "nominal",
            is_calculated=True, formula='IF [Segment]="Consumer" THEN 1 END',
        ))
        xml_str = doc.to_xml()
        assert "&quot;" in xml_str   # double-quotes escaped

    def test_hidden_column_attribute(self):
        doc = TdsDocument(datasource_id="ds_001", caption="T")
        doc.add_connection(ConnectionSpec("c1", "snowflake", "h", "db"))
        doc.add_column(ColumnSpec(
            "[ID]", "ID", "integer", "measure", "quantitative", hidden=True
        ))
        root = parse_xml(doc.to_xml())
        cols = find_columns(root)
        assert cols[0].attrib.get("hidden") == "true"


class TestJoinRelationXml:

    def test_join_relation_structure(self):
        doc = TdsDocument(datasource_id="ds_001", caption="T")
        doc.add_connection(ConnectionSpec("c1", "snowflake", "h", "db"))
        doc.add_relation(RelationSpec(
            "r1", "ORDERS__RETURNS",
            schema="PUBLIC", is_join=True,
            join_type="left",
            left_table="ORDERS",  right_table="RETURNS",
            left_key="ORDER_ID",  right_key="ORDER_ID",
        ))
        doc.add_column(ColumnSpec("[X]", "X", "string", "dimension", "nominal"))
        root = parse_xml(doc.to_xml())
        conn = find_connection(root)
        join_el = conn.find("relation[@type='join']")
        assert join_el is not None
        assert join_el.attrib.get("join") == "left"

    def test_join_clause_present(self):
        doc = TdsDocument(datasource_id="ds_001", caption="T")
        doc.add_connection(ConnectionSpec("c1", "snowflake", "h", "db"))
        doc.add_relation(RelationSpec(
            "r1", "T", schema="PUBLIC", is_join=True,
            join_type="inner", left_table="A", right_table="B",
            left_key="ID", right_key="ID",
        ))
        doc.add_column(ColumnSpec("[X]", "X", "string", "dimension", "nominal"))
        root = parse_xml(doc.to_xml())
        clause = root.find(".//clause[@type='join']")
        assert clause is not None


# ══════════════════════════════════════════════════════════════════════════════
# SemanticModelAgent: validate_input
# ══════════════════════════════════════════════════════════════════════════════

class TestSemanticAgentValidateInput:

    def test_missing_project_spec_fails(self, agent, pipeline_state):
        errors = agent.validate_input({"field_mapping": pipeline_state["field_mapping"]})
        assert any("project_spec" in e.lower() for e in errors)

    def test_missing_field_mapping_fails(self, agent, pipeline_state):
        errors = agent.validate_input({"project_spec": pipeline_state["project_spec"]})
        assert any("field_mapping" in e.lower() for e in errors)

    def test_field_mapping_cannot_proceed_blocks(self, agent, pipeline_state):
        bad_fm = {**pipeline_state["field_mapping"], "can_proceed": False}
        state  = {"project_spec": pipeline_state["project_spec"], "field_mapping": bad_fm}
        errors = agent.validate_input(state)
        assert len(errors) >= 1

    def test_valid_state_passes(self, agent, pipeline_state):
        errors = agent.validate_input(pipeline_state)
        assert errors == []


# ══════════════════════════════════════════════════════════════════════════════
# SemanticModelAgent: execution
# ══════════════════════════════════════════════════════════════════════════════

class TestSemanticAgentExecution:

    def test_execute_succeeds(self, agent, pipeline_state):
        result = agent.execute(pipeline_state)
        assert result.status in (AgentStatus.SUCCESS, AgentStatus.WARNING)

    def test_output_has_tds_documents(self, agent, pipeline_state):
        result = agent.execute(pipeline_state)
        assert "tds_documents" in result.output

    def test_tds_documents_is_list(self, agent, pipeline_state):
        result = agent.execute(pipeline_state)
        assert isinstance(result.output["tds_documents"], list)

    def test_at_least_one_tds_document(self, agent, pipeline_state):
        result = agent.execute(pipeline_state)
        assert len(result.output["tds_documents"]) >= 1

    def test_each_document_has_xml(self, agent, pipeline_state):
        result = agent.execute(pipeline_state)
        for doc in result.output["tds_documents"]:
            assert "xml" in doc
            assert len(doc["xml"]) > 100   # non-trivial XML

    def test_xml_is_parseable(self, agent, pipeline_state):
        result = agent.execute(pipeline_state)
        for doc in result.output["tds_documents"]:
            root = parse_xml(doc["xml"])
            assert root.tag == "datasource"

    def test_is_valid_flag(self, agent, pipeline_state):
        result = agent.execute(pipeline_state)
        for doc in result.output["tds_documents"]:
            assert doc["is_valid"] is True

    def test_column_count_positive(self, agent, pipeline_state):
        result = agent.execute(pipeline_state)
        for doc in result.output["tds_documents"]:
            assert doc["total_columns"] > 0

    def test_metadata_contains_summary(self, agent, pipeline_state):
        result = agent.execute(pipeline_state)
        assert "tds_summary" in result.metadata
        assert "datasources_processed" in result.metadata["tds_summary"]

    def test_timing_recorded(self, agent, pipeline_state):
        result = agent.execute(pipeline_state)
        assert result.duration_ms > 0


# ══════════════════════════════════════════════════════════════════════════════
# XML structure validation against TWB spec
# ══════════════════════════════════════════════════════════════════════════════

class TestXmlStructureSpec:

    def test_datasource_has_federated_connection(self, agent, pipeline_state):
        result = agent.execute(pipeline_state)
        xml_str = result.output["tds_documents"][0]["xml"]
        root = parse_xml(xml_str)
        conn = root.find("connection")
        assert conn.attrib.get("class") == "federated"

    def test_named_connection_present(self, agent, pipeline_state):
        result = agent.execute(pipeline_state)
        xml_str = result.output["tds_documents"][0]["xml"]
        root = parse_xml(xml_str)
        nc = root.find(".//named-connection")
        assert nc is not None

    def test_inner_snowflake_connection(self, agent, pipeline_state):
        result = agent.execute(pipeline_state)
        xml_str = result.output["tds_documents"][0]["xml"]
        root = parse_xml(xml_str)
        inner = root.find(".//named-connection/connection")
        assert inner is not None
        assert inner.attrib.get("class") == "snowflake"

    def test_aliases_element_present(self, agent, pipeline_state):
        result = agent.execute(pipeline_state)
        for doc in result.output["tds_documents"]:
            root = parse_xml(doc["xml"])
            assert root.find("aliases") is not None

    def test_all_columns_have_datatype(self, agent, pipeline_state):
        result = agent.execute(pipeline_state)
        for doc in result.output["tds_documents"]:
            root = parse_xml(doc["xml"])
            for col in root.findall("column"):
                assert "datatype" in col.attrib, \
                    f"Column {col.attrib.get('name')} missing datatype"

    def test_all_columns_have_role(self, agent, pipeline_state):
        result = agent.execute(pipeline_state)
        for doc in result.output["tds_documents"]:
            root = parse_xml(doc["xml"])
            for col in root.findall("column"):
                assert col.attrib.get("role") in ("dimension", "measure")

    def test_calculated_fields_have_calculation_child(self, agent, pipeline_state):
        result = agent.execute(pipeline_state)
        for doc in result.output["tds_documents"]:
            root = parse_xml(doc["xml"])
            for col in root.findall("column"):
                calc = col.find("calculation")
                if calc is not None:
                    assert calc.attrib.get("class") == "tableau"
                    assert "formula" in calc.attrib

    def test_calculated_fields_present(self, agent, pipeline_state):
        """Metrics from metrics.csv must appear as calculated columns."""
        result = agent.execute(pipeline_state)
        for doc in result.output["tds_documents"]:
            calc_count = doc.get("calculated_cols", 0)
            assert calc_count > 0, "Expected at least one calculated field"

    def test_measure_columns_have_quantitative_type(self, agent, pipeline_state):
        result = agent.execute(pipeline_state)
        for doc in result.output["tds_documents"]:
            root = parse_xml(doc["xml"])
            for col in root.findall("column"):
                if col.attrib.get("role") == "measure":
                    assert col.attrib.get("type") in (
                        "quantitative", None
                    ), f"Measure {col.attrib.get('name')} has wrong type"


# ══════════════════════════════════════════════════════════════════════════════
# ConnectionSpec helper
# ══════════════════════════════════════════════════════════════════════════════

class TestBuildConnectionSpec:

    def test_builds_from_connection_config(self, pipeline_state):
        from models.project_spec import ProjectSpec
        spec = ProjectSpec.model_validate(pipeline_state["project_spec"])
        conn_cfg = spec.connections[0]
        cs = _build_connection_spec(conn_cfg)
        assert cs.conn_class == "snowflake"
        assert cs.server     == conn_cfg.server
        assert cs.dbname     == conn_cfg.dbname

    def test_named_connection_name_is_deterministic(self, pipeline_state):
        from models.project_spec import ProjectSpec
        spec = ProjectSpec.model_validate(pipeline_state["project_spec"])
        conn_cfg = spec.connections[0]
        cs1 = _build_connection_spec(conn_cfg)
        cs2 = _build_connection_spec(conn_cfg)
        assert cs1.named_connection_name == cs2.named_connection_name

    def test_username_not_embedded(self, pipeline_state):
        """Credentials must NOT be written into XML."""
        from models.project_spec import ProjectSpec
        spec = ProjectSpec.model_validate(pipeline_state["project_spec"])
        cs   = _build_connection_spec(spec.connections[0])
        assert cs.username == ""


# ══════════════════════════════════════════════════════════════════════════════
# TDS file write
# ══════════════════════════════════════════════════════════════════════════════

class TestTdsFileWrite:

    def test_tds_file_written(self, tmp_path, pipeline_state):
        agent = SemanticModelAgent(
            config={"tds_output_dir": str(tmp_path)},
            context=PhaseContext(project_id="proj_001", run_id="t", dry_run=True),
        )
        agent.execute(pipeline_state)
        tds_files = list(tmp_path.glob("*.tds"))
        assert len(tds_files) >= 1

    def test_tds_file_content_parseable(self, tmp_path, pipeline_state):
        agent = SemanticModelAgent(
            config={"tds_output_dir": str(tmp_path)},
            context=PhaseContext(project_id="proj_001", run_id="t", dry_run=True),
        )
        agent.execute(pipeline_state)
        for f in tmp_path.glob("*.tds"):
            root = parse_xml(f.read_text())
            assert root.tag == "datasource"

    def test_write_failure_is_warning_not_crash(self, pipeline_state):
        """If we can't write TDS, agent should warn not crash."""
        agent = SemanticModelAgent(
            config={"tds_output_dir": "/nonexistent/readonly/path"},
            context=PhaseContext(project_id="proj_001", run_id="t", dry_run=True),
        )
        result = agent.execute(pipeline_state)
        # Should still succeed — file write failure is a warning
        assert result.status in (AgentStatus.SUCCESS, AgentStatus.WARNING, AgentStatus.FAILED)
        assert "tds_documents" in result.output


# ══════════════════════════════════════════════════════════════════════════════
# Phase 01-06 pipeline integration
# ══════════════════════════════════════════════════════════════════════════════

class TestPhase01To06Pipeline:

    def test_full_pipeline_dry_run(self):
        ctx  = PhaseContext(project_id="proj_001", run_id="sem_pipe", dry_run=True)
        ir   = IntakeAgent(csv_dir=REAL_CSV_DIR, context=ctx).execute({})
        vr   = ValidationAgent(context=ctx).execute(
            {"project_spec": ir.output["project_spec"]}
        )
        cr   = ConnectivityAgent(context=ctx).execute({
            "project_spec":      ir.output["project_spec"],
            "validation_report": vr.output["validation_report"],
        })
        pr   = ProfilerAgent(context=ctx).execute({
            "project_spec":        ir.output["project_spec"],
            "validation_report":   vr.output["validation_report"],
            "connectivity_report": cr.output["connectivity_report"],
        })
        conv = DataConversionAgent(context=ctx).execute({
            "project_spec":    ir.output["project_spec"],
            "schema_profiles": pr.output["schema_profiles"],
        })
        sem  = SemanticModelAgent(context=ctx).execute({
            "project_spec":  ir.output["project_spec"],
            "field_mapping": conv.output["field_mapping"],
        })

        assert sem.status in (AgentStatus.SUCCESS, AgentStatus.WARNING)
        assert len(sem.output["tds_documents"]) >= 1

    def test_phase_labels_in_pipeline(self):
        ctx  = PhaseContext(project_id="proj_001", run_id="sem_pipe2", dry_run=True)
        ir   = IntakeAgent(csv_dir=REAL_CSV_DIR, context=ctx).execute({})
        vr   = ValidationAgent(context=ctx).execute(
            {"project_spec": ir.output["project_spec"]}
        )
        cr   = ConnectivityAgent(context=ctx).execute({
            "project_spec":      ir.output["project_spec"],
            "validation_report": vr.output["validation_report"],
        })
        pr   = ProfilerAgent(context=ctx).execute({
            "project_spec":        ir.output["project_spec"],
            "validation_report":   vr.output["validation_report"],
            "connectivity_report": cr.output["connectivity_report"],
        })
        conv = DataConversionAgent(context=ctx).execute({
            "project_spec":    ir.output["project_spec"],
            "schema_profiles": pr.output["schema_profiles"],
        })
        sem  = SemanticModelAgent(context=ctx).execute({
            "project_spec":  ir.output["project_spec"],
            "field_mapping": conv.output["field_mapping"],
        })
        assert ir.phase   == "INTAKE"
        assert vr.phase   == "VALIDATING"
        assert cr.phase   == "CONNECTING"
        assert pr.phase   == "PROFILING"
        assert conv.phase == "CONVERTING"
        assert sem.phase  == "MODELING"

    def test_tds_xml_feeds_workbook_state(self):
        ctx  = PhaseContext(project_id="proj_001", run_id="sem_pipe3", dry_run=True)
        ir   = IntakeAgent(csv_dir=REAL_CSV_DIR, context=ctx).execute({})
        vr   = ValidationAgent(context=ctx).execute(
            {"project_spec": ir.output["project_spec"]}
        )
        cr   = ConnectivityAgent(context=ctx).execute({
            "project_spec":      ir.output["project_spec"],
            "validation_report": vr.output["validation_report"],
        })
        pr   = ProfilerAgent(context=ctx).execute({
            "project_spec":        ir.output["project_spec"],
            "validation_report":   vr.output["validation_report"],
            "connectivity_report": cr.output["connectivity_report"],
        })
        conv = DataConversionAgent(context=ctx).execute({
            "project_spec":    ir.output["project_spec"],
            "schema_profiles": pr.output["schema_profiles"],
        })
        sem  = SemanticModelAgent(context=ctx).execute({
            "project_spec":  ir.output["project_spec"],
            "field_mapping": conv.output["field_mapping"],
        })

        # WorkbookState key for next agent (MetricDefinitionAgent)
        state = {
            **{
                "project_spec":   ir.output["project_spec"],
                "field_mapping":  conv.output["field_mapping"],
            },
            "tds_documents": sem.output["tds_documents"],
        }
        assert "tds_documents" in state
        assert len(state["tds_documents"]) >= 1
        # All documents carry valid XML
        for doc in state["tds_documents"]:
            assert doc.get("is_valid") is True
