# tests/unit/test_profiler_agent.py
"""Unit Tests — ProfilerAgent and SchemaProfile (all DB calls mocked)."""

import sys
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from agents.base_agent import AgentStatus, PhaseContext
from agents.connectivity_agent import ConnectivityAgent
from agents.intake_agent import IntakeAgent
from agents.profiler_agent import ProfilerAgent, _env
from agents.validation_agent import ValidationAgent
from models.connectivity_report import (
    ConnectivityReport, ConnectionHealth, ConnectionResult, TableauCloudResult,
)
from models.schema_profile import (
    ColumnProfile, SchemaProfile, TableProfile, map_physical_to_tableau,
)

PROJECT_ROOT = Path(__file__).parent.parent.parent
REAL_CSV_DIR = PROJECT_ROOT / "csv_inputs"


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def pipeline_state() -> Dict[str, Any]:
    ctx = PhaseContext(project_id="proj_001", run_id="run_test", dry_run=True)
    intake = IntakeAgent(csv_dir=REAL_CSV_DIR, context=ctx)
    ir = intake.execute({})
    val = ValidationAgent(context=ctx)
    vr = val.execute({"project_spec": ir.output["project_spec"]})
    conn_report = ConnectivityReport(project_id="proj_001", run_id="run_test")
    conn_report.add_result(ConnectionResult(
        connection_id="conn_001", health=ConnectionHealth.GREEN,
        host="test.snowflakecomputing.com", db_class="snowflake",
        tcp_ok=True, auth_ok=True, query_ok=True,
    ))
    conn_report.tableau_cloud = TableauCloudResult(ok=True, site_id="test")
    return {
        "project_spec":       ir.output["project_spec"],
        "validation_report":  vr.output["validation_report"],
        "connectivity_report": conn_report.to_dict(),
    }

@pytest.fixture
def dry_run_context():
    return PhaseContext(project_id="proj_001", run_id="run_test", dry_run=True)

@pytest.fixture
def live_context():
    return PhaseContext(project_id="proj_001", run_id="run_test", dry_run=False)

@pytest.fixture
def dry_agent(dry_run_context):
    return ProfilerAgent(context=dry_run_context)

@pytest.fixture
def live_agent(live_context):
    return ProfilerAgent(context=live_context)

def _red_state(pipeline_state):
    r = ConnectivityReport(project_id="proj_001", run_id="test")
    r.add_result(ConnectionResult(connection_id="conn_001",
                                  health=ConnectionHealth.RED, host="h"))
    r.tableau_cloud = TableauCloudResult(ok=True)
    return {**pipeline_state, "connectivity_report": r.to_dict()}

def _green_state(pipeline_state):
    r = ConnectivityReport(project_id="proj_001", run_id="test")
    r.add_result(ConnectionResult(connection_id="conn_001",
                                  health=ConnectionHealth.GREEN, host="h",
                                  tcp_ok=True, auth_ok=True, query_ok=True))
    r.tableau_cloud = TableauCloudResult(ok=True)
    return {**pipeline_state, "connectivity_report": r.to_dict()}


# ══════════════════════════════════════════════════════════════════════════════
class TestMapPhysicalToTableau:
    def test_varchar_maps_to_string(self):       assert map_physical_to_tableau("VARCHAR") == "string"
    def test_varchar_with_precision(self):        assert map_physical_to_tableau("VARCHAR(255)") == "string"
    def test_number_with_scale_to_real(self):     assert map_physical_to_tableau("NUMBER(18,2)") == "real"
    def test_number_no_scale_to_real(self):       assert map_physical_to_tableau("NUMBER") == "real"
    def test_integer_maps_to_integer(self):       assert map_physical_to_tableau("INTEGER") == "integer"
    def test_bigint_maps_to_integer(self):        assert map_physical_to_tableau("BIGINT") == "integer"
    def test_boolean_maps_to_boolean(self):       assert map_physical_to_tableau("BOOLEAN") == "boolean"
    def test_date_maps_to_date(self):             assert map_physical_to_tableau("DATE") == "date"
    def test_timestamp_ntz_to_datetime(self):     assert map_physical_to_tableau("TIMESTAMP_NTZ") == "datetime"
    def test_timestamp_to_datetime(self):         assert map_physical_to_tableau("TIMESTAMP") == "datetime"
    def test_float_maps_to_real(self):            assert map_physical_to_tableau("FLOAT") == "real"
    def test_unknown_type_maps_to_string(self):   assert map_physical_to_tableau("TOTALLY_UNKNOWN") == "string"
    def test_lowercase_normalized(self):          assert map_physical_to_tableau("varchar") == "string"
    def test_text_maps_to_string(self):           assert map_physical_to_tableau("TEXT") == "string"


# ══════════════════════════════════════════════════════════════════════════════
class TestColumnProfile:
    def test_type_match_true_when_agree(self):
        col = ColumnProfile("REVENUE","NUMBER(18,2)","real",declared_datatype="real")
        assert col.type_match is True

    def test_type_match_false_when_differ(self):
        col = ColumnProfile("ID","NUMBER(10,0)","real",declared_datatype="integer")
        assert col.type_match is False

    def test_type_match_true_when_no_declared(self):
        col = ColumnProfile("X","TEXT","string")
        assert col.type_match is True

    def test_null_rate_computed(self):
        col = ColumnProfile("X","TEXT","string",row_count=100,null_count=10)
        assert col.null_rate == 0.1

    def test_null_rate_none_when_row_count_missing(self):
        col = ColumnProfile("X","TEXT","string",null_count=5)
        assert col.null_rate is None

    def test_to_dict_has_required_keys(self):
        col = ColumnProfile("AMT","NUMBER(18,2)","real",declared_datatype="real")
        d = col.to_dict()
        for k in ["physical_name","physical_type","tableau_datatype","type_match","nullable","sample_values"]:
            assert k in d

    def test_sample_values_capped_at_5(self):
        col = ColumnProfile("X","TEXT","string",sample_values=["a","b","c","d","e","f","g"])
        assert len(col.to_dict()["sample_values"]) == 5


# ══════════════════════════════════════════════════════════════════════════════
class TestTableProfile:
    def test_add_column_fluent(self):
        tp = TableProfile("t1","ORDERS",schema="PUBLIC")
        tp.add_column(ColumnProfile("ID","INTEGER","integer"))
        assert len(tp.columns) == 1

    def test_type_mismatches_detected(self):
        tp = TableProfile("t1","ORDERS",profiled=True)
        tp.add_column(ColumnProfile("ID","NUMBER(10,0)","real",declared_datatype="integer"))
        assert len(tp.type_mismatches) == 1

    def test_no_mismatches_when_match(self):
        tp = TableProfile("t1","ORDERS",profiled=True)
        tp.add_column(ColumnProfile("REVENUE","NUMBER(18,2)","real",declared_datatype="real"))
        assert len(tp.type_mismatches) == 0

    def test_undeclared_columns_detected(self):
        tp = TableProfile("t1","ORDERS",profiled=True)
        tp.add_column(ColumnProfile("MYSTERY_COL","TEXT","string",declared_name=""))
        assert len(tp.undeclared_columns) == 1

    def test_get_column_case_insensitive(self):
        tp = TableProfile("t1","ORDERS",profiled=True)
        tp.add_column(ColumnProfile("REVENUE","NUMBER","real"))
        assert tp.get_column("revenue") is not None
        assert tp.get_column("REVENUE") is not None

    def test_get_column_missing_returns_none(self):
        tp = TableProfile("t1","ORDERS",profiled=True)
        assert tp.get_column("NONEXISTENT") is None

    def test_to_dict_has_required_keys(self):
        tp = TableProfile("t1","ORDERS",profiled=True)
        d = tp.to_dict()
        for k in ["table_id","table_name","schema","profiled","column_count","type_mismatches","columns"]:
            assert k in d


# ══════════════════════════════════════════════════════════════════════════════
class TestSchemaProfile:
    def test_empty_can_proceed_true(self):
        # Empty datasource (e.g. published) has nothing to profile -> trivially ok
        assert SchemaProfile(datasource_id="ds_001").can_proceed is True

    def test_all_unprofiled_tables_cannot_proceed(self):
        p = SchemaProfile(datasource_id="ds_001")
        p.add_table(TableProfile("t1","ORDERS",profiled=False))
        assert p.can_proceed is False

    def test_one_profiled_can_proceed(self):
        p = SchemaProfile(datasource_id="ds_001")
        p.add_table(TableProfile("t1","ORDERS",profiled=True))
        assert p.can_proceed is True

    def test_mixed_can_proceed(self):
        p = SchemaProfile(datasource_id="ds_001")
        p.add_table(TableProfile("t1","ORDERS",    profiled=True))
        p.add_table(TableProfile("t2","CUSTOMERS", profiled=False))
        assert p.can_proceed is True

    def test_total_type_mismatches_aggregated(self):
        p = SchemaProfile(datasource_id="ds_001")
        tp = TableProfile("t1","ORDERS",profiled=True)
        tp.add_column(ColumnProfile("ID","NUMBER","real",declared_datatype="integer"))
        p.add_table(tp)
        assert p.total_type_mismatches == 1

    def test_total_columns_aggregated(self):
        p = SchemaProfile(datasource_id="ds_001")
        tp = TableProfile("t1","T",profiled=True)
        tp.add_column(ColumnProfile("A","TEXT","string"))
        tp.add_column(ColumnProfile("B","INTEGER","integer"))
        p.add_table(tp)
        assert p.total_columns == 2

    def test_get_table_case_insensitive(self):
        p = SchemaProfile(datasource_id="ds_001")
        p.add_table(TableProfile("t1","ORDERS",profiled=True))
        assert p.get_table("orders") is not None
        assert p.get_table("ORDERS") is not None

    def test_get_table_missing_returns_none(self):
        p = SchemaProfile(datasource_id="ds_001")
        assert p.get_table("NONEXISTENT") is None

    def test_summary_has_all_keys(self):
        s = SchemaProfile(datasource_id="ds_001").summary()
        for k in ["datasource_id","can_proceed","total_tables","profiled_tables",
                  "unprofiled_tables","total_columns","type_mismatches"]:
            assert k in s

    def test_to_dict_includes_tables(self):
        p = SchemaProfile(datasource_id="ds_001")
        p.add_table(TableProfile("t1","ORDERS",profiled=True))
        assert len(p.to_dict()["tables"]) == 1

    def test_repr_contains_datasource(self):
        assert "ds_001" in repr(SchemaProfile(datasource_id="ds_001"))

    def test_fluent_chaining(self):
        p = (SchemaProfile(datasource_id="ds_001")
             .add_table(TableProfile("t1","A",profiled=True))
             .add_table(TableProfile("t2","B",profiled=True)))
        assert len(p.tables) == 2


# ══════════════════════════════════════════════════════════════════════════════
class TestProfilerAgentValidateInput:
    def test_missing_project_spec_fails(self, dry_agent):
        errors = dry_agent.validate_input({"connectivity_report": {"can_proceed": True}})
        assert any("project_spec" in e.lower() for e in errors)

    def test_missing_connectivity_report_fails(self, dry_agent, pipeline_state):
        errors = dry_agent.validate_input({"project_spec": pipeline_state["project_spec"]})
        assert any("connectivity_report" in e.lower() for e in errors)

    def test_connectivity_report_present_passes(self, dry_agent):
        # An empty dict is fine at validate_input stage; run() handles errors gracefully
        errors = dry_agent.validate_input({
            "project_spec": {"dummy": 1},
            "connectivity_report": {"can_proceed": True}
        })
        assert errors == []

    def test_valid_state_passes(self, dry_agent, pipeline_state):
        assert dry_agent.validate_input(pipeline_state) == []


# ══════════════════════════════════════════════════════════════════════════════
class TestDryRunProfiling:
    def test_dry_run_succeeds(self, dry_agent, pipeline_state):
        result = dry_agent.execute(pipeline_state)
        assert result.status == AgentStatus.SUCCESS

    def test_produces_schema_profiles(self, dry_agent, pipeline_state):
        result = dry_agent.execute(pipeline_state)
        assert "schema_profiles" in result.output
        assert len(result.output["schema_profiles"]) >= 1

    def test_tables_are_profiled(self, dry_agent, pipeline_state):
        result = dry_agent.execute(pipeline_state)
        # Only check datasources that have tables declared (skip published datasources)
        profiles_with_tables = [p for p in result.output["schema_profiles"] if p["total_tables"] > 0]
        assert len(profiles_with_tables) >= 1
        for p in profiles_with_tables:
            assert p["profiled_tables"] >= 1

    def test_can_proceed_all_true(self, dry_agent, pipeline_state):
        result = dry_agent.execute(pipeline_state)
        for p in result.output["schema_profiles"]:
            assert p["can_proceed"] is True

    def test_columns_populated(self, dry_agent, pipeline_state):
        result = dry_agent.execute(pipeline_state)
        # At least one datasource has columns; published datasources may have 0
        total_cols = sum(p["total_columns"] for p in result.output["schema_profiles"])
        assert total_cols > 0

    def test_no_db_calls_in_dry_run(self, dry_agent, pipeline_state):
        with patch.object(dry_agent, "_introspect_snowflake") as mock_sf:
            dry_agent.execute(pipeline_state)
            mock_sf.assert_not_called()

    def test_metadata_populated(self, dry_agent, pipeline_state):
        result = dry_agent.execute(pipeline_state)
        assert "profiling_summary" in result.metadata
        assert "datasources_profiled" in result.metadata["profiling_summary"]
        assert "total_columns"        in result.metadata["profiling_summary"]

    def test_timing_recorded(self, dry_agent, pipeline_state):
        result = dry_agent.execute(pipeline_state)
        assert result.duration_ms > 0


# ══════════════════════════════════════════════════════════════════════════════
class TestRedConnectionPath:
    def test_red_marks_tables_unprofiled(self, live_context, pipeline_state):
        result = ProfilerAgent(context=live_context).execute(_red_state(pipeline_state))
        for p in result.output["schema_profiles"]:
            assert p["profiled_tables"] == 0

    def test_red_produces_failed_status(self, live_context, pipeline_state):
        result = ProfilerAgent(context=live_context).execute(_red_state(pipeline_state))
        assert result.status == AgentStatus.FAILED

    def test_red_no_introspect_calls(self, live_context, pipeline_state):
        agent = ProfilerAgent(context=live_context)
        with patch.object(agent, "_introspect_snowflake") as mock_sf:
            agent.execute(_red_state(pipeline_state))
            mock_sf.assert_not_called()


# ══════════════════════════════════════════════════════════════════════════════
class TestGreenPathMockedDriver:
    _mock_cols = [
        ("ORDER_ID",  "NUMBER(10,0)", "YES"),
        ("REVENUE",   "NUMBER(18,2)", "NO"),
        ("REGION",    "VARCHAR",      "YES"),
        ("ORDER_DATE","DATE",         "YES"),
    ]

    def test_green_snowflake_profiling(self, live_context, pipeline_state):
        agent = ProfilerAgent(context=live_context)
        with patch.object(agent, "_introspect_snowflake",
                          return_value=(self._mock_cols, 125000, None)):
            result = agent.execute(_green_state(pipeline_state))
        assert "schema_profiles" in result.output

    def test_profiled_table_has_columns(self, live_context, pipeline_state):
        agent = ProfilerAgent(context=live_context)
        with patch.object(agent, "_introspect_snowflake",
                          return_value=([("ORDER_ID","INTEGER","NO"),("AMOUNT","NUMBER","YES")], 5000, None)):
            result = agent.execute(_green_state(pipeline_state))
        for p in result.output["schema_profiles"]:
            for tbl in p["tables"]:
                if tbl["profiled"]:
                    assert tbl["column_count"] > 0

    def test_row_count_captured(self, live_context, pipeline_state):
        agent = ProfilerAgent(context=live_context)
        with patch.object(agent, "_introspect_snowflake",
                          return_value=([("ID","INTEGER","NO")], 99999, None)):
            result = agent.execute(_green_state(pipeline_state))
        for p in result.output["schema_profiles"]:
            for tbl in p["tables"]:
                if tbl["profiled"]:
                    assert tbl["row_count"] == 99999


# ══════════════════════════════════════════════════════════════════════════════
class TestMissingDriverDegradation:
    def test_driver_error_does_not_crash(self, live_context, pipeline_state):
        agent = ProfilerAgent(context=live_context)
        with patch.object(agent, "_introspect_snowflake",
                          return_value=([], None, "snowflake-connector-python not installed")):
            result = agent.execute(_green_state(pipeline_state))
        assert "schema_profiles" in result.output

    def test_introspect_error_marks_table_unprofiled(self, live_context, pipeline_state):
        agent = ProfilerAgent(context=live_context)
        with patch.object(agent, "_introspect_snowflake",
                          return_value=([], None, "Connection timed out")):
            result = agent.execute(_green_state(pipeline_state))
        for p in result.output["schema_profiles"]:
            for tbl in p["tables"]:
                assert tbl["profiled"] is False


# ══════════════════════════════════════════════════════════════════════════════
class TestTypeMismatchDetection:
    def test_mismatch_on_model_level(self):
        tp = TableProfile("t1","ORDERS",profiled=True)
        tp.add_column(ColumnProfile("ORDER_ID","NUMBER(10,0)","real",
                                    declared_name="ORDER_ID",declared_datatype="integer"))
        assert len(tp.type_mismatches) == 1

    def test_no_mismatch_when_consistent(self):
        tp = TableProfile("t1","ORDERS",profiled=True)
        tp.add_column(ColumnProfile("NAME","VARCHAR","string",declared_datatype="string"))
        assert len(tp.type_mismatches) == 0


# ══════════════════════════════════════════════════════════════════════════════
class TestProfilerAgentConfig:
    def test_agent_id(self, dry_agent):               assert dry_agent.agent_id == "profiler_agent"
    def test_phase(self, dry_agent):                  assert dry_agent.phase == "PROFILING"
    def test_row_count_default_enabled(self, dry_run_context):
        assert ProfilerAgent(context=dry_run_context).enable_row_counts is True
    def test_row_count_disabled_via_config(self, dry_run_context):
        assert ProfilerAgent(config={"enable_row_count_sampling": False},
                             context=dry_run_context).enable_row_counts is False
    def test_null_profiling_default_disabled(self, dry_run_context):
        assert ProfilerAgent(context=dry_run_context).enable_null_profile is False


# ══════════════════════════════════════════════════════════════════════════════
class TestEnvHelper:
    def test_returns_env_value(self, monkeypatch):
        monkeypatch.setenv("PROFILER_TEST_VAR", "hello")
        assert _env("PROFILER_TEST_VAR") == "hello"

    def test_returns_empty_for_unset(self, monkeypatch):
        monkeypatch.delenv("PROFILER_TEST_UNSET", raising=False)
        assert _env("PROFILER_TEST_UNSET") == ""

    def test_returns_empty_for_none(self):
        assert _env(None) == ""


# ══════════════════════════════════════════════════════════════════════════════
class TestFullPipelineDryRun:
    def test_all_four_phases_complete(self):
        ctx = PhaseContext(project_id="proj_001", run_id="pipe_test", dry_run=True)
        ir = IntakeAgent(csv_dir=REAL_CSV_DIR, context=ctx).execute({})
        assert ir.status in (AgentStatus.SUCCESS, AgentStatus.WARNING)

        vr = ValidationAgent(context=ctx).execute({"project_spec": ir.output["project_spec"]})
        assert vr.status in (AgentStatus.SUCCESS, AgentStatus.WARNING)

        cr = ConnectivityAgent(context=ctx).execute({
            "project_spec":      ir.output["project_spec"],
            "validation_report": vr.output["validation_report"],
        })
        assert cr.status == AgentStatus.SUCCESS

        pr = ProfilerAgent(context=ctx).execute({
            "project_spec":       ir.output["project_spec"],
            "validation_report":  vr.output["validation_report"],
            "connectivity_report": cr.output["connectivity_report"],
        })
        assert pr.status == AgentStatus.SUCCESS
        assert pr.output["schema_profiles"][0]["can_proceed"] is True

    def test_phase_labels_in_pipeline(self):
        ctx = PhaseContext(project_id="proj_001", run_id="label_test", dry_run=True)
        ir = IntakeAgent(csv_dir=REAL_CSV_DIR, context=ctx).execute({})
        vr = ValidationAgent(context=ctx).execute({"project_spec": ir.output["project_spec"]})
        cr = ConnectivityAgent(context=ctx).execute({
            "project_spec":      ir.output["project_spec"],
            "validation_report": vr.output["validation_report"],
        })
        pr = ProfilerAgent(context=ctx).execute({
            "project_spec":       ir.output["project_spec"],
            "validation_report":  vr.output["validation_report"],
            "connectivity_report": cr.output["connectivity_report"],
        })
        assert ir.phase == "INTAKE"
        assert vr.phase == "VALIDATING"
        assert cr.phase == "CONNECTING"
        assert pr.phase == "PROFILING"
