# tests/unit/test_intake_agent.py
"""
Unit Tests — IntakeAgent and ProjectSpec
=========================================

Tests use the real csv_inputs/ fixture files (the Superstore sample data).
Additional fixture CSVs are created in tmp_path for failure/edge case tests.
No live API calls, no database connections.

Run from project root:
    pytest tests/unit/test_intake_agent.py -v
"""

import shutil
import sys
from pathlib import Path
from typing import Dict

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from agents.base_agent import AgentStatus, ErrorSeverity, PhaseContext
from agents.intake_agent import IntakeAgent
from models.project_spec import (
    AuthConfig,
    ColumnConfig,
    ConnectionConfig,
    DashboardRequirement,
    DataSourceConfig,
    DimensionConfig,
    FigmaLayout,
    MetricConfig,
    MstrAttribute,
    MstrMetric,
    ProjectConfig,
    ProjectSpec,
    RelationshipConfig,
    TableConfig,
)

# ── Paths ──────────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).parent.parent.parent
REAL_CSV_DIR = PROJECT_ROOT / "csv_inputs"


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_context():
    return PhaseContext(
        project_id="proj_test",
        run_id="run_001",
        environment="dev",
    )


@pytest.fixture
def minimal_state():
    return {}


@pytest.fixture
def valid_csv_dir(tmp_path):
    """
    Copy the real csv_inputs/ into a tmp_path so tests can modify without
    affecting the real files.
    """
    csv_dir = tmp_path / "csv_inputs"
    shutil.copytree(REAL_CSV_DIR, csv_dir)
    return csv_dir


# ══════════════════════════════════════════════════════════════════════════════
# ProjectConfig model
# ══════════════════════════════════════════════════════════════════════════════

class TestProjectConfig:
    def test_valid_construction(self):
        cfg = ProjectConfig(
            project_id="p1",
            project_name="Test Project",
            tableau_site="mysite",
            tableau_server_url="https://test.online.tableau.com",
            target_project="Sales",
            workbook_name="My_Dashboard",
        )
        assert cfg.project_id   == "p1"
        assert cfg.environment  == "dev"   # default
        assert cfg.enable_extract is False  # default

    def test_environment_normalised_to_lowercase(self):
        cfg = ProjectConfig(
            project_id="p1", project_name="n", tableau_site="s",
            tableau_server_url="https://x", target_project="p",
            workbook_name="w", environment="PROD",
        )
        assert cfg.environment == "prod"

    def test_invalid_environment_raises(self):
        with pytest.raises(Exception, match="environment"):
            ProjectConfig(
                project_id="p1", project_name="n", tableau_site="s",
                tableau_server_url="https://x", target_project="p",
                workbook_name="w", environment="sandbox",
            )

    def test_enable_extract_from_string_true(self):
        cfg = ProjectConfig(
            project_id="p1", project_name="n", tableau_site="s",
            tableau_server_url="https://x", target_project="p",
            workbook_name="w", enable_extract="true",
        )
        assert cfg.enable_extract is True

    def test_enable_extract_from_string_false(self):
        cfg = ProjectConfig(
            project_id="p1", project_name="n", tableau_site="s",
            tableau_server_url="https://x", target_project="p",
            workbook_name="w", enable_extract="false",
        )
        assert cfg.enable_extract is False


# ══════════════════════════════════════════════════════════════════════════════
# Individual row models
# ══════════════════════════════════════════════════════════════════════════════

class TestColumnConfig:
    def test_valid_column(self):
        col = ColumnConfig(
            column_id="col_001", table_id="tbl_001",
            column_name="REVENUE", datatype="real", role="measure",
        )
        assert col.datatype == "real"
        assert col.role     == "measure"
        assert col.hidden   is False

    def test_datatype_normalized(self):
        col = ColumnConfig(
            column_id="c", table_id="t",
            column_name="N", datatype="STRING", role="dimension",
        )
        assert col.datatype == "string"

    def test_invalid_datatype_raises(self):
        with pytest.raises(Exception, match="datatype"):
            ColumnConfig(
                column_id="c", table_id="t",
                column_name="N", datatype="BIGINT", role="dimension",
            )

    def test_invalid_role_raises(self):
        with pytest.raises(Exception, match="role"):
            ColumnConfig(
                column_id="c", table_id="t",
                column_name="N", datatype="string", role="attribute",
            )

    def test_hidden_from_string(self):
        col = ColumnConfig(
            column_id="c", table_id="t", column_name="N",
            datatype="string", role="dimension", hidden="true",
        )
        assert col.hidden is True


class TestMetricConfig:
    def test_is_lod_from_string(self):
        m = MetricConfig(
            metric_id="m1", datasource_id="ds1",
            metric_name="Test", formula="{ FIXED [X] : SUM([Y]) }",
            datatype="real", is_lod="true",
        )
        assert m.is_lod is True

    def test_formula_preserved(self):
        formula = "SUM([Profit]) / SUM([Sales])"
        m = MetricConfig(
            metric_id="m1", datasource_id="ds1",
            metric_name="Profit Ratio", formula=formula, datatype="real",
        )
        assert m.formula == formula


class TestDashboardRequirement:
    def test_pipe_separated_rows(self):
        req = DashboardRequirement(
            view_id="v1", view_name="Test", view_type="worksheet",
            datasource_id="ds1", rows="Sub-Category|Category",
        )
        assert req.row_fields == ["Sub-Category", "Category"]

    def test_pipe_separated_dashboard_views(self):
        req = DashboardRequirement(
            view_id="dash1", view_name="Overview", view_type="dashboard",
            datasource_id="ds1", views_in_dashboard="v1|v2|v3",
        )
        assert req.dashboard_view_ids == ["v1", "v2", "v3"]

    def test_empty_rows_returns_empty_list(self):
        req = DashboardRequirement(
            view_id="v1", view_name="Test", view_type="worksheet",
            datasource_id="ds1",
        )
        assert req.row_fields == []

    def test_width_height_defaults(self):
        req = DashboardRequirement(
            view_id="v1", view_name="Test", view_type="worksheet",
            datasource_id="ds1",
        )
        assert req.width_px  == 1366
        assert req.height_px == 768


class TestDimensionConfig:
    def test_column_ids_parsed(self):
        dim = DimensionConfig(
            dimension_id="d1", datasource_id="ds1",
            dimension_name="Product Hierarchy",
            dimension_type="hierarchy",
            columns="col_001|col_002|col_003",
        )
        assert dim.column_ids == ["col_001", "col_002", "col_003"]


class TestConnectionConfig:
    def test_port_as_string_parsed(self):
        conn = ConnectionConfig(
            connection_id="c1", **{"class": "snowflake"},
            server="acct.snowflakecomputing.com",
            dbname="ANALYTICS", **{"schema": "PUBLIC"},
            auth_method="Username Password", auth_id="auth_001",
            port="443",
        )
        assert conn.port == 443

    def test_port_empty_string_becomes_none(self):
        conn = ConnectionConfig(
            connection_id="c1", **{"class": "snowflake"},
            server="acct.snowflakecomputing.com",
            dbname="ANALYTICS", **{"schema": "PUBLIC"},
            auth_method="Username Password", auth_id="auth_001",
            port="",
        )
        assert conn.port is None


# ══════════════════════════════════════════════════════════════════════════════
# ProjectSpec.from_csv_dir
# ══════════════════════════════════════════════════════════════════════════════

class TestProjectSpecFromCsvDir:
    def test_loads_successfully_from_real_csvs(self):
        spec = ProjectSpec.from_csv_dir(REAL_CSV_DIR)
        assert spec.project_config.project_id == "proj_001"

    def test_all_counts_match_csv_rows(self):
        spec = ProjectSpec.from_csv_dir(REAL_CSV_DIR)
        assert len(spec.data_sources)           == 2
        assert len(spec.tables)                 == 3
        assert len(spec.columns)                == 25
        assert len(spec.metrics)                == 10
        assert len(spec.dimensions)             == 5
        assert len(spec.dashboard_requirements) == 9
        assert len(spec.mstr_attributes)        == 13
        assert len(spec.mstr_metrics)           == 12

    def test_worksheets_and_dashboards_split(self):
        spec       = ProjectSpec.from_csv_dir(REAL_CSV_DIR)
        worksheets = spec.get_worksheets()
        dashboards = spec.get_dashboards()
        assert len(worksheets) == 8
        assert len(dashboards) == 1

    def test_primary_datasource_identified(self):
        spec = ProjectSpec.from_csv_dir(REAL_CSV_DIR)
        primary = spec.get_primary_datasource()
        assert primary is not None
        assert primary.datasource_name == "Superstore Orders"

    def test_get_connection_by_id(self):
        spec = ProjectSpec.from_csv_dir(REAL_CSV_DIR)
        conn = spec.get_connection("conn_001")
        assert conn is not None
        assert "snowflake" in conn.class_.lower()

    def test_get_tables_for_datasource(self):
        spec   = ProjectSpec.from_csv_dir(REAL_CSV_DIR)
        tables = spec.get_tables_for_datasource("ds_001")
        assert len(tables) >= 1

    def test_get_columns_for_table(self):
        spec    = ProjectSpec.from_csv_dir(REAL_CSV_DIR)
        tables  = spec.get_tables_for_datasource("ds_001")
        if tables:
            cols = spec.get_columns_for_table(tables[0].table_id)
            assert len(cols) >= 1

    def test_lod_metrics_identified(self):
        spec    = ProjectSpec.from_csv_dir(REAL_CSV_DIR)
        lod_metrics = [m for m in spec.metrics if m.is_lod]
        assert len(lod_metrics) >= 1

    def test_all_mstr_metrics_have_tableau_formula(self):
        spec = ProjectSpec.from_csv_dir(REAL_CSV_DIR)
        for m in spec.mstr_metrics:
            assert m.tableau_formula is not None and m.tableau_formula.strip() != "", \
                f"MSTR metric {m.mstr_metric_id} has no tableau_formula"

    def test_summary_dict_has_all_keys(self):
        spec = ProjectSpec.from_csv_dir(REAL_CSV_DIR)
        s = spec.summary()
        assert "project_id"   in s
        assert "data_sources" in s
        assert "columns"      in s
        assert "worksheets"   in s
        assert "dashboards"   in s

    def test_missing_directory_raises(self, tmp_path):
        with pytest.raises(ValueError, match="Missing required CSV files"):
            ProjectSpec.from_csv_dir(tmp_path / "nonexistent")

    def test_missing_required_file_raises(self, tmp_path):
        # Copy CSVs but omit project_config.csv
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        for f in REAL_CSV_DIR.glob("*.csv"):
            if f.name != "project_config.csv":
                shutil.copy(f, csv_dir / f.name)
        with pytest.raises(ValueError, match="project_config.csv"):
            ProjectSpec.from_csv_dir(csv_dir)


# ══════════════════════════════════════════════════════════════════════════════
# IntakeAgent.validate_input
# ══════════════════════════════════════════════════════════════════════════════

class TestIntakeAgentValidateInput:
    def test_valid_csv_dir_passes_validation(self, sample_context):
        agent  = IntakeAgent(csv_dir=REAL_CSV_DIR, context=sample_context)
        errors = agent.validate_input({})
        assert errors == []

    def test_missing_csv_dir_returns_error(self, tmp_path, sample_context):
        agent  = IntakeAgent(csv_dir=tmp_path / "nonexistent", context=sample_context)
        errors = agent.validate_input({})
        assert len(errors) >= 1
        assert any("not found" in e.lower() for e in errors)

    def test_missing_project_config_returns_error(self, tmp_path, sample_context):
        csv_dir = tmp_path / "empty_csv"
        csv_dir.mkdir()
        agent  = IntakeAgent(csv_dir=csv_dir, context=sample_context)
        errors = agent.validate_input({})
        assert len(errors) >= 1
        assert any("project_config" in e.lower() for e in errors)


# ══════════════════════════════════════════════════════════════════════════════
# IntakeAgent.execute — success path
# ══════════════════════════════════════════════════════════════════════════════

class TestIntakeAgentSuccess:
    def test_execute_success_with_real_csvs(self, sample_context, minimal_state):
        agent  = IntakeAgent(csv_dir=REAL_CSV_DIR, context=sample_context)
        result = agent.execute(minimal_state)

        assert result.status in (AgentStatus.SUCCESS, AgentStatus.WARNING), \
            f"Expected SUCCESS or WARNING, got {result.status}: " \
            f"{[e.message for e in result.errors]}"

    def test_project_spec_in_output(self, sample_context, minimal_state):
        agent  = IntakeAgent(csv_dir=REAL_CSV_DIR, context=sample_context)
        result = agent.execute(minimal_state)

        assert "project_spec"  in result.output
        assert "intake_report" in result.output

    def test_project_spec_has_correct_project_id(self, sample_context, minimal_state):
        agent  = IntakeAgent(csv_dir=REAL_CSV_DIR, context=sample_context)
        result = agent.execute(minimal_state)

        assert result.output["project_spec"]["project_config"]["project_id"] == "proj_001"

    def test_intake_report_has_row_counts(self, sample_context, minimal_state):
        agent  = IntakeAgent(csv_dir=REAL_CSV_DIR, context=sample_context)
        result = agent.execute(minimal_state)
        report = result.output["intake_report"]

        assert "row_counts"      in report
        assert "columns.csv"     in report["row_counts"]
        assert report["row_counts"]["columns.csv"] == 25

    def test_timing_recorded(self, sample_context, minimal_state):
        agent  = IntakeAgent(csv_dir=REAL_CSV_DIR, context=sample_context)
        result = agent.execute(minimal_state)
        assert result.duration_ms > 0


# ══════════════════════════════════════════════════════════════════════════════
# IntakeAgent.execute — failure paths
# ══════════════════════════════════════════════════════════════════════════════

class TestIntakeAgentFailures:
    def test_missing_csv_dir_fails(self, tmp_path, sample_context, minimal_state):
        agent  = IntakeAgent(csv_dir=tmp_path / "nope", context=sample_context)
        result = agent.execute(minimal_state)
        assert result.status == AgentStatus.FAILED

    def test_missing_required_file_fails(self, tmp_path, sample_context, minimal_state):
        """Remove a required CSV — agent should FAILED with CRITICAL error."""
        csv_dir = tmp_path / "csv"
        shutil.copytree(REAL_CSV_DIR, csv_dir)
        (csv_dir / "metrics.csv").unlink()

        agent  = IntakeAgent(csv_dir=csv_dir, context=sample_context)
        result = agent.execute(minimal_state)

        assert result.status == AgentStatus.FAILED
        assert any("metrics.csv" in e.message for e in result.errors)
        assert any(e.severity == ErrorSeverity.CRITICAL for e in result.errors)

    def test_missing_required_column_fails(self, tmp_path, sample_context, minimal_state):
        """Remove a required column from a CSV — agent should fail."""
        csv_dir = tmp_path / "csv"
        shutil.copytree(REAL_CSV_DIR, csv_dir)

        df = pd.read_csv(csv_dir / "metrics.csv")
        df = df.drop(columns=["formula"])
        df.to_csv(csv_dir / "metrics.csv", index=False)

        agent  = IntakeAgent(csv_dir=csv_dir, context=sample_context)
        result = agent.execute(minimal_state)

        assert result.status == AgentStatus.FAILED
        assert any("formula" in e.message for e in result.errors)

    def test_fk_violation_fails(self, tmp_path, sample_context, minimal_state):
        """Introduce an FK violation in tables.csv → unknown datasource_id."""
        csv_dir = tmp_path / "csv"
        shutil.copytree(REAL_CSV_DIR, csv_dir)

        df = pd.read_csv(csv_dir / "tables.csv")
        df.loc[0, "datasource_id"] = "ds_DOES_NOT_EXIST"
        df.to_csv(csv_dir / "tables.csv", index=False)

        agent  = IntakeAgent(csv_dir=csv_dir, context=sample_context)
        result = agent.execute(minimal_state)

        assert result.status == AgentStatus.FAILED
        assert any("ds_DOES_NOT_EXIST" in e.message for e in result.errors)

    def test_empty_metric_formula_fails(self, tmp_path, sample_context, minimal_state):
        """A metric with empty formula should produce a blocking error."""
        csv_dir = tmp_path / "csv"
        shutil.copytree(REAL_CSV_DIR, csv_dir)

        df = pd.read_csv(csv_dir / "metrics.csv")
        df.loc[0, "formula"] = ""
        df.to_csv(csv_dir / "metrics.csv", index=False)

        agent  = IntakeAgent(csv_dir=csv_dir, context=sample_context)
        result = agent.execute(minimal_state)

        assert result.status == AgentStatus.FAILED
        assert any("empty formula" in e.message.lower() for e in result.errors)

    def test_empty_required_file_fails(self, tmp_path, sample_context, minimal_state):
        """A CSV with 0 data rows should fail."""
        csv_dir = tmp_path / "csv"
        shutil.copytree(REAL_CSV_DIR, csv_dir)

        df = pd.read_csv(csv_dir / "metrics.csv")
        empty_df = df.head(0)   # keep headers, no rows
        empty_df.to_csv(csv_dir / "metrics.csv", index=False)

        agent  = IntakeAgent(csv_dir=csv_dir, context=sample_context)
        result = agent.execute(minimal_state)

        assert result.status == AgentStatus.FAILED

    def test_optional_file_missing_is_warning_not_failure(
        self, tmp_path, sample_context, minimal_state
    ):
        """figma_layout.csv is optional — missing it should warn, not fail."""
        csv_dir = tmp_path / "csv"
        shutil.copytree(REAL_CSV_DIR, csv_dir)
        figma = csv_dir / "figma_layout.csv"
        if figma.exists():
            figma.unlink()

        agent  = IntakeAgent(csv_dir=csv_dir, context=sample_context)
        result = agent.execute(minimal_state)

        # Should succeed (or warn) but not FAIL
        assert result.status != AgentStatus.FAILED, \
            f"Should not fail on missing optional file. Errors: {[e.message for e in result.errors]}"
        assert result.warning_count >= 1
        assert any("figma_layout.csv" in w.message for w in result.warnings)

    def test_invalid_dashboard_view_reference_fails(
        self, tmp_path, sample_context, minimal_state
    ):
        """Dashboard referencing non-existent view_id should fail."""
        csv_dir = tmp_path / "csv"
        shutil.copytree(REAL_CSV_DIR, csv_dir)

        df = pd.read_csv(csv_dir / "dashboard_requirements.csv")
        # Find dashboard row and corrupt its views_in_dashboard
        dash_idx = df[df["view_type"] == "dashboard"].index
        if len(dash_idx) > 0:
            df.loc[dash_idx[0], "views_in_dashboard"] = "view_NONEXISTENT|view_FAKE"
            df.to_csv(csv_dir / "dashboard_requirements.csv", index=False)

            agent  = IntakeAgent(csv_dir=csv_dir, context=sample_context)
            result = agent.execute(minimal_state)

            assert result.status == AgentStatus.FAILED
            assert any("view_NONEXISTENT" in e.message for e in result.errors)

    def test_multiple_primary_datasources_is_warning(
        self, tmp_path, sample_context, minimal_state
    ):
        """Having 2 primary data sources should warn but not block."""
        csv_dir = tmp_path / "csv"
        shutil.copytree(REAL_CSV_DIR, csv_dir)

        df = pd.read_csv(csv_dir / "data_sources.csv")
        df["is_primary"] = "true"   # all rows marked primary
        df.to_csv(csv_dir / "data_sources.csv", index=False)

        agent  = IntakeAgent(csv_dir=csv_dir, context=sample_context)
        result = agent.execute(minimal_state)

        # Should succeed or warn, not fail
        assert result.status != AgentStatus.FAILED
        assert any("primary" in w.message.lower() for w in result.warnings)


# ══════════════════════════════════════════════════════════════════════════════
# IntakeAgent — default csv_dir from config
# ══════════════════════════════════════════════════════════════════════════════

class TestIntakeAgentConfig:
    def test_csv_dir_from_config_dict(self, sample_context, minimal_state):
        """IntakeAgent can receive csv_dir via config dict."""
        agent  = IntakeAgent(
            config={"csv_dir": str(REAL_CSV_DIR)},
            context=sample_context,
        )
        result = agent.execute(minimal_state)
        assert result.status != AgentStatus.FAILED

    def test_csv_dir_argument_overrides_config(self, sample_context, minimal_state):
        """Explicit csv_dir argument wins over config dict."""
        agent = IntakeAgent(
            csv_dir=REAL_CSV_DIR,
            config={"csv_dir": "/some/other/path"},
            context=sample_context,
        )
        assert agent.csv_dir == REAL_CSV_DIR

    def test_agent_id_is_intake_agent(self, sample_context):
        agent = IntakeAgent(context=sample_context)
        assert agent.agent_id == "intake_agent"

    def test_phase_is_intake(self, sample_context):
        agent = IntakeAgent(context=sample_context)
        assert agent.phase == "INTAKE"
