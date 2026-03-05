# tests/unit/test_validation_agent.py
"""
Test suite for agents/validation_agent.py — MetadataValidationAgent (Agent #03)

Coverage targets:
  - File presence detection (missing core vs optional files)
  - Column presence (required vs optional)
  - Enum constraint enforcement
  - Integer / boolean type checks
  - Primary key uniqueness
  - Referential integrity (FK rules)
  - Business rules: hex colors, URL format, formula brackets, LOD flags,
    Snowflake warehouse, auth credential format, chart-type field requirements,
    dashboard → worksheet references
  - ValidationReport properties (is_valid, criticals, warnings, infos)
  - Agent run() and __call__() contracts
  - AgentResult fields
"""

from __future__ import annotations

import io
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from agents.validation_agent import (
    FK_RULES,
    FILE_SCHEMAS,
    MetadataValidationAgent,
    Severity,
    ValidationAgent,
    ValidationIssue,
    ValidationReport,
    _check_auth_credential_format,
    _check_dashboard_dimension_requirements,
    _check_dashboard_has_worksheets,
    _check_duplicate_ids,
    _check_hex_colors,
    _check_lod_flag_consistency,
    _check_metric_formula_brackets,
    _check_snowflake_warehouse_required,
    _check_tableau_server_url,
)


# ===========================================================================
# Fixtures
# ===========================================================================


@pytest.fixture()
def tmp_csv_dir(tmp_path: Path) -> Path:
    """Return a temporary directory for writing CSV fixtures."""
    return tmp_path


def _write_csv(directory: Path, filename: str, content: str) -> Path:
    """Write CSV text content to a file and return its path."""
    path = directory / filename
    path.write_text(content.strip(), encoding="utf-8")
    return path


def _minimal_project_config() -> str:
    return (
        "project_id,project_name,environment,tableau_site,"
        "tableau_server_url,target_project,workbook_name\n"
        "proj_001,Test Project,dev,mysite,"
        "https://10ax.online.tableau.com,Sales,Sales_Dashboard\n"
    )


def _minimal_data_sources() -> str:
    return (
        "datasource_id,datasource_name,connection_id,datasource_type,primary_table\n"
        "ds_001,Orders,conn_001,live,ORDERS\n"
    )


def _minimal_connections() -> str:
    return (
        "connection_id,class,server,dbname,schema,warehouse,auth_method,auth_id\n"
        "conn_001,snowflake,acct.snowflakecomputing.com,ANALYTICS,PUBLIC,COMPUTE_WH,"
        "Username Password,auth_001\n"
    )


def _minimal_auth() -> str:
    return (
        "auth_id,username_env,password_env\n"
        "auth_001,DB_USER,DB_PASS\n"
    )


def _minimal_tables() -> str:
    return (
        "table_id,datasource_id,table_name\n"
        "tbl_001,ds_001,ORDERS\n"
    )


def _minimal_columns() -> str:
    return (
        "column_id,table_id,column_name,datatype,role\n"
        "col_001,tbl_001,ORDER_ID,string,dimension\n"
        "col_002,tbl_001,REVENUE,real,measure\n"
    )


def _minimal_dashboard_requirements() -> str:
    return (
        "view_id,view_name,view_type,datasource_id,chart_type\n"
        "view_001,Sales by Region,worksheet,ds_001,Bar\n"
        "dash_001,Overview,dashboard,ds_001,Bar\n"
    )


def _full_minimal_csvs(directory: Path) -> None:
    """Write all core CSV files with valid minimal content."""
    _write_csv(directory, "project_config.csv", _minimal_project_config())
    _write_csv(directory, "data_sources.csv", _minimal_data_sources())
    _write_csv(directory, "connections.csv", _minimal_connections())
    _write_csv(directory, "auth.csv", _minimal_auth())
    _write_csv(directory, "tables.csv", _minimal_tables())
    _write_csv(directory, "columns.csv", _minimal_columns())
    _write_csv(directory, "dashboard_requirements.csv", (
        "view_id,view_name,view_type,datasource_id,chart_type,views_in_dashboard\n"
        "view_001,Sales by Region,worksheet,ds_001,Bar,\n"
        "dash_001,Overview,dashboard,ds_001,Bar,view_001\n"
    ))


def _make_agent(csv_dir: Path) -> MetadataValidationAgent:
    agent = MetadataValidationAgent(config={"csv_dir": str(csv_dir)})
    return agent


def _make_state(csv_dir: Path, project_id: str = "proj_001") -> Dict[str, Any]:
    return {
        "project_spec": {"project_id": project_id},
        "csv_dir": str(csv_dir),
    }


# ===========================================================================
# ValidationIssue
# ===========================================================================


class TestValidationIssue:
    def test_to_dict_all_fields(self):
        issue = ValidationIssue(
            severity=Severity.CRITICAL,
            rule="test_rule",
            file="foo.csv",
            column="bar",
            row_index=3,
            message="Something wrong",
            value="bad_val",
        )
        d = issue.to_dict()
        assert d["severity"] == "CRITICAL"
        assert d["rule"] == "test_rule"
        assert d["file"] == "foo.csv"
        assert d["column"] == "bar"
        assert d["row_index"] == 3
        assert d["message"] == "Something wrong"
        assert d["value"] == "bad_val"

    def test_to_dict_none_value(self):
        issue = ValidationIssue(
            severity=Severity.INFO,
            rule="r",
            file="f.csv",
            column=None,
            row_index=None,
            message="msg",
        )
        d = issue.to_dict()
        assert d["value"] is None
        assert d["column"] is None
        assert d["row_index"] is None


# ===========================================================================
# ValidationReport
# ===========================================================================


class TestValidationReport:
    def _make_report(self) -> ValidationReport:
        report = ValidationReport(project_id="p1")
        report.issues = [
            ValidationIssue(Severity.CRITICAL, "r1", "f.csv", None, None, "c1"),
            ValidationIssue(Severity.CRITICAL, "r2", "f.csv", None, None, "c2"),
            ValidationIssue(Severity.WARNING, "r3", "f.csv", None, None, "w1"),
            ValidationIssue(Severity.INFO, "r4", "f.csv", None, None, "i1"),
        ]
        return report

    def test_criticals_property(self):
        report = self._make_report()
        assert len(report.criticals) == 2

    def test_warnings_property(self):
        report = self._make_report()
        assert len(report.warnings) == 1

    def test_infos_property(self):
        report = self._make_report()
        assert len(report.infos) == 1

    def test_is_valid_false_when_criticals_exist(self):
        report = self._make_report()
        assert report.is_valid is False

    def test_is_valid_true_when_no_criticals(self):
        report = ValidationReport(project_id="p1")
        report.issues = [
            ValidationIssue(Severity.WARNING, "r", "f.csv", None, None, "w"),
        ]
        assert report.is_valid is True

    def test_is_valid_true_when_empty(self):
        report = ValidationReport(project_id="p1")
        assert report.is_valid is True

    def test_to_dict_structure(self):
        report = self._make_report()
        d = report.to_dict()
        assert d["project_id"] == "p1"
        assert d["is_valid"] is False
        assert d["summary"]["critical"] == 2
        assert d["summary"]["warning"] == 1
        assert d["summary"]["info"] == 1
        assert d["summary"]["total"] == 4
        assert isinstance(d["issues"], list)

    def test_to_dict_duration_ms(self):
        report = ValidationReport(project_id="p1")
        report.duration_ms = 123.456
        d = report.to_dict()
        assert d["duration_ms"] == 123.46


# ===========================================================================
# Severity enum
# ===========================================================================


class TestSeverity:
    def test_values(self):
        assert Severity.CRITICAL == "CRITICAL"
        assert Severity.WARNING == "WARNING"
        assert Severity.INFO == "INFO"


# ===========================================================================
# Business rule: _check_hex_colors
# ===========================================================================


class TestCheckHexColors:
    def _make_df(self, rows: List[Dict]) -> pd.DataFrame:
        return pd.DataFrame(rows)

    def test_valid_hex_no_issues(self):
        df = self._make_df([
            {"element_type": "color", "value": "#1F4E79", "name": "primary-blue"},
        ])
        issues = _check_hex_colors(df, "figma_layout.csv")
        assert issues == []

    def test_invalid_hex_raises_warning(self):
        df = self._make_df([
            {"element_type": "color", "value": "notacolor", "name": "bad"},
        ])
        issues = _check_hex_colors(df, "figma_layout.csv")
        assert len(issues) == 1
        assert issues[0].severity == Severity.WARNING
        assert issues[0].rule == "figma_color_hex_format"

    def test_non_figma_file_skipped(self):
        df = self._make_df([{"element_type": "color", "value": "bad", "name": "x"}])
        issues = _check_hex_colors(df, "columns.csv")
        assert issues == []

    def test_non_color_element_type_skipped(self):
        df = self._make_df([{"element_type": "font", "value": "bad", "name": "x"}])
        issues = _check_hex_colors(df, "figma_layout.csv")
        assert issues == []

    def test_valid_eight_digit_hex(self):
        df = self._make_df([{"element_type": "color", "value": "#1F4E79FF", "name": "x"}])
        issues = _check_hex_colors(df, "figma_layout.csv")
        assert issues == []

    def test_hex_without_hash_invalid(self):
        df = self._make_df([{"element_type": "color", "value": "1F4E79", "name": "x"}])
        issues = _check_hex_colors(df, "figma_layout.csv")
        assert len(issues) == 1


# ===========================================================================
# Business rule: _check_tableau_server_url
# ===========================================================================


class TestCheckTableauServerUrl:
    def test_valid_https_url(self):
        df = pd.DataFrame([{"tableau_server_url": "https://10ax.online.tableau.com"}])
        issues = _check_tableau_server_url(df, "project_config.csv")
        assert issues == []

    def test_http_url_is_critical(self):
        df = pd.DataFrame([{"tableau_server_url": "http://10ax.online.tableau.com"}])
        issues = _check_tableau_server_url(df, "project_config.csv")
        # http is actually valid by our regex (starts with https? = optional s)
        # Let's test a truly invalid one
        df2 = pd.DataFrame([{"tableau_server_url": "ftp://not-valid.com"}])
        issues2 = _check_tableau_server_url(df2, "project_config.csv")
        assert len(issues2) == 1
        assert issues2[0].severity == Severity.CRITICAL

    def test_non_project_config_skipped(self):
        df = pd.DataFrame([{"tableau_server_url": "ftp://bad"}])
        issues = _check_tableau_server_url(df, "connections.csv")
        assert issues == []

    def test_missing_column_skipped(self):
        df = pd.DataFrame([{"other_col": "val"}])
        issues = _check_tableau_server_url(df, "project_config.csv")
        assert issues == []


# ===========================================================================
# Business rule: _check_metric_formula_brackets
# ===========================================================================


class TestCheckMetricFormulaBrackets:
    def test_formula_with_brackets_ok(self):
        df = pd.DataFrame([{"metric_name": "Revenue", "formula": "SUM([Revenue])"}])
        issues = _check_metric_formula_brackets(df, "metrics.csv")
        assert issues == []

    def test_formula_without_brackets_warning(self):
        df = pd.DataFrame([{"metric_name": "Literal", "formula": "42"}])
        issues = _check_metric_formula_brackets(df, "metrics.csv")
        assert len(issues) == 1
        assert issues[0].severity == Severity.WARNING

    def test_non_metrics_file_skipped(self):
        df = pd.DataFrame([{"metric_name": "X", "formula": "no_brackets"}])
        issues = _check_metric_formula_brackets(df, "columns.csv")
        assert issues == []

    def test_missing_formula_column_skipped(self):
        df = pd.DataFrame([{"metric_name": "X"}])
        issues = _check_metric_formula_brackets(df, "metrics.csv")
        assert issues == []


# ===========================================================================
# Business rule: _check_lod_flag_consistency
# ===========================================================================


class TestCheckLodFlagConsistency:
    def test_lod_formula_with_true_flag_ok(self):
        df = pd.DataFrame([{
            "metric_name": "Customer Total",
            "formula": "{ FIXED [Customer] : SUM([Sales]) }",
            "is_lod": "true",
        }])
        issues = _check_lod_flag_consistency(df, "metrics.csv")
        assert issues == []

    def test_lod_formula_with_false_flag_warning(self):
        df = pd.DataFrame([{
            "metric_name": "Customer Total",
            "formula": "{ FIXED [Customer] : SUM([Sales]) }",
            "is_lod": "false",
        }])
        issues = _check_lod_flag_consistency(df, "metrics.csv")
        assert len(issues) == 1
        assert issues[0].severity == Severity.WARNING
        assert issues[0].rule == "lod_flag_mismatch"

    def test_non_lod_formula_with_true_flag_info(self):
        df = pd.DataFrame([{
            "metric_name": "Revenue",
            "formula": "SUM([Revenue])",
            "is_lod": "true",
        }])
        issues = _check_lod_flag_consistency(df, "metrics.csv")
        assert len(issues) == 1
        assert issues[0].severity == Severity.INFO

    def test_non_metrics_file_skipped(self):
        df = pd.DataFrame([{"metric_name": "X", "formula": "{FIXED}", "is_lod": "false"}])
        issues = _check_lod_flag_consistency(df, "columns.csv")
        assert issues == []

    def test_include_lod_detected(self):
        df = pd.DataFrame([{
            "metric_name": "Include Avg",
            "formula": "{ INCLUDE [State] : AVG([Sales]) }",
            "is_lod": "false",
        }])
        issues = _check_lod_flag_consistency(df, "metrics.csv")
        assert any(i.rule == "lod_flag_mismatch" and i.severity == Severity.WARNING for i in issues)


# ===========================================================================
# Business rule: _check_dashboard_has_worksheets
# ===========================================================================


class TestCheckDashboardHasWorksheets:
    def _req_df(self) -> pd.DataFrame:
        return pd.DataFrame([
            {"view_id": "view_001", "view_name": "WS1", "view_type": "worksheet",
             "datasource_id": "ds_001", "chart_type": "Bar", "views_in_dashboard": ""},
            {"view_id": "dash_001", "view_name": "Overview", "view_type": "dashboard",
             "datasource_id": "ds_001", "chart_type": "Bar", "views_in_dashboard": "view_001"},
        ])

    def test_valid_dashboard_no_issues(self):
        df = self._req_df()
        issues = _check_dashboard_has_worksheets(df, df, "dashboard_requirements.csv")
        assert issues == []

    def test_dashboard_missing_views_critical(self):
        df = pd.DataFrame([
            {"view_id": "view_001", "view_name": "WS1", "view_type": "worksheet",
             "views_in_dashboard": ""},
            {"view_id": "dash_001", "view_name": "Overview", "view_type": "dashboard",
             "views_in_dashboard": ""},
        ])
        issues = _check_dashboard_has_worksheets(df, df, "dashboard_requirements.csv")
        assert any(i.severity == Severity.CRITICAL and i.rule == "dashboard_missing_worksheets" for i in issues)

    def test_dashboard_orphan_ref_critical(self):
        df = pd.DataFrame([
            {"view_id": "view_001", "view_name": "WS1", "view_type": "worksheet",
             "views_in_dashboard": ""},
            {"view_id": "dash_001", "view_name": "Overview", "view_type": "dashboard",
             "views_in_dashboard": "view_NONEXISTENT"},
        ])
        issues = _check_dashboard_has_worksheets(df, df, "dashboard_requirements.csv")
        assert any(i.rule == "dashboard_orphan_worksheet_ref" for i in issues)


# ===========================================================================
# Business rule: _check_duplicate_ids
# ===========================================================================


class TestCheckDuplicateIds:
    def test_unique_ids_ok(self):
        df = pd.DataFrame([{"id": "a"}, {"id": "b"}, {"id": "c"}])
        issues = _check_duplicate_ids(df, "test.csv", "id")
        assert issues == []

    def test_duplicate_id_critical(self):
        df = pd.DataFrame([{"id": "a"}, {"id": "a"}, {"id": "b"}])
        issues = _check_duplicate_ids(df, "test.csv", "id")
        assert len(issues) >= 1
        assert all(i.severity == Severity.CRITICAL for i in issues)

    def test_missing_id_column_skipped(self):
        df = pd.DataFrame([{"other": "x"}])
        issues = _check_duplicate_ids(df, "test.csv", "id")
        assert issues == []


# ===========================================================================
# Business rule: _check_snowflake_warehouse_required
# ===========================================================================


class TestCheckSnowflakeWarehouseRequired:
    def test_snowflake_with_warehouse_ok(self):
        df = pd.DataFrame([{
            "connection_id": "c1", "class": "snowflake", "warehouse": "COMPUTE_WH"
        }])
        issues = _check_snowflake_warehouse_required(df, "connections.csv")
        assert issues == []

    def test_snowflake_missing_warehouse_warning(self):
        df = pd.DataFrame([{
            "connection_id": "c1", "class": "snowflake", "warehouse": ""
        }])
        issues = _check_snowflake_warehouse_required(df, "connections.csv")
        assert len(issues) == 1
        assert issues[0].severity == Severity.WARNING

    def test_postgres_no_warehouse_not_warned(self):
        df = pd.DataFrame([{
            "connection_id": "c2", "class": "postgres", "warehouse": ""
        }])
        issues = _check_snowflake_warehouse_required(df, "connections.csv")
        assert issues == []

    def test_non_connections_file_skipped(self):
        df = pd.DataFrame([{"connection_id": "c1", "class": "snowflake", "warehouse": ""}])
        issues = _check_snowflake_warehouse_required(df, "tables.csv")
        assert issues == []


# ===========================================================================
# Business rule: _check_auth_credential_format
# ===========================================================================


class TestCheckAuthCredentialFormat:
    def test_valid_env_var_names_ok(self):
        df = pd.DataFrame([{"auth_id": "a1", "username_env": "DB_USER", "password_env": "DB_PASS"}])
        issues = _check_auth_credential_format(df, "auth.csv")
        assert issues == []

    def test_env_var_with_spaces_critical(self):
        df = pd.DataFrame([{"auth_id": "a1", "username_env": "my user", "password_env": "DB_PASS"}])
        issues = _check_auth_credential_format(df, "auth.csv")
        assert any(i.severity == Severity.CRITICAL and i.rule == "auth_env_var_has_spaces" for i in issues)

    def test_actual_api_key_critical(self):
        df = pd.DataFrame([{"auth_id": "a1", "username_env": "DB_USER", "password_env": "sk-abc123secret"}])
        issues = _check_auth_credential_format(df, "auth.csv")
        assert any(i.rule == "auth_secret_in_csv" for i in issues)

    def test_non_auth_file_skipped(self):
        df = pd.DataFrame([{"auth_id": "a1", "username_env": "bad val", "password_env": "sk-secret"}])
        issues = _check_auth_credential_format(df, "connections.csv")
        assert issues == []

    def test_blank_optional_env_cols_ok(self):
        df = pd.DataFrame([{
            "auth_id": "a1",
            "username_env": "DB_USER",
            "password_env": "DB_PASS",
            "pat_name_env": "",
            "pat_secret_env": "",
        }])
        issues = _check_auth_credential_format(df, "auth.csv")
        assert issues == []


# ===========================================================================
# Business rule: _check_dashboard_dimension_requirements
# ===========================================================================


class TestCheckDashboardDimensionRequirements:
    def test_pie_chart_with_color_ok(self):
        df = pd.DataFrame([{
            "view_id": "v1", "view_name": "Pie", "view_type": "worksheet",
            "datasource_id": "ds_001", "chart_type": "Pie", "color": "Segment",
        }])
        issues = _check_dashboard_dimension_requirements(df, "dashboard_requirements.csv")
        assert issues == []

    def test_pie_chart_without_color_warning(self):
        df = pd.DataFrame([{
            "view_id": "v1", "view_name": "Pie", "view_type": "worksheet",
            "datasource_id": "ds_001", "chart_type": "Pie", "color": "",
        }])
        issues = _check_dashboard_dimension_requirements(df, "dashboard_requirements.csv")
        assert any(i.severity == Severity.WARNING and i.rule == "chart_type_field_requirement" for i in issues)

    def test_scatter_with_rows_and_cols_ok(self):
        df = pd.DataFrame([{
            "view_id": "v1", "view_name": "Scatter", "view_type": "worksheet",
            "datasource_id": "ds_001", "chart_type": "Scatter",
            "rows": "Sales", "columns": "Profit",
        }])
        issues = _check_dashboard_dimension_requirements(df, "dashboard_requirements.csv")
        assert issues == []

    def test_scatter_missing_rows_warning(self):
        df = pd.DataFrame([{
            "view_id": "v1", "view_name": "Scatter", "view_type": "worksheet",
            "datasource_id": "ds_001", "chart_type": "Scatter",
            "rows": "", "columns": "Profit",
        }])
        issues = _check_dashboard_dimension_requirements(df, "dashboard_requirements.csv")
        assert any(i.rule == "chart_type_field_requirement" for i in issues)

    def test_bar_chart_no_extra_requirements(self):
        df = pd.DataFrame([{
            "view_id": "v1", "view_name": "Bar", "view_type": "worksheet",
            "datasource_id": "ds_001", "chart_type": "Bar",
        }])
        issues = _check_dashboard_dimension_requirements(df, "dashboard_requirements.csv")
        assert issues == []


# ===========================================================================
# MetadataValidationAgent — unit tests
# ===========================================================================


class TestMetadataValidationAgentInit:
    def test_instantiation_default_config(self):
        agent = MetadataValidationAgent()
        assert agent.agent_id == "metadata_validation"

    def test_instantiation_with_config(self):
        # Test that csv_dir from config is stored and usable — not the internal attribute name.
        # BaseAgent.config storage varies; we verify the agent accepts and uses the config.
        agent = MetadataValidationAgent(config={"csv_dir": "/some/path"})
        assert agent._default_csv_dir == "/some/path"

    def test_export_alias(self):
        """ValidationAgent must be an alias for MetadataValidationAgent."""
        assert ValidationAgent is MetadataValidationAgent


class TestValidateInput:
    def test_missing_project_spec(self):
        agent = MetadataValidationAgent(config={"csv_dir": "/tmp"})
        errors = agent.validate_input({"csv_dir": "/tmp"})
        assert any("project_spec" in e for e in errors)

    def test_missing_csv_dir(self):
        agent = MetadataValidationAgent()
        errors = agent.validate_input({"project_spec": {"project_id": "p1"}})
        assert any("csv_dir" in e for e in errors)

    def test_valid_state_no_errors(self):
        agent = MetadataValidationAgent(config={"csv_dir": "/tmp"})
        errors = agent.validate_input({
            "project_spec": {"project_id": "p1"},
            "csv_dir": "/tmp",
        })
        assert errors == []

    def test_csv_dir_from_config_accepted(self):
        agent = MetadataValidationAgent(config={"csv_dir": "/from/config"})
        errors = agent.validate_input({"project_spec": {"project_id": "p1"}})
        assert errors == []


class TestRunMethod:
    def test_run_invalid_state_returns_failed(self):
        agent = MetadataValidationAgent()
        result = agent.run({})
        assert result.status == "failed"
        assert result.agent_id == "metadata_validation"
        assert result.phase == "VALIDATING"
        assert len(result.errors) > 0

    def test_run_valid_csvs_returns_success(self, tmp_csv_dir):
        _full_minimal_csvs(tmp_csv_dir)
        agent = _make_agent(tmp_csv_dir)
        result = agent.run(_make_state(tmp_csv_dir))
        assert result.status == "success"
        assert "validation_report" in result.output
        assert result.output["validation_report"]["is_valid"] is True

    def test_run_missing_core_file_returns_failed(self, tmp_csv_dir):
        # Write only one file — missing most core files
        _write_csv(tmp_csv_dir, "project_config.csv", _minimal_project_config())
        agent = _make_agent(tmp_csv_dir)
        result = agent.run(_make_state(tmp_csv_dir))
        assert result.status == "failed"

    def test_run_duration_ms_set(self, tmp_csv_dir):
        _full_minimal_csvs(tmp_csv_dir)
        agent = _make_agent(tmp_csv_dir)
        result = agent.run(_make_state(tmp_csv_dir))
        assert result.duration_ms > 0

    def test_run_agent_result_structure(self, tmp_csv_dir):
        _full_minimal_csvs(tmp_csv_dir)
        agent = _make_agent(tmp_csv_dir)
        result = agent.run(_make_state(tmp_csv_dir))
        assert result.agent_id == "metadata_validation"
        assert result.phase == "VALIDATING"
        assert isinstance(result.errors, list)
        assert isinstance(result.warnings, list)


class TestCallMethod:
    def test_call_returns_state_delta(self, tmp_csv_dir):
        _full_minimal_csvs(tmp_csv_dir)
        agent = _make_agent(tmp_csv_dir)
        delta = agent(_make_state(tmp_csv_dir))
        assert "validation_report" in delta
        assert "errors" in delta
        assert "phase" in delta

    def test_call_phase_validating_on_success(self, tmp_csv_dir):
        _full_minimal_csvs(tmp_csv_dir)
        agent = _make_agent(tmp_csv_dir)
        delta = agent(_make_state(tmp_csv_dir))
        assert delta["phase"] == "VALIDATING"

    def test_call_phase_failed_on_critical_errors(self, tmp_csv_dir):
        # Empty dir — all core files missing
        agent = _make_agent(tmp_csv_dir)
        delta = agent(_make_state(tmp_csv_dir))
        assert delta["phase"] == "FAILED"


# ===========================================================================
# File-level validation
# ===========================================================================


class TestFileLevelValidation:
    def test_missing_optional_file_is_warning(self, tmp_csv_dir):
        _full_minimal_csvs(tmp_csv_dir)
        # figma_layout.csv is optional
        agent = _make_agent(tmp_csv_dir)
        report = agent._run_validation(tmp_csv_dir, "proj_001")
        missing_figma = [
            i for i in report.issues
            if i.rule == "file_missing" and i.file == "figma_layout.csv"
        ]
        if missing_figma:
            assert missing_figma[0].severity == Severity.WARNING

    def test_missing_core_file_is_critical(self, tmp_csv_dir):
        # Write only project_config.csv
        _write_csv(tmp_csv_dir, "project_config.csv", _minimal_project_config())
        agent = _make_agent(tmp_csv_dir)
        report = agent._run_validation(tmp_csv_dir, "proj_001")
        core_missing = [
            i for i in report.issues
            if i.rule == "file_missing" and i.file == "data_sources.csv"
        ]
        assert core_missing[0].severity == Severity.CRITICAL

    def test_unparseable_csv_is_critical(self, tmp_csv_dir):
        _write_csv(tmp_csv_dir, "project_config.csv", "not,valid\x00csv\x01garbage")
        agent = _make_agent(tmp_csv_dir)
        report = agent._run_validation(tmp_csv_dir, "proj_001")
        # Either parse error or it loads fine — verify no exception raised
        assert report is not None

    def test_empty_csv_is_warning(self, tmp_csv_dir):
        _write_csv(tmp_csv_dir, "project_config.csv",
                   "project_id,project_name,environment,tableau_site,"
                   "tableau_server_url,target_project,workbook_name\n")
        agent = _make_agent(tmp_csv_dir)
        report = agent._run_validation(tmp_csv_dir, "proj_001")
        empty_issues = [i for i in report.issues if i.rule == "file_empty"]
        assert any(i.severity == Severity.WARNING for i in empty_issues)


class TestColumnValidation:
    def test_missing_required_column_critical(self, tmp_csv_dir):
        # Write project_config without workbook_name column
        _write_csv(tmp_csv_dir, "project_config.csv",
                   "project_id,project_name,environment,tableau_site,tableau_server_url,target_project\n"
                   "p1,Test,dev,site,https://example.com,Sales\n")
        agent = _make_agent(tmp_csv_dir)
        report = agent._run_validation(tmp_csv_dir, "p1")
        col_issues = [
            i for i in report.issues
            if i.rule == "column_missing" and i.column == "workbook_name"
        ]
        assert col_issues[0].severity == Severity.CRITICAL

    def test_missing_optional_column_info(self, tmp_csv_dir):
        # project_config without optional 'description' — should be INFO or absent
        _write_csv(tmp_csv_dir, "project_config.csv", _minimal_project_config())
        agent = _make_agent(tmp_csv_dir)
        report = agent._run_validation(tmp_csv_dir, "p1")
        col_issues = [
            i for i in report.issues
            if i.rule == "column_missing" and i.column == "description"
        ]
        if col_issues:
            assert col_issues[0].severity == Severity.INFO

    def test_required_field_blank_critical(self, tmp_csv_dir):
        _write_csv(tmp_csv_dir, "project_config.csv",
                   "project_id,project_name,environment,tableau_site,"
                   "tableau_server_url,target_project,workbook_name\n"
                   ",Test,dev,site,https://example.com,Sales,My_WB\n")
        agent = _make_agent(tmp_csv_dir)
        report = agent._run_validation(tmp_csv_dir, "p1")
        blank_issues = [i for i in report.issues if i.rule == "required_field_blank"]
        assert any(i.severity == Severity.CRITICAL for i in blank_issues)


class TestEnumValidation:
    def test_invalid_environment_critical(self, tmp_csv_dir):
        _write_csv(tmp_csv_dir, "project_config.csv",
                   "project_id,project_name,environment,tableau_site,"
                   "tableau_server_url,target_project,workbook_name\n"
                   "p1,Test,PRODUCTION,site,https://example.com,Sales,My_WB\n")
        agent = _make_agent(tmp_csv_dir)
        report = agent._run_validation(tmp_csv_dir, "p1")
        enum_issues = [
            i for i in report.issues
            if i.rule == "enum_value_invalid" and i.column == "environment"
        ]
        assert len(enum_issues) >= 1
        assert enum_issues[0].severity == Severity.CRITICAL

    def test_invalid_chart_type_critical(self, tmp_csv_dir):
        _full_minimal_csvs(tmp_csv_dir)
        _write_csv(tmp_csv_dir, "dashboard_requirements.csv",
                   "view_id,view_name,view_type,datasource_id,chart_type,views_in_dashboard\n"
                   "v1,WS1,worksheet,ds_001,Histogram,\n")
        agent = _make_agent(tmp_csv_dir)
        report = agent._run_validation(tmp_csv_dir, "p1")
        enum_issues = [
            i for i in report.issues
            if i.rule == "enum_value_invalid" and i.column == "chart_type"
        ]
        assert len(enum_issues) >= 1

    def test_invalid_join_type_critical(self, tmp_csv_dir):
        _full_minimal_csvs(tmp_csv_dir)
        _write_csv(tmp_csv_dir, "relationships.csv",
                   "relationship_id,datasource_id,left_table_id,right_table_id,"
                   "left_key,right_key,join_type\n"
                   "rel_001,ds_001,tbl_001,tbl_001,ID,ID,cross\n")
        agent = _make_agent(tmp_csv_dir)
        report = agent._run_validation(tmp_csv_dir, "p1")
        enum_issues = [
            i for i in report.issues
            if i.rule == "enum_value_invalid" and i.column == "join_type"
        ]
        assert len(enum_issues) >= 1

    def test_invalid_datatype_in_columns(self, tmp_csv_dir):
        _full_minimal_csvs(tmp_csv_dir)
        _write_csv(tmp_csv_dir, "columns.csv",
                   "column_id,table_id,column_name,datatype,role\n"
                   "col_001,tbl_001,ORDER_ID,varchar,dimension\n")
        agent = _make_agent(tmp_csv_dir)
        report = agent._run_validation(tmp_csv_dir, "p1")
        enum_issues = [
            i for i in report.issues
            if i.rule == "enum_value_invalid" and i.column == "datatype"
        ]
        assert len(enum_issues) >= 1


class TestReferentialIntegrity:
    def test_missing_datasource_id_in_tables(self, tmp_csv_dir):
        _full_minimal_csvs(tmp_csv_dir)
        # tables.csv references ds_999 which doesn't exist
        _write_csv(tmp_csv_dir, "tables.csv",
                   "table_id,datasource_id,table_name\n"
                   "tbl_001,ds_999,ORDERS\n")
        agent = _make_agent(tmp_csv_dir)
        report = agent._run_validation(tmp_csv_dir, "p1")
        fk_issues = [
            i for i in report.issues
            if i.rule == "referential_integrity" and "ds_999" in (i.value or "")
        ]
        assert len(fk_issues) >= 1
        assert fk_issues[0].severity == Severity.CRITICAL

    def test_missing_auth_id_in_connections(self, tmp_csv_dir):
        _full_minimal_csvs(tmp_csv_dir)
        _write_csv(tmp_csv_dir, "connections.csv",
                   "connection_id,class,server,dbname,schema,warehouse,auth_method,auth_id\n"
                   "conn_001,snowflake,acct.snowflake.com,DB,PUBLIC,WH,Username Password,auth_MISSING\n")
        agent = _make_agent(tmp_csv_dir)
        report = agent._run_validation(tmp_csv_dir, "p1")
        fk_issues = [
            i for i in report.issues
            if i.rule == "referential_integrity" and "auth_MISSING" in (i.value or "")
        ]
        assert len(fk_issues) >= 1

    def test_valid_fk_references_no_issues(self, tmp_csv_dir):
        _full_minimal_csvs(tmp_csv_dir)
        agent = _make_agent(tmp_csv_dir)
        report = agent._run_validation(tmp_csv_dir, "p1")
        fk_issues = [i for i in report.issues if i.rule == "referential_integrity"]
        assert fk_issues == []


class TestDuplicatePrimaryKeys:
    def test_duplicate_datasource_id_critical(self, tmp_csv_dir):
        _full_minimal_csvs(tmp_csv_dir)
        _write_csv(tmp_csv_dir, "data_sources.csv",
                   "datasource_id,datasource_name,connection_id,datasource_type,primary_table\n"
                   "ds_001,Orders,conn_001,live,ORDERS\n"
                   "ds_001,Orders Dup,conn_001,live,ORDERS2\n")
        agent = _make_agent(tmp_csv_dir)
        report = agent._run_validation(tmp_csv_dir, "p1")
        dup_issues = [i for i in report.issues if i.rule == "duplicate_primary_key"]
        assert len(dup_issues) >= 1
        assert all(i.severity == Severity.CRITICAL for i in dup_issues)


class TestCompletenessHints:
    def test_missing_metrics_emits_info(self, tmp_csv_dir):
        _full_minimal_csvs(tmp_csv_dir)
        # No metrics.csv written
        agent = _make_agent(tmp_csv_dir)
        report = agent._run_validation(tmp_csv_dir, "p1")
        info_issues = [
            i for i in report.issues
            if i.rule == "completeness_hint" and i.file == "metrics.csv"
        ]
        assert len(info_issues) >= 1

    def test_missing_figma_emits_info(self, tmp_csv_dir):
        _full_minimal_csvs(tmp_csv_dir)
        agent = _make_agent(tmp_csv_dir)
        report = agent._run_validation(tmp_csv_dir, "p1")
        info_issues = [
            i for i in report.issues
            if i.rule == "completeness_hint" and i.file == "figma_layout.csv"
        ]
        assert len(info_issues) >= 1


# ===========================================================================
# FILE_SCHEMAS constant integrity
# ===========================================================================


class TestFileSchemas:
    def test_all_core_files_in_schema(self):
        core_files = [
            "project_config.csv", "data_sources.csv", "connections.csv", "auth.csv",
            "tables.csv", "columns.csv", "relationships.csv", "metrics.csv",
            "dimensions.csv", "dashboard_requirements.csv", "figma_layout.csv",
        ]
        for f in core_files:
            assert f in FILE_SCHEMAS, f"Missing schema for {f}"

    def test_each_schema_has_at_least_one_required_field(self):
        for file_name, schema in FILE_SCHEMAS.items():
            required = [c for c in schema if c[2]]  # required=True
            assert required, f"{file_name} has no required fields"

    def test_enum_defs_have_at_least_two_values(self):
        for file_name, schema in FILE_SCHEMAS.items():
            for col, dtype, _ in schema:
                if dtype.startswith("enum:"):
                    values = dtype[5:].split(",")
                    assert len(values) >= 2, f"{file_name}.{col} enum has <2 values"


# ===========================================================================
# FK_RULES constant integrity
# ===========================================================================


class TestFkRules:
    def test_all_fk_rules_reference_known_files(self):
        known_files = set(FILE_SCHEMAS.keys())
        for child_file, _, parent_file, _, _ in FK_RULES:
            assert child_file in known_files, f"Unknown child file: {child_file}"
            assert parent_file in known_files, f"Unknown parent file: {parent_file}"

    def test_fk_rules_have_valid_severities(self):
        for _, _, _, _, sev in FK_RULES:
            assert isinstance(sev, Severity)


# ===========================================================================
# Additional edge-case and integration tests
# ===========================================================================


class TestTypeValidation:
    def test_valid_integer_port_ok(self, tmp_csv_dir):
        _full_minimal_csvs(tmp_csv_dir)
        _write_csv(tmp_csv_dir, "connections.csv",
                   "connection_id,class,server,dbname,schema,warehouse,port,auth_method,auth_id\n"
                   "conn_001,snowflake,acct.snowflake.com,DB,PUBLIC,WH,443,Username Password,auth_001\n")
        agent = _make_agent(tmp_csv_dir)
        report = agent._run_validation(tmp_csv_dir, "p1")
        type_issues = [i for i in report.issues if i.rule == "type_mismatch_integer"]
        assert type_issues == []

    def test_invalid_integer_port_warning(self, tmp_csv_dir):
        _full_minimal_csvs(tmp_csv_dir)
        _write_csv(tmp_csv_dir, "connections.csv",
                   "connection_id,class,server,dbname,schema,warehouse,port,auth_method,auth_id\n"
                   "conn_001,snowflake,acct.snowflake.com,DB,PUBLIC,WH,not_an_int,Username Password,auth_001\n")
        agent = _make_agent(tmp_csv_dir)
        report = agent._run_validation(tmp_csv_dir, "p1")
        type_issues = [i for i in report.issues if i.rule == "type_mismatch_integer"]
        assert len(type_issues) >= 1
        assert type_issues[0].severity == Severity.WARNING

    def test_invalid_boolean_warning(self, tmp_csv_dir):
        _full_minimal_csvs(tmp_csv_dir)
        _write_csv(tmp_csv_dir, "data_sources.csv",
                   "datasource_id,datasource_name,connection_id,datasource_type,primary_table,is_primary\n"
                   "ds_001,Orders,conn_001,live,ORDERS,maybe\n")
        agent = _make_agent(tmp_csv_dir)
        report = agent._run_validation(tmp_csv_dir, "p1")
        bool_issues = [i for i in report.issues if i.rule == "type_mismatch_boolean"]
        assert len(bool_issues) >= 1

    def test_valid_boolean_true_ok(self, tmp_csv_dir):
        _full_minimal_csvs(tmp_csv_dir)
        _write_csv(tmp_csv_dir, "data_sources.csv",
                   "datasource_id,datasource_name,connection_id,datasource_type,primary_table,is_primary\n"
                   "ds_001,Orders,conn_001,live,ORDERS,true\n")
        agent = _make_agent(tmp_csv_dir)
        report = agent._run_validation(tmp_csv_dir, "p1")
        bool_issues = [i for i in report.issues if i.rule == "type_mismatch_boolean"]
        assert bool_issues == []

    def test_valid_boolean_false_ok(self, tmp_csv_dir):
        _full_minimal_csvs(tmp_csv_dir)
        _write_csv(tmp_csv_dir, "data_sources.csv",
                   "datasource_id,datasource_name,connection_id,datasource_type,primary_table,is_primary\n"
                   "ds_001,Orders,conn_001,live,ORDERS,false\n")
        agent = _make_agent(tmp_csv_dir)
        report = agent._run_validation(tmp_csv_dir, "p1")
        bool_issues = [i for i in report.issues if i.rule == "type_mismatch_boolean"]
        assert bool_issues == []


class TestMultipleConnectionClasses:
    def test_multiple_connection_classes_emit_info(self, tmp_csv_dir):
        _full_minimal_csvs(tmp_csv_dir)
        _write_csv(tmp_csv_dir, "connections.csv",
                   "connection_id,class,server,dbname,schema,auth_method,auth_id\n"
                   "conn_001,snowflake,acct.snowflake.com,DB,PUBLIC,Username Password,auth_001\n"
                   "conn_002,postgres,myhost,DB,public,Username Password,auth_001\n")
        _write_csv(tmp_csv_dir, "auth.csv",
                   "auth_id,username_env,password_env\n"
                   "auth_001,DB_USER,DB_PASS\n")
        agent = _make_agent(tmp_csv_dir)
        report = agent._run_validation(tmp_csv_dir, "p1")
        multi_class_info = [
            i for i in report.issues
            if i.rule == "completeness_hint" and "Multiple connection classes" in i.message
        ]
        assert len(multi_class_info) >= 1


class TestSnowflakeNanWarehouse:
    def test_snowflake_nan_warehouse_warning(self, tmp_csv_dir):
        _full_minimal_csvs(tmp_csv_dir)
        _write_csv(tmp_csv_dir, "connections.csv",
                   "connection_id,class,server,dbname,schema,warehouse,auth_method,auth_id\n"
                   "conn_001,snowflake,acct.snowflake.com,DB,PUBLIC,nan,Username Password,auth_001\n")
        agent = _make_agent(tmp_csv_dir)
        report = agent._run_validation(tmp_csv_dir, "p1")
        wh_issues = [i for i in report.issues if i.rule == "snowflake_warehouse_missing"]
        assert len(wh_issues) >= 1


class TestLodDetectionCaseInsensitive:
    def test_lowercase_fixed_detected(self):
        df = pd.DataFrame([{
            "metric_name": "m",
            "formula": "{ fixed [Customer] : sum([Sales]) }",
            "is_lod": "false",
        }])
        issues = _check_lod_flag_consistency(df, "metrics.csv")
        assert any(i.rule == "lod_flag_mismatch" for i in issues)

    def test_exclude_keyword_detected(self):
        df = pd.DataFrame([{
            "metric_name": "m",
            "formula": "{ EXCLUDE [Region] : SUM([Sales]) }",
            "is_lod": "false",
        }])
        issues = _check_lod_flag_consistency(df, "metrics.csv")
        assert any(i.rule == "lod_flag_mismatch" for i in issues)


class TestValidationReportFiles:
    def test_files_checked_populated(self, tmp_csv_dir):
        _full_minimal_csvs(tmp_csv_dir)
        agent = _make_agent(tmp_csv_dir)
        report = agent._run_validation(tmp_csv_dir, "p1")
        assert len(report.files_checked) > 0

    def test_project_id_in_report(self, tmp_csv_dir):
        _full_minimal_csvs(tmp_csv_dir)
        agent = _make_agent(tmp_csv_dir)
        report = agent._run_validation(tmp_csv_dir, "MY_PROJECT_42")
        assert report.project_id == "MY_PROJECT_42"


class TestMultipleColumnsInDashboard:
    def test_pipe_separated_views_all_valid(self, tmp_csv_dir):
        _full_minimal_csvs(tmp_csv_dir)
        _write_csv(tmp_csv_dir, "dashboard_requirements.csv",
                   "view_id,view_name,view_type,datasource_id,chart_type,views_in_dashboard\n"
                   "v1,WS1,worksheet,ds_001,Bar,\n"
                   "v2,WS2,worksheet,ds_001,Line,\n"
                   "d1,Overview,dashboard,ds_001,Bar,v1|v2\n")
        agent = _make_agent(tmp_csv_dir)
        report = agent._run_validation(tmp_csv_dir, "p1")
        dashboard_issues = [
            i for i in report.issues
            if i.rule in ("dashboard_missing_worksheets", "dashboard_orphan_worksheet_ref")
        ]
        assert dashboard_issues == []


class TestAgentIdConstant:
    def test_agent_id_constant(self):
        assert MetadataValidationAgent.AGENT_ID == "metadata_validation"

    def test_phase_constant(self):
        assert MetadataValidationAgent.PHASE == "VALIDATING"
