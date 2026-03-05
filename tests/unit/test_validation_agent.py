# tests/unit/test_validation_agent.py
"""
Unit Tests — ValidationAgent and ValidationReport
==================================================

Tests use the real ProjectSpec (from csv_inputs/) as a base fixture,
then mutate copies to exercise each validation rule category.

Run from project root:
    pytest tests/unit/test_validation_agent.py -v
"""

import copy
import sys
from pathlib import Path
from typing import Any, Dict

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from agents.base_agent import AgentStatus, PhaseContext
from agents.intake_agent import IntakeAgent
from agents.validation_agent import ValidationAgent
from models.project_spec import ProjectSpec
from models.validation_report import (
    FindingCategory,
    FindingSeverity,
    ValidationFinding,
    ValidationReport,
)

# ── Paths ──────────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).parent.parent.parent
REAL_CSV_DIR = PROJECT_ROOT / "csv_inputs"


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def real_spec_dict() -> Dict[str, Any]:
    """Load the real ProjectSpec once per module — this is expensive."""
    spec = ProjectSpec.from_csv_dir(REAL_CSV_DIR)
    return spec.model_dump()


@pytest.fixture
def spec_dict(real_spec_dict) -> Dict[str, Any]:
    """Deep copy so each test can mutate freely."""
    return copy.deepcopy(real_spec_dict)


@pytest.fixture
def sample_context() -> PhaseContext:
    return PhaseContext(project_id="proj_001", run_id="run_test")


@pytest.fixture
def agent(sample_context) -> ValidationAgent:
    return ValidationAgent(context=sample_context)


@pytest.fixture
def state(spec_dict) -> Dict[str, Any]:
    return {"project_spec": spec_dict}


# ══════════════════════════════════════════════════════════════════════════════
# ValidationReport model
# ══════════════════════════════════════════════════════════════════════════════

class TestValidationReport:
    def test_empty_report_can_proceed(self):
        r = ValidationReport(project_id="p1")
        assert r.can_proceed is True
        assert r.blocking_count == 0

    def test_critical_finding_blocks_proceed(self):
        r = ValidationReport(project_id="p1")
        r.add_critical("source.csv", "fatal error", rule="R1")
        assert r.can_proceed is False
        assert r.critical_count == 1

    def test_high_finding_blocks_proceed(self):
        r = ValidationReport(project_id="p1")
        r.add_finding("source.csv", "high error",
                      severity=FindingSeverity.HIGH, rule="R1")
        assert r.can_proceed is False

    def test_warning_does_not_block(self):
        r = ValidationReport(project_id="p1")
        r.add_warning("source.csv", "just a warning")
        assert r.can_proceed is True

    def test_info_does_not_block(self):
        r = ValidationReport(project_id="p1")
        r.add_info("source.csv", "fyi")
        assert r.can_proceed is True

    def test_fluent_chaining(self):
        r = (ValidationReport(project_id="p1")
             .add_critical("f.csv", "err1", rule="R1")
             .add_warning("f.csv", "warn1")
             .add_info("f.csv", "info1"))
        assert r.critical_count == 1
        assert r.warning_count  == 1
        assert r.info_count     == 1

    def test_counts_are_correct(self):
        r = ValidationReport(project_id="p1")
        r.add_critical("a.csv", "c1")
        r.add_critical("a.csv", "c2")
        r.add_finding("b.csv", "h1", severity=FindingSeverity.HIGH)
        r.add_warning("c.csv", "w1")
        assert r.critical_count  == 2
        assert r.high_count      == 1
        assert r.warning_count   == 1
        assert r.blocking_count  == 3

    def test_summary_has_all_keys(self):
        r = ValidationReport(project_id="p1")
        s = r.summary()
        for key in ["project_id", "can_proceed", "critical",
                    "high", "warnings", "info", "total_findings"]:
            assert key in s, f"Missing key: {key}"

    def test_to_dict_includes_findings(self):
        r = ValidationReport(project_id="p1")
        r.add_critical("a.csv", "test error")
        d = r.to_dict()
        assert len(d["findings"]) == 1
        assert d["findings"][0]["severity"] == "CRITICAL"

    def test_findings_by_severity(self):
        r = ValidationReport(project_id="p1")
        r.add_critical("a.csv", "c")
        r.add_warning("b.csv", "w")
        criticals = r.findings_by_severity(FindingSeverity.CRITICAL)
        assert len(criticals) == 1
        assert criticals[0].message == "c"

    def test_findings_by_source(self):
        r = ValidationReport(project_id="p1")
        r.add_critical("metrics.csv", "err")
        r.add_warning("columns.csv", "warn")
        assert len(r.findings_by_source("metrics.csv")) == 1

    def test_findings_by_category(self):
        r = ValidationReport(project_id="p1")
        r.add_finding("a.csv", "biz rule", category=FindingCategory.BUSINESS)
        r.add_finding("b.csv", "schema",   category=FindingCategory.SCHEMA)
        biz = r.findings_by_category(FindingCategory.BUSINESS)
        assert len(biz) == 1

    def test_repr_contains_can_proceed(self):
        r = ValidationReport(project_id="p1")
        assert "can_proceed" in repr(r)

    def test_rules_run_tracking(self):
        r = ValidationReport(project_id="p1")
        r.rules_run = ["CAT1", "CAT2", "CAT3"]
        s = r.summary()
        assert s["rules_run"] == 3


# ══════════════════════════════════════════════════════════════════════════════
# ValidationAgent — validate_input
# ══════════════════════════════════════════════════════════════════════════════

class TestValidationAgentValidateInput:
    def test_missing_project_spec_fails(self, agent):
        errors = agent.validate_input({})
        assert len(errors) >= 1
        assert any("project_spec" in e.lower() for e in errors)

    def test_project_spec_present_passes(self, agent, state):
        errors = agent.validate_input(state)
        assert errors == []


# ══════════════════════════════════════════════════════════════════════════════
# ValidationAgent — success path with real CSVs
# ══════════════════════════════════════════════════════════════════════════════

class TestValidationAgentSuccess:
    def test_execute_on_real_spec_does_not_fail(self, agent, state):
        result = agent.execute(state)
        assert result.status in (AgentStatus.SUCCESS, AgentStatus.WARNING), \
            f"Expected SUCCESS or WARNING, got {result.status}. " \
            f"Errors: {[e.message for e in result.errors]}"

    def test_validation_report_in_output(self, agent, state):
        result = agent.execute(state)
        assert "validation_report" in result.output

    def test_validation_report_can_proceed(self, agent, state):
        result = agent.execute(state)
        report = result.output["validation_report"]
        assert report["can_proceed"] is True, \
            f"Real CSVs should pass validation. Blocking findings: " \
            f"{[f for f in report['findings'] if f['severity'] in ('CRITICAL','HIGH')]}"

    def test_all_6_rule_categories_run(self, agent, state):
        result = agent.execute(state)
        rules = result.output["validation_report"]["rules_run"]
        for cat in ["CAT1", "CAT2", "CAT3", "CAT4", "CAT5", "CAT6"]:
            assert cat in rules, f"Rule category {cat} did not run"

    def test_security_reminder_info_finding_present(self, agent, state):
        result = agent.execute(state)
        findings = result.output["validation_report"]["findings"]
        info_rules = [f["rule"] for f in findings if f["severity"] == "INFO"]
        assert any("rotation" in r for r in info_rules)

    def test_timing_recorded(self, agent, state):
        result = agent.execute(state)
        assert result.duration_ms > 0

    def test_metadata_contains_summary(self, agent, state):
        result = agent.execute(state)
        assert "validation_summary" in result.metadata


# ══════════════════════════════════════════════════════════════════════════════
# CAT-1: Schema validation
# ══════════════════════════════════════════════════════════════════════════════

class TestCat1SchemaValidation:
    def test_invalid_environment_is_critical(self, agent, spec_dict):
        spec_dict["project_config"]["environment"] = "sandbox"
        result = agent.execute({"project_spec": spec_dict})
        report = result.output["validation_report"]
        criticals = [f for f in report["findings"] if f["severity"] == "CRITICAL"]
        assert len(criticals) >= 1, "Expected at least one CRITICAL for invalid environment"

    def test_http_url_is_high(self, agent, spec_dict):
        spec_dict["project_config"]["tableau_server_url"] = "http://not-secure.example.com"
        result = agent.execute({"project_spec": spec_dict})
        report = result.output["validation_report"]
        url_findings = [
            f for f in report["findings"]
            if "url" in (f.get("rule") or "").lower()
            or "url" in (f.get("field") or "").lower()
            or "http" in (f.get("message") or "").lower()
            or "https" in (f.get("message") or "").lower()
        ]
        assert len(url_findings) >= 1, \
            f"Expected URL finding. All findings: {[f['message'] for f in report['findings']]}"

    def test_invalid_column_datatype_is_critical(self, agent, spec_dict):
        spec_dict["columns"][0]["datatype"] = "BIGINT"
        result = agent.execute({"project_spec": spec_dict})
        report = result.output["validation_report"]
        criticals = [f for f in report["findings"] if f["severity"] == "CRITICAL"]
        assert len(criticals) >= 1, "Expected CRITICAL for invalid column datatype"
        assert any(
            "BIGINT" in f["message"] or "datatype" in f.get("rule", "")
            for f in criticals
        )

    def test_invalid_column_role_is_critical(self, agent, spec_dict):
        spec_dict["columns"][0]["role"] = "attribute"
        result = agent.execute({"project_spec": spec_dict})
        report = result.output["validation_report"]
        criticals = [f for f in report["findings"] if f["severity"] == "CRITICAL"]
        assert len(criticals) >= 1, "Expected CRITICAL for invalid column role"

    def test_invalid_chart_type_is_high(self, agent, spec_dict):
        worksheets = [r for r in spec_dict["dashboard_requirements"]
                      if r["view_type"] == "worksheet"]
        worksheets[0]["chart_type"] = "Donut"
        result = agent.execute({"project_spec": spec_dict})
        report = result.output["validation_report"]
        assert any("chart_type" in f["rule"] for f in report["findings"])

    def test_invalid_sort_direction_is_warning(self, agent, spec_dict):
        spec_dict["dashboard_requirements"][0]["sort_direction"] = "random"
        result = agent.execute({"project_spec": spec_dict})
        report = result.output["validation_report"]
        assert any("sort_direction" in f["rule"] for f in report["findings"])

    def test_valid_spec_no_cat1_errors(self, agent, state):
        result = agent.execute(state)
        report = result.output["validation_report"]
        cat1_criticals = [
            f for f in report["findings"]
            if f["rule"].startswith("CAT1") and f["severity"] == "CRITICAL"
        ]
        assert cat1_criticals == [], f"Unexpected CAT1 criticals: {cat1_criticals}"


# ══════════════════════════════════════════════════════════════════════════════
# CAT-2: Completeness checks
# ══════════════════════════════════════════════════════════════════════════════

class TestCat2CompletenessChecks:
    def test_worksheet_with_no_shelves_is_high(self, agent, spec_dict):
        worksheet = next(
            r for r in spec_dict["dashboard_requirements"]
            if r["view_type"] == "worksheet"
        )
        worksheet["rows"] = None
        worksheet["columns"] = None
        result = agent.execute({"project_spec": spec_dict})
        report = result.output["validation_report"]
        assert any("worksheet_shelves" in f["rule"] for f in report["findings"])

    def test_dashboard_with_no_views_is_high(self, agent, spec_dict):
        dashboard = next(
            r for r in spec_dict["dashboard_requirements"]
            if r["view_type"] == "dashboard"
        )
        dashboard["views_in_dashboard"] = None
        result = agent.execute({"project_spec": spec_dict})
        report = result.output["validation_report"]
        assert any("dashboard_views" in f["rule"] for f in report["findings"])

    def test_pie_without_color_is_warning(self, agent, spec_dict):
        worksheet = next(
            r for r in spec_dict["dashboard_requirements"]
            if r["view_type"] == "worksheet"
        )
        worksheet["chart_type"] = "Pie"
        worksheet["color"]      = None
        result = agent.execute({"project_spec": spec_dict})
        report = result.output["validation_report"]
        assert any("pie_color" in f["rule"] for f in report["findings"])

    def test_snowflake_without_warehouse_is_high(self, agent, spec_dict):
        for conn in spec_dict["connections"]:
            if conn["class_"] == "snowflake":
                conn["warehouse"] = None
        result = agent.execute({"project_spec": spec_dict})
        report = result.output["validation_report"]
        assert any("snowflake_warehouse" in f["rule"] for f in report["findings"])

    def test_published_ds_without_name_is_critical(self, agent, spec_dict):
        spec_dict["data_sources"][0]["datasource_type"]   = "published"
        spec_dict["data_sources"][0]["published_ds_name"] = None
        result = agent.execute({"project_spec": spec_dict})
        report = result.output["validation_report"]
        assert any(
            "published_ds_name" in f["rule"] and f["severity"] == "CRITICAL"
            for f in report["findings"]
        )

    def test_auth_without_username_env_is_critical(self, agent, spec_dict):
        spec_dict["auth_configs"][0]["username_env"] = None
        result = agent.execute({"project_spec": spec_dict})
        report = result.output["validation_report"]
        criticals = [f for f in report["findings"] if f["severity"] == "CRITICAL"]
        assert len(criticals) >= 1, "Expected CRITICAL for missing username_env"


# ══════════════════════════════════════════════════════════════════════════════
# CAT-3: Business rules
# ══════════════════════════════════════════════════════════════════════════════

class TestCat3BusinessRules:
    def test_unbalanced_brackets_is_critical(self, agent, spec_dict):
        spec_dict["metrics"][0]["formula"] = "SUM([Sales)"
        result = agent.execute({"project_spec": spec_dict})
        report = result.output["validation_report"]
        assert any("formula_brackets" in f["rule"] for f in report["findings"])

    def test_unbalanced_parens_is_critical(self, agent, spec_dict):
        spec_dict["metrics"][0]["formula"] = "SUM([Sales]"
        result = agent.execute({"project_spec": spec_dict})
        report = result.output["validation_report"]
        assert any("formula_parens" in f["rule"] for f in report["findings"])

    def test_lod_missing_colon_is_critical(self, agent, spec_dict):
        spec_dict["metrics"][0]["formula"] = "{ FIXED [Customer Name] SUM([Sales]) }"
        result = agent.execute({"project_spec": spec_dict})
        report = result.output["validation_report"]
        assert any("lod_colon" in f["rule"] for f in report["findings"])

    def test_lod_unbalanced_braces_is_critical(self, agent, spec_dict):
        spec_dict["metrics"][0]["formula"] = "{ FIXED [Customer Name] : SUM([Sales])"
        result = agent.execute({"project_spec": spec_dict})
        report = result.output["validation_report"]
        assert any("lod_braces" in f["rule"] for f in report["findings"])

    def test_division_without_nullif_is_info(self, agent, spec_dict):
        spec_dict["metrics"][0]["formula"] = "SUM([Profit]) / SUM([Sales])"
        result = agent.execute({"project_spec": spec_dict})
        report = result.output["validation_report"]
        assert any("division_by_zero" in f["rule"] for f in report["findings"])

    def test_division_with_zn_no_warning(self, agent, spec_dict):
        metric_id = spec_dict["metrics"][0]["metric_id"]
        spec_dict["metrics"][0]["formula"] = "SUM([Profit]) / ZN(SUM([Sales]))"
        result = agent.execute({"project_spec": spec_dict})
        report = result.output["validation_report"]
        # Check this *specific* metric has no division_by_zero finding
        findings_for_metric = [
            f for f in report["findings"]
            if "division_by_zero" in f["rule"] and metric_id in f.get("message", "")
        ]
        assert findings_for_metric == [], \
            f"Unexpected division_by_zero for {metric_id}: {findings_for_metric}"

    def test_lod_marked_false_but_has_lod_syntax_is_warning(self, agent, spec_dict):
        spec_dict["metrics"][0]["is_lod"] = False
        spec_dict["metrics"][0]["formula"] = "{ FIXED [Region] : SUM([Sales]) }"
        result = agent.execute({"project_spec": spec_dict})
        report = result.output["validation_report"]
        assert any("lod_marker_consistency" in f["rule"] for f in report["findings"])

    def test_workbook_name_with_spaces_is_warning(self, agent, spec_dict):
        spec_dict["project_config"]["workbook_name"] = "My Sales Dashboard"
        result = agent.execute({"project_spec": spec_dict})
        report = result.output["validation_report"]
        assert any("workbook_name_spaces" in f["rule"] for f in report["findings"])

    def test_dimension_with_aggregation_is_warning(self, agent, spec_dict):
        dim_col = next(c for c in spec_dict["columns"] if c["role"] == "dimension")
        dim_col["aggregation"] = "Sum"
        result = agent.execute({"project_spec": spec_dict})
        report = result.output["validation_report"]
        assert any("dimension_aggregation" in f["rule"] for f in report["findings"])

    def test_valid_lod_formula_no_critical(self, agent, spec_dict):
        spec_dict["metrics"][0]["formula"] = "{ FIXED [Customer Name] : SUM([Sales]) }"
        spec_dict["metrics"][0]["is_lod"]  = True
        result = agent.execute({"project_spec": spec_dict})
        report = result.output["validation_report"]
        lod_criticals = [f for f in report["findings"]
                         if f["rule"].startswith("CAT3") and f["severity"] == "CRITICAL"]
        assert lod_criticals == []


# ══════════════════════════════════════════════════════════════════════════════
# CAT-4: Cross-file consistency
# ══════════════════════════════════════════════════════════════════════════════

class TestCat4CrossFileConsistency:
    def test_invalid_join_key_in_relationship_is_high(self, agent, spec_dict):
        if spec_dict["relationships"]:
            spec_dict["relationships"][0]["left_key"] = "NONEXISTENT_COLUMN_XYZ"
            result = agent.execute({"project_spec": spec_dict})
            report = result.output["validation_report"]
            assert any("join_key_exists" in f["rule"] for f in report["findings"])

    def test_invalid_dimension_column_id_is_high(self, agent, spec_dict):
        if spec_dict["dimensions"]:
            spec_dict["dimensions"][0]["columns"] = "col_FAKE_999|col_FAKE_888"
            result = agent.execute({"project_spec": spec_dict})
            report = result.output["validation_report"]
            assert any("dimension_column_ids" in f["rule"] for f in report["findings"])

    def test_unknown_dashboard_layout_is_warning(self, agent, spec_dict):
        for r in spec_dict["dashboard_requirements"]:
            if r["view_type"] == "dashboard":
                r["dashboard_layout"] = "custom-weird-layout"
        result = agent.execute({"project_spec": spec_dict})
        report = result.output["validation_report"]
        assert any("dashboard_layout" in f["rule"] for f in report["findings"])


# ══════════════════════════════════════════════════════════════════════════════
# CAT-5: Performance warnings
# ══════════════════════════════════════════════════════════════════════════════

class TestCat5PerformanceWarnings:
    def test_many_worksheets_in_dashboard_is_warning(self, agent, spec_dict):
        for r in spec_dict["dashboard_requirements"]:
            if r["view_type"] == "dashboard":
                # Fake 20 worksheets in views_in_dashboard
                r["views_in_dashboard"] = "|".join(f"v{i:03d}" for i in range(20))
        result = agent.execute({"project_spec": spec_dict})
        report = result.output["validation_report"]
        assert any("worksheets_per_dashboard" in f["rule"] for f in report["findings"])

    def test_nested_lod_is_warning(self, agent, spec_dict):
        spec_dict["metrics"][0]["formula"] = (
            "{ FIXED [Region] : SUM({ FIXED [Customer Name] : SUM([Sales]) }) }"
        )
        spec_dict["metrics"][0]["is_lod"] = True
        result = agent.execute({"project_spec": spec_dict})
        report = result.output["validation_report"]
        assert any("nested_lod" in f["rule"] for f in report["findings"])


# ══════════════════════════════════════════════════════════════════════════════
# CAT-6: Security checks
# ══════════════════════════════════════════════════════════════════════════════

class TestCat6SecurityChecks:
    def test_unset_env_var_produces_warning(self, agent, spec_dict, monkeypatch):
        # Force a specific env var name and make sure it's NOT set
        spec_dict["auth_configs"][0]["username_env"] = "TABLEAU_TEST_UNSET_VAR_XYZ"
        monkeypatch.delenv("TABLEAU_TEST_UNSET_VAR_XYZ", raising=False)

        result = agent.execute({"project_spec": spec_dict})
        report = result.output["validation_report"]
        assert any("env_var_not_set" in f["rule"] for f in report["findings"])

    def test_set_env_var_no_warning(self, agent, spec_dict, monkeypatch):
        spec_dict["auth_configs"][0]["username_env"] = "TABLEAU_TEST_SET_VAR_XYZ"
        monkeypatch.setenv("TABLEAU_TEST_SET_VAR_XYZ", "test_user")

        result = agent.execute({"project_spec": spec_dict})
        report = result.output["validation_report"]
        unset_for_this_var = [
            f for f in report["findings"]
            if "env_var_not_set" in f["rule"]
            and "TABLEAU_TEST_SET_VAR_XYZ" in f["message"]
        ]
        assert unset_for_this_var == []

    def test_security_reminder_always_present(self, agent, state):
        result = agent.execute(state)
        report = result.output["validation_report"]
        assert any(
            "credential_rotation_reminder" in f["rule"]
            for f in report["findings"]
        )


# ══════════════════════════════════════════════════════════════════════════════
# End-to-end: IntakeAgent → ValidationAgent pipeline
# ══════════════════════════════════════════════════════════════════════════════

class TestIntakeToValidationPipeline:
    def test_full_pipeline_runs_without_failure(self, sample_context):
        """Run the full Phase 01 → Phase 02 pipeline with real CSVs."""
        # Phase 01: Intake
        intake_agent  = IntakeAgent(csv_dir=REAL_CSV_DIR, context=sample_context)
        intake_result = intake_agent.execute({})
        assert intake_result.status in (AgentStatus.SUCCESS, AgentStatus.WARNING)

        # Phase 02: Validation
        val_state  = {"project_spec": intake_result.output["project_spec"]}
        val_agent  = ValidationAgent(context=sample_context)
        val_result = val_agent.execute(val_state)

        assert val_result.status in (AgentStatus.SUCCESS, AgentStatus.WARNING)
        assert val_result.output["validation_report"]["can_proceed"] is True

    def test_pipeline_produces_correct_phase_labels(self, sample_context):
        intake_agent  = IntakeAgent(csv_dir=REAL_CSV_DIR, context=sample_context)
        intake_result = intake_agent.execute({})
        assert intake_result.phase == "INTAKE"

        val_agent  = ValidationAgent(context=sample_context)
        val_result = val_agent.execute(
            {"project_spec": intake_result.output["project_spec"]}
        )
        assert val_result.phase == "VALIDATING"
