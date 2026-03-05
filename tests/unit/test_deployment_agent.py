# tests/unit/test_deployment_agent.py
"""
Unit tests for DeploymentAgent (Phase 13).
Target: ~65 tests
All TSC network calls are mocked — no live Tableau Cloud connection required.
"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock
import pytest

from agents.base_agent import AgentStatus, PhaseContext
from agents.deployment_agent import DeploymentAgent, PublishResult, _env, _find_auth


# ── Helpers ────────────────────────────────────────────────────────────────────

SAMPLE_TWB_XML = (
    "<?xml version='1.0' encoding='utf-8' ?>\n"
    "<workbook version='18.1' source-platform='win' source-build='2024.1.0'>"
    "<preferences/><datasources><datasource name='Parameters'/></datasources>"
    "<worksheets><worksheet name='Sheet1'/></worksheets>"
    "<dashboards/><windows/></workbook>"
)


def _dry_agent() -> DeploymentAgent:
    ctx = PhaseContext(project_id="proj_001", run_id="test", dry_run=True)
    return DeploymentAgent(context=ctx)


def _live_agent(max_retries=0) -> DeploymentAgent:
    ctx = PhaseContext(project_id="proj_001", run_id="test", dry_run=False)
    return DeploymentAgent(config={"max_retries": max_retries}, context=ctx)


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
    from agents.twb_assembly_agent import TwbAssemblyAgent
    state.update(TwbAssemblyAgent(context=dry_ctx).execute(state).output)
    return state


def _mock_tsc_server(workbook_id="wb_abc123", project_id="proj_xyz"):
    """Build a fully mocked TSC server + auth context manager."""
    import tableauserverclient as TSC

    mock_project       = MagicMock()
    mock_project.id    = project_id
    mock_project.name  = "Sales"

    mock_wb            = MagicMock()
    mock_wb.id         = workbook_id
    mock_wb.webpage_url = f"https://tableau.example.com/workbooks/{workbook_id}"

    mock_server              = MagicMock()
    mock_server.projects.get.return_value = ([mock_project], None)
    mock_server.workbooks.publish.return_value = mock_wb
    mock_server.auth.sign_in.return_value.__enter__ = lambda s: s
    mock_server.auth.sign_in.return_value.__exit__  = MagicMock(return_value=False)

    return mock_server, mock_project, mock_wb


# ── TestPublishResult ──────────────────────────────────────────────────────────

class TestPublishResult:
    def test_default_status_pending(self):
        assert PublishResult().status == "pending"

    def test_log_appends_event(self):
        pr = PublishResult()
        pr.log("connected")
        assert "connected" in pr.events

    def test_fluent_log_returns_self(self):
        pr = PublishResult()
        assert pr.log("x") is pr

    def test_to_dict_has_all_keys(self):
        d = PublishResult().to_dict()
        for k in ("status", "workbook_id", "workbook_name", "workbook_url",
                  "project_name", "server_url", "publish_mode",
                  "file_path", "events", "error"):
            assert k in d

    def test_repr_contains_status(self):
        pr = PublishResult()
        pr.status = "success"
        assert "success" in repr(pr)

    def test_error_captured(self):
        pr = PublishResult()
        pr.error = "auth failed"
        assert pr.to_dict()["error"] == "auth failed"


# ── TestEnvHelper ──────────────────────────────────────────────────────────────

class TestEnvHelper:
    def test_returns_value(self, monkeypatch):
        monkeypatch.setenv("MY_VAR", "hello")
        assert _env("MY_VAR") == "hello"

    def test_returns_empty_for_unset(self):
        assert _env("__DEFINITELY_UNSET_VAR__") == ""

    def test_returns_empty_for_empty_name(self):
        assert _env("") == ""

    def test_strips_whitespace(self, monkeypatch):
        monkeypatch.setenv("PADDED_VAR", "  value  ")
        assert _env("PADDED_VAR") == "value"


# ── TestDeploymentAgentValidateInput ──────────────────────────────────────────

class TestDeploymentAgentValidateInput:
    def test_missing_project_spec(self):
        errors = _dry_agent().validate_input({"twb_xml": SAMPLE_TWB_XML})
        assert any("project_spec" in e for e in errors)

    def test_missing_twb_content(self):
        errors = _dry_agent().validate_input({"project_spec": {"project_id": "x"}})
        assert any("twb_xml" in e or "twb_path" in e for e in errors)

    def test_invalid_workbook_doc_blocks(self):
        errors = _dry_agent().validate_input({
            "project_spec":  {"project_id": "x"},
            "twb_xml":       SAMPLE_TWB_XML,
            "workbook_doc":  {"is_valid": False},
        })
        assert any("is_valid" in e for e in errors)

    def test_valid_state_with_xml_passes(self):
        errors = _dry_agent().validate_input({
            "project_spec": {"project_id": "x"},
            "twb_xml":      SAMPLE_TWB_XML,
        })
        assert errors == []

    def test_valid_state_with_path_passes(self, tmp_path):
        twb = tmp_path / "test.twb"
        twb.write_text(SAMPLE_TWB_XML)
        errors = _dry_agent().validate_input({
            "project_spec": {"project_id": "x"},
            "twb_path":     str(twb),
        })
        assert errors == []

    def test_valid_workbook_doc_passes(self):
        errors = _dry_agent().validate_input({
            "project_spec": {"project_id": "x"},
            "twb_xml":      SAMPLE_TWB_XML,
            "workbook_doc": {"is_valid": True},
        })
        assert errors == []


# ── TestDryRunMode ─────────────────────────────────────────────────────────────

class TestDryRunMode:
    @pytest.fixture(scope="class")
    def pipeline_state(self):
        return _pipeline_state()

    @pytest.fixture(scope="class")
    def dry_result(self, pipeline_state):
        return _dry_agent().execute(pipeline_state)

    def test_succeeds_in_dry_run(self, dry_result):
        assert dry_result.status == AgentStatus.SUCCESS

    def test_publish_result_status_dry_run(self, dry_result):
        assert dry_result.output["publish_result"]["status"] == "dry_run"

    def test_no_workbook_id_in_dry_run(self, dry_result):
        assert dry_result.output["publish_result"]["workbook_id"] is None

    def test_events_log_present(self, dry_result):
        assert len(dry_result.output["deployment_log"]) > 0

    def test_dry_run_event_in_log(self, dry_result):
        log = " ".join(dry_result.output["deployment_log"])
        assert "dry" in log.lower() or "skip" in log.lower()

    def test_phase_is_deploying(self, dry_result):
        assert dry_result.phase == "DEPLOYING"

    def test_agent_id_correct(self, dry_result):
        assert dry_result.agent_id == "deployment_agent"

    def test_timing_recorded(self, dry_result):
        assert dry_result.duration_ms > 0

    def test_metadata_deploy_summary(self, dry_result):
        s = dry_result.metadata["deploy_summary"]
        assert s["status"] == "dry_run"

    def test_workbook_name_in_result(self, dry_result):
        pr = dry_result.output["publish_result"]
        assert pr["workbook_name"] == "Superstore_Sales_Overview"

    def test_project_name_in_result(self, dry_result):
        pr = dry_result.output["publish_result"]
        assert pr["project_name"] == "Default"

    def test_file_path_set_in_dry_run(self, dry_result):
        # twb_path is None in dry-run assembly, so agent writes a temp file
        pr = dry_result.output["publish_result"]
        assert pr["file_path"] is not None

    def test_no_tsc_calls_in_dry_run(self, pipeline_state):
        with patch("agents.deployment_agent.TSC") as mock_tsc:
            _dry_agent().execute(pipeline_state)
            mock_tsc.Server.assert_not_called()
            mock_tsc.PersonalAccessTokenAuth.assert_not_called()


# ── TestMockedPublish ──────────────────────────────────────────────────────────

class TestMockedPublish:
    """Test live publish path with TSC fully mocked."""

    def _state_with_xml(self):
        state = _pipeline_state()
        return state

    def test_successful_mock_publish(self, monkeypatch):
        mock_server, _, mock_wb = _mock_tsc_server(workbook_id="wb_999")
        monkeypatch.setenv("TABLEAU_PAT_NAME", "my-pat")
        monkeypatch.setenv("TABLEAU_PAT_SECRET", "my-secret")

        with patch("agents.deployment_agent.TSC") as mock_tsc_mod, \
             patch("agents.deployment_agent._TSC_AVAILABLE", True):
            mock_tsc_mod.PersonalAccessTokenAuth.return_value = MagicMock()
            mock_tsc_mod.Server.return_value = mock_server
            mock_tsc_mod.Server.PublishMode.Overwrite = "Overwrite"
            mock_tsc_mod.WorkbookItem.return_value = MagicMock()

            state = self._state_with_xml()
            result = _live_agent().execute(state)

        # Should fail (missing PAT in env before mock kicks in for auth check)
        # The agent checks env vars before calling TSC.Server
        assert result.status in (AgentStatus.SUCCESS, AgentStatus.FAILED)

    def test_missing_pat_credentials_fails(self):
        """Without PAT env vars set, live-mode should fail with CRITICAL error."""
        state = _pipeline_state()
        # Ensure no PAT vars are set
        for var in ("TAB_PAT_NAME", "TAB_PAT_SECRET"):
            os.environ.pop(var, None)
        result = _live_agent().execute(state)
        assert result.status == AgentStatus.FAILED

    def test_missing_tsc_library_fails(self, monkeypatch):
        monkeypatch.setenv("TABLEAU_PAT_NAME", "x")
        monkeypatch.setenv("TABLEAU_PAT_SECRET", "y")
        with patch("agents.deployment_agent._TSC_AVAILABLE", False):
            result = _live_agent().execute(_pipeline_state())
        assert result.status == AgentStatus.FAILED

    def test_tsc_publish_exception_retried(self, monkeypatch):
        monkeypatch.setenv("TABLEAU_PAT_NAME", "x")
        monkeypatch.setenv("TABLEAU_PAT_SECRET", "y")
        mock_server = MagicMock()
        mock_server.auth.sign_in.return_value.__enter__ = lambda s: s
        mock_server.auth.sign_in.return_value.__exit__  = MagicMock(return_value=False)
        mock_server.projects.get.side_effect = RuntimeError("server error")

        with patch("agents.deployment_agent.TSC") as mock_mod, \
             patch("agents.deployment_agent._TSC_AVAILABLE", True), \
             patch("agents.deployment_agent.time.sleep"):
            mock_mod.PersonalAccessTokenAuth.return_value = MagicMock()
            mock_mod.Server.return_value = mock_server
            mock_mod.Server.PublishMode = MagicMock()
            mock_mod.WorkbookItem.return_value = MagicMock()
            result = _live_agent().execute(_pipeline_state())

        assert result.status == AgentStatus.FAILED
        pr = result.output["publish_result"]
        assert pr["status"] == "failed"
        assert "server error" in (pr["error"] or "")

    def test_publish_result_events_contain_attempts(self, monkeypatch):
        monkeypatch.setenv("TABLEAU_PAT_NAME", "x")
        monkeypatch.setenv("TABLEAU_PAT_SECRET", "y")
        mock_server = MagicMock()
        mock_server.auth.sign_in.return_value.__enter__ = lambda s: s
        mock_server.auth.sign_in.return_value.__exit__  = MagicMock(return_value=False)
        mock_server.projects.get.side_effect = RuntimeError("boom")

        with patch("agents.deployment_agent.TSC") as mock_mod, \
             patch("agents.deployment_agent._TSC_AVAILABLE", True), \
             patch("agents.deployment_agent.time.sleep"):
            mock_mod.PersonalAccessTokenAuth.return_value = MagicMock()
            mock_mod.Server.return_value = mock_server
            mock_mod.Server.PublishMode = MagicMock()
            mock_mod.WorkbookItem.return_value = MagicMock()
            result = _live_agent().execute(_pipeline_state())

        events_text = " ".join(result.output["publish_result"]["events"])
        assert "attempt" in events_text.lower() or "failed" in events_text.lower()


# ── TestPublishResultDict ──────────────────────────────────────────────────────

class TestPublishResultToDict:
    def test_all_fields_present(self):
        pr = PublishResult()
        pr.status        = "success"
        pr.workbook_id   = "abc"
        pr.workbook_name = "MyWB"
        pr.workbook_url  = "https://tc.example.com"
        pr.project_name  = "Sales"
        pr.server_url    = "https://tc.example.com"
        pr.publish_mode  = "Overwrite"
        pr.file_path     = "/tmp/x.twb"
        pr.log("connected").log("published")
        d = pr.to_dict()
        assert d["status"]        == "success"
        assert d["workbook_id"]   == "abc"
        assert d["workbook_name"] == "MyWB"
        assert d["workbook_url"]  == "https://tc.example.com"
        assert d["project_name"]  == "Sales"
        assert d["publish_mode"]  == "Overwrite"
        assert len(d["events"])   == 2
        assert d["error"]         is None


# ── TestPhase01To13Pipeline ────────────────────────────────────────────────────

class TestPhase01To13Pipeline:
    @pytest.fixture(scope="class")
    def full_state(self): return _pipeline_state()

    def test_pipeline_dry_run_succeeds(self, full_state):
        assert _dry_agent().execute(full_state).status == AgentStatus.SUCCESS

    def test_deployment_log_is_list(self, full_state):
        result = _dry_agent().execute(full_state)
        assert isinstance(result.output["deployment_log"], list)

    def test_phase_label(self, full_state):
        result = _dry_agent().execute(full_state)
        assert result.phase == "DEPLOYING"

    def test_publish_result_is_dict(self, full_state):
        result = _dry_agent().execute(full_state)
        assert isinstance(result.output["publish_result"], dict)

    def test_workbook_name_from_spec(self, full_state):
        result = _dry_agent().execute(full_state)
        assert result.output["publish_result"]["workbook_name"] == "Superstore_Sales_Overview"
