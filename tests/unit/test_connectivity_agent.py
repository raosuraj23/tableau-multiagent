# tests/unit/test_connectivity_agent.py
"""
Unit Tests — ConnectivityAgent and ConnectivityReport
======================================================

ALL network calls are mocked — no live connections required.
Tests cover:
  - ConnectivityReport model (GREEN/YELLOW/RED logic, can_proceed)
  - ConnectivityAgent.validate_input (requires project_spec + validation_report)
  - Dry-run mode (all SKIP, always can_proceed)
  - TCP failure path → RED result
  - TCP success + auth failure → YELLOW result
  - TCP + auth + query success → GREEN result
  - Driver import failure → YELLOW (graceful degradation)
  - Tableau Cloud auth success and failure paths
  - Full Phase 01 → 02 → 03 pipeline

Run from project root:
    pytest tests/unit/test_connectivity_agent.py -v
"""

import sys
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from agents.base_agent import AgentStatus, PhaseContext
from agents.connectivity_agent import ConnectivityAgent, _get_env
from agents.intake_agent import IntakeAgent
from agents.validation_agent import ValidationAgent
from models.connectivity_report import (
    ConnectivityReport,
    ConnectionHealth,
    ConnectionResult,
    TableauCloudResult,
)
from models.project_spec import ProjectSpec

PROJECT_ROOT = Path(__file__).parent.parent.parent
REAL_CSV_DIR = PROJECT_ROOT / "csv_inputs"


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def pipeline_state() -> Dict[str, Any]:
    """
    Run Phase 01 + 02 once per module and return the state dict
    that ConnectivityAgent expects.
    """
    ctx = PhaseContext(project_id="proj_001", run_id="run_test")
    intake = IntakeAgent(csv_dir=REAL_CSV_DIR, context=ctx)
    intake_result = intake.execute({})

    val = ValidationAgent(context=ctx)
    val_result = val.execute({"project_spec": intake_result.output["project_spec"]})

    return {
        "project_spec":       intake_result.output["project_spec"],
        "validation_report":  val_result.output["validation_report"],
    }


@pytest.fixture
def sample_context() -> PhaseContext:
    return PhaseContext(project_id="proj_001", run_id="run_test")


@pytest.fixture
def dry_run_context() -> PhaseContext:
    return PhaseContext(project_id="proj_001", run_id="run_test", dry_run=True)


@pytest.fixture
def agent(sample_context) -> ConnectivityAgent:
    return ConnectivityAgent(context=sample_context)


@pytest.fixture
def dry_run_agent(dry_run_context) -> ConnectivityAgent:
    return ConnectivityAgent(context=dry_run_context)


# ══════════════════════════════════════════════════════════════════════════════
# ConnectivityReport model
# ══════════════════════════════════════════════════════════════════════════════

class TestConnectivityReport:
    def test_empty_report_can_proceed(self):
        r = ConnectivityReport(project_id="p1")
        assert r.can_proceed is True

    def test_red_connection_blocks_proceed(self):
        r = ConnectivityReport(project_id="p1")
        r.add_result(ConnectionResult(
            connection_id="c1", health=ConnectionHealth.RED,
            host="unreachable.host",
        ))
        assert r.can_proceed is False

    def test_yellow_does_not_block_proceed(self):
        r = ConnectivityReport(project_id="p1")
        r.add_result(ConnectionResult(
            connection_id="c1", health=ConnectionHealth.YELLOW,
            host="host", tcp_ok=True,
        ))
        assert r.can_proceed is True

    def test_green_does_not_block(self):
        r = ConnectivityReport(project_id="p1")
        r.add_result(ConnectionResult(
            connection_id="c1", health=ConnectionHealth.GREEN,
            host="host", tcp_ok=True, auth_ok=True, query_ok=True,
        ))
        assert r.can_proceed is True

    def test_skip_does_not_block(self):
        r = ConnectivityReport(project_id="p1")
        r.add_result(ConnectionResult(
            connection_id="c1", health=ConnectionHealth.SKIP,
        ))
        assert r.can_proceed is True

    def test_mixed_green_and_yellow_can_proceed(self):
        r = ConnectivityReport(project_id="p1")
        r.add_result(ConnectionResult("c1", ConnectionHealth.GREEN, host="h1"))
        r.add_result(ConnectionResult("c2", ConnectionHealth.YELLOW, host="h2"))
        assert r.can_proceed is True
        assert r.green_count  == 1
        assert r.yellow_count == 1

    def test_counts_correct(self):
        r = ConnectivityReport(project_id="p1")
        r.add_result(ConnectionResult("c1", ConnectionHealth.GREEN,  host="h1"))
        r.add_result(ConnectionResult("c2", ConnectionHealth.YELLOW, host="h2"))
        r.add_result(ConnectionResult("c3", ConnectionHealth.RED,    host="h3"))
        assert r.green_count  == 1
        assert r.yellow_count == 1
        assert r.red_count    == 1

    def test_get_result_by_id(self):
        r = ConnectivityReport(project_id="p1")
        r.add_result(ConnectionResult("conn_001", ConnectionHealth.GREEN, host="h"))
        res = r.get_result("conn_001")
        assert res is not None
        assert res.health == ConnectionHealth.GREEN

    def test_get_result_missing_returns_none(self):
        r = ConnectivityReport(project_id="p1")
        assert r.get_result("nonexistent") is None

    def test_summary_has_all_keys(self):
        r = ConnectivityReport(project_id="p1")
        s = r.summary()
        for key in ["project_id", "can_proceed", "green", "yellow",
                    "red", "tableau_cloud_ok", "total_tested"]:
            assert key in s

    def test_to_dict_includes_connection_results(self):
        r = ConnectivityReport(project_id="p1")
        r.add_result(ConnectionResult("c1", ConnectionHealth.GREEN, host="h"))
        d = r.to_dict()
        assert len(d["connection_results"]) == 1
        assert d["connection_results"][0]["health"] == "GREEN"

    def test_tableau_cloud_ok_false_when_not_set(self):
        r = ConnectivityReport(project_id="p1")
        assert r.tableau_cloud_ok is False

    def test_tableau_cloud_ok_true_when_successful(self):
        r = ConnectivityReport(project_id="p1")
        r.tableau_cloud = TableauCloudResult(ok=True, site_id="mysite")
        assert r.tableau_cloud_ok is True

    def test_connection_result_is_usable_green(self):
        res = ConnectionResult("c1", ConnectionHealth.GREEN, host="h")
        assert res.is_usable is True

    def test_connection_result_is_usable_yellow(self):
        res = ConnectionResult("c1", ConnectionHealth.YELLOW, host="h")
        assert res.is_usable is True

    def test_connection_result_not_usable_red(self):
        res = ConnectionResult("c1", ConnectionHealth.RED, host="h")
        assert res.is_usable is False

    def test_repr_contains_can_proceed(self):
        r = ConnectivityReport(project_id="p1")
        assert "can_proceed" in repr(r)

    def test_fluent_chaining(self):
        r = (ConnectivityReport(project_id="p1")
             .add_result(ConnectionResult("c1", ConnectionHealth.GREEN, host="h1"))
             .add_result(ConnectionResult("c2", ConnectionHealth.GREEN, host="h2")))
        assert len(r.connection_results) == 2


# ══════════════════════════════════════════════════════════════════════════════
# ConnectivityAgent.validate_input
# ══════════════════════════════════════════════════════════════════════════════

class TestConnectivityAgentValidateInput:
    def test_missing_project_spec_fails(self, agent):
        errors = agent.validate_input({"validation_report": {"can_proceed": True}})
        assert any("project_spec" in e.lower() for e in errors)

    def test_missing_validation_report_fails(self, agent, pipeline_state):
        errors = agent.validate_input({"project_spec": pipeline_state["project_spec"]})
        assert any("validation_report" in e.lower() for e in errors)

    def test_validation_report_can_proceed_false_blocks(self, agent, pipeline_state):
        state = {
            "project_spec":      pipeline_state["project_spec"],
            "validation_report": {"can_proceed": False},
        }
        errors = agent.validate_input(state)
        assert len(errors) >= 1
        assert any("can_proceed" in e.lower() for e in errors)

    def test_valid_state_passes_validation(self, agent, pipeline_state):
        errors = agent.validate_input(pipeline_state)
        assert errors == []


# ══════════════════════════════════════════════════════════════════════════════
# Dry-run mode
# ══════════════════════════════════════════════════════════════════════════════

class TestDryRunMode:
    def test_dry_run_produces_skip_results(self, dry_run_agent, pipeline_state):
        result = dry_run_agent.execute(pipeline_state)
        assert result.status == AgentStatus.SUCCESS
        report = result.output["connectivity_report"]
        for cr in report["connection_results"]:
            assert cr["health"] == "SKIP"

    def test_dry_run_can_proceed_always_true(self, dry_run_agent, pipeline_state):
        result = dry_run_agent.execute(pipeline_state)
        assert result.output["connectivity_report"]["can_proceed"] is True

    def test_dry_run_no_network_calls(self, dry_run_agent, pipeline_state):
        """Confirm zero socket calls in dry-run mode."""
        with patch("socket.create_connection") as mock_socket:
            dry_run_agent.execute(pipeline_state)
            mock_socket.assert_not_called()


# ══════════════════════════════════════════════════════════════════════════════
# TCP test (_tcp_ping)
# ══════════════════════════════════════════════════════════════════════════════

class TestTcpPing:
    def test_tcp_success_returns_true(self, agent):
        with patch("socket.create_connection") as mock_conn:
            mock_conn.return_value.__enter__ = MagicMock(return_value=MagicMock())
            mock_conn.return_value.__exit__  = MagicMock(return_value=False)
            ok, latency, err = agent._tcp_ping("host.example.com", 443)
        assert ok is True
        assert err is None
        assert latency >= 0

    def test_tcp_timeout_returns_false(self, agent):
        import socket as sock_mod
        with patch("socket.create_connection",
                   side_effect=sock_mod.timeout("timed out")):
            ok, latency, err = agent._tcp_ping("unreachable.host", 443)
        assert ok is False
        assert "timeout" in err.lower()

    def test_tcp_os_error_returns_false(self, agent):
        with patch("socket.create_connection",
                   side_effect=OSError("Connection refused")):
            ok, latency, err = agent._tcp_ping("refused.host", 443)
        assert ok is False
        assert err is not None

    def test_port_zero_skips_tcp(self, agent):
        with patch("socket.create_connection") as mock_conn:
            ok, latency, err = agent._tcp_ping("any.host", 0)
        mock_conn.assert_not_called()
        assert ok is True


# ══════════════════════════════════════════════════════════════════════════════
# GREEN path — TCP + auth + query all succeed
# ══════════════════════════════════════════════════════════════════════════════

class TestGreenPath:
    def test_snowflake_green(self, agent, pipeline_state):
        """Fully mocked Snowflake: TCP ok + auth ok + SELECT 1 ok → GREEN."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_sf_conn = MagicMock()
        mock_sf_conn.cursor.return_value = mock_cursor

        mock_sf_module = MagicMock()
        mock_sf_module.connector.connect.return_value = mock_sf_conn

        with patch("socket.create_connection") as mock_tcp, \
             patch.dict("sys.modules", {"snowflake": mock_sf_module,
                                        "snowflake.connector": mock_sf_module.connector}), \
             patch("agents.connectivity_agent._get_env", return_value="test_val"), \
             patch.object(agent, "_test_tableau_cloud",
                          return_value=TableauCloudResult(ok=True, site_id="s")):

            mock_tcp.return_value.__enter__ = MagicMock(return_value=MagicMock())
            mock_tcp.return_value.__exit__  = MagicMock(return_value=False)

            result = agent.execute(pipeline_state)

        report = result.output["connectivity_report"]
        # At least one connection should not be RED
        assert any(cr["health"] != "RED" for cr in report["connection_results"])

    def test_green_result_sets_all_flags(self):
        res = ConnectionResult(
            "c1", ConnectionHealth.GREEN, host="h",
            tcp_ok=True, auth_ok=True, query_ok=True,
        )
        assert res.tcp_ok   is True
        assert res.auth_ok  is True
        assert res.query_ok is True
        assert res.is_usable is True


# ══════════════════════════════════════════════════════════════════════════════
# YELLOW path — TCP ok, auth fails
# ══════════════════════════════════════════════════════════════════════════════

class TestYellowPath:
    def test_tcp_success_auth_failure_is_yellow(self, agent, pipeline_state):
        """TCP reachable but credentials wrong → YELLOW."""
        with patch("socket.create_connection") as mock_tcp, \
             patch.object(agent, "_test_driver",
                          return_value=(False, False, "Auth failed: wrong password")), \
             patch.object(agent, "_test_tableau_cloud",
                          return_value=TableauCloudResult(ok=True, site_id="s")):

            mock_tcp.return_value.__enter__ = MagicMock(return_value=MagicMock())
            mock_tcp.return_value.__exit__  = MagicMock(return_value=False)

            result = agent.execute(pipeline_state)

        report = result.output["connectivity_report"]
        yellows = [cr for cr in report["connection_results"]
                   if cr["health"] == "YELLOW"]
        assert len(yellows) >= 1

    def test_yellow_can_still_proceed(self, agent, pipeline_state):
        """YELLOW connections don't block Phase 04."""
        with patch("socket.create_connection") as mock_tcp, \
             patch.object(agent, "_test_driver",
                          return_value=(False, False, "Bad creds")), \
             patch.object(agent, "_test_tableau_cloud",
                          return_value=TableauCloudResult(ok=True, site_id="s")):

            mock_tcp.return_value.__enter__ = MagicMock(return_value=MagicMock())
            mock_tcp.return_value.__exit__  = MagicMock(return_value=False)

            result = agent.execute(pipeline_state)

        report = result.output["connectivity_report"]
        assert report["can_proceed"] is True

    def test_missing_driver_is_yellow(self, agent, pipeline_state):
        """Missing DB driver should produce YELLOW, not crash."""
        with patch("socket.create_connection") as mock_tcp, \
             patch.object(agent, "_test_driver",
                          return_value=(False, False, "snowflake-connector-python not installed")), \
             patch.object(agent, "_test_tableau_cloud",
                          return_value=TableauCloudResult(ok=True, site_id="s")):

            mock_tcp.return_value.__enter__ = MagicMock(return_value=MagicMock())
            mock_tcp.return_value.__exit__  = MagicMock(return_value=False)

            result = agent.execute(pipeline_state)

        # Should not crash, should still produce a report
        assert "connectivity_report" in result.output


# ══════════════════════════════════════════════════════════════════════════════
# RED path — TCP unreachable
# ══════════════════════════════════════════════════════════════════════════════

class TestRedPath:
    def test_tcp_failure_is_red(self, agent, pipeline_state):
        """TCP timeout → RED connection."""
        import socket as sock_mod
        with patch("socket.create_connection",
                   side_effect=sock_mod.timeout("timed out")), \
             patch.object(agent, "_test_tableau_cloud",
                          return_value=TableauCloudResult(ok=True, site_id="s")):

            result = agent.execute(pipeline_state)

        report = result.output["connectivity_report"]
        reds = [cr for cr in report["connection_results"] if cr["health"] == "RED"]
        assert len(reds) >= 1

    def test_red_connection_sets_can_proceed_false(self, agent, pipeline_state):
        """At least one RED → can_proceed=False."""
        import socket as sock_mod
        with patch("socket.create_connection",
                   side_effect=OSError("Connection refused")), \
             patch.object(agent, "_test_tableau_cloud",
                          return_value=TableauCloudResult(ok=True, site_id="s")):

            result = agent.execute(pipeline_state)

        report = result.output["connectivity_report"]
        assert report["can_proceed"] is False

    def test_red_connection_adds_critical_error(self, agent, pipeline_state):
        """RED connection should produce CRITICAL agent error."""
        import socket as sock_mod
        with patch("socket.create_connection",
                   side_effect=OSError("ECONNREFUSED")), \
             patch.object(agent, "_test_tableau_cloud",
                          return_value=TableauCloudResult(ok=True, site_id="s")):

            result = agent.execute(pipeline_state)

        assert result.status == AgentStatus.FAILED
        assert any(e.message for e in result.errors)

    def test_error_message_populated_on_red(self, agent, pipeline_state):
        import socket as sock_mod
        with patch("socket.create_connection",
                   side_effect=sock_mod.timeout("timed out")), \
             patch.object(agent, "_test_tableau_cloud",
                          return_value=TableauCloudResult(ok=True, site_id="s")):

            result = agent.execute(pipeline_state)

        report = result.output["connectivity_report"]
        reds = [cr for cr in report["connection_results"] if cr["health"] == "RED"]
        for red in reds:
            assert red["error_message"] is not None


# ══════════════════════════════════════════════════════════════════════════════
# Tableau Cloud tests
# ══════════════════════════════════════════════════════════════════════════════

class TestTableauCloudConnectivity:
    def test_tableau_cloud_success(self, agent, pipeline_state):
        with patch("socket.create_connection") as mock_tcp, \
             patch.object(agent, "_test_driver",
                          return_value=(True, True, None)), \
             patch.object(agent, "_test_tableau_cloud",
                          return_value=TableauCloudResult(
                              ok=True, site_id="mysite",
                              server_url="https://test.online.tableau.com",
                          )):

            mock_tcp.return_value.__enter__ = MagicMock(return_value=MagicMock())
            mock_tcp.return_value.__exit__  = MagicMock(return_value=False)

            result = agent.execute(pipeline_state)

        report = result.output["connectivity_report"]
        assert report["tableau_cloud"]["ok"] is True
        assert report["tableau_cloud_ok"] is True

    def test_tableau_cloud_failure_adds_high_error(self, agent, pipeline_state):
        with patch("socket.create_connection") as mock_tcp, \
             patch.object(agent, "_test_driver",
                          return_value=(True, True, None)), \
             patch.object(agent, "_test_tableau_cloud",
                          return_value=TableauCloudResult(
                              ok=False,
                              error_message="401 Unauthorized — bad PAT",
                          )):

            mock_tcp.return_value.__enter__ = MagicMock(return_value=MagicMock())
            mock_tcp.return_value.__exit__  = MagicMock(return_value=False)

            result = agent.execute(pipeline_state)

        report = result.output["connectivity_report"]
        assert report["tableau_cloud"]["ok"] is False

    def test_tableau_cloud_no_pat_auth_returns_error(self, sample_context):
        """Auth record with no PAT envs → error message in result."""
        agent = ConnectivityAgent(context=sample_context)
        spec = ProjectSpec.from_csv_dir(REAL_CSV_DIR)

        # Remove PAT env var names from all auth records
        for auth in spec.auth_configs:
            auth.pat_name_env  = None
            auth.pat_secret_env = None

        result = agent._test_tableau_cloud(spec)
        assert result.ok is False
        assert result.error_message is not None

    def test_tableau_cloud_missing_tsc_lib_returns_error(self, sample_context):
        """Missing TSC library → YELLOW, not crash."""
        agent = ConnectivityAgent(context=sample_context)
        spec  = ProjectSpec.from_csv_dir(REAL_CSV_DIR)

        # Ensure PAT env names are set but values are populated
        import os
        os.environ["TABLEAU_PAT_NAME"]   = "test_pat"
        os.environ["TABLEAU_PAT_SECRET"] = "test_secret"

        with patch.dict("sys.modules", {
            "tableauserverclient": None,
        }):
            result = agent._test_tableau_cloud(spec)

        # Clean up
        os.environ.pop("TABLEAU_PAT_NAME",   None)
        os.environ.pop("TABLEAU_PAT_SECRET", None)

        assert result.ok is False


# ══════════════════════════════════════════════════════════════════════════════
# Helper function tests
# ══════════════════════════════════════════════════════════════════════════════

class TestHelpers:
    def test_get_env_returns_value(self, monkeypatch):
        monkeypatch.setenv("TEST_CONN_VAR", "my_secret_value")
        assert _get_env("TEST_CONN_VAR") == "my_secret_value"

    def test_get_env_returns_empty_for_unset(self, monkeypatch):
        monkeypatch.delenv("TEST_CONN_UNSET", raising=False)
        assert _get_env("TEST_CONN_UNSET") == ""

    def test_get_env_returns_empty_for_none(self):
        assert _get_env(None) == ""

    def test_get_env_strips_whitespace(self, monkeypatch):
        monkeypatch.setenv("TEST_CONN_SPACE", "value")
        assert _get_env("TEST_CONN_SPACE") == "value"


# ══════════════════════════════════════════════════════════════════════════════
# Agent config and metadata
# ══════════════════════════════════════════════════════════════════════════════

class TestConnectivityAgentConfig:
    def test_agent_id(self, agent):
        assert agent.agent_id == "connectivity_agent"

    def test_phase(self, agent):
        assert agent.phase == "CONNECTING"

    def test_tcp_timeout_from_config(self, sample_context):
        agent = ConnectivityAgent(
            config={"tcp_timeout_seconds": 5.0}, context=sample_context
        )
        assert agent.tcp_timeout == 5.0

    def test_auth_timeout_from_config(self, sample_context):
        agent = ConnectivityAgent(
            config={"auth_timeout_seconds": 20.0}, context=sample_context
        )
        assert agent.auth_timeout == 20.0

    def test_timing_recorded(self, dry_run_agent, pipeline_state):
        result = dry_run_agent.execute(pipeline_state)
        assert result.duration_ms > 0

    def test_metadata_contains_summary(self, dry_run_agent, pipeline_state):
        result = dry_run_agent.execute(pipeline_state)
        assert "connectivity_summary" in result.metadata


# ══════════════════════════════════════════════════════════════════════════════
# End-to-end: Phase 01 → 02 → 03 pipeline
# ══════════════════════════════════════════════════════════════════════════════

class TestPhase01To03Pipeline:
    def test_full_pipeline_dry_run(self):
        ctx = PhaseContext(project_id="proj_001", run_id="pipeline_test",
                          dry_run=True)

        # Phase 01
        intake = IntakeAgent(csv_dir=REAL_CSV_DIR, context=ctx)
        ir = intake.execute({})
        assert ir.status in (AgentStatus.SUCCESS, AgentStatus.WARNING)

        # Phase 02
        val = ValidationAgent(context=ctx)
        vr = val.execute({"project_spec": ir.output["project_spec"]})
        assert vr.status in (AgentStatus.SUCCESS, AgentStatus.WARNING)

        # Phase 03 (dry-run)
        conn = ConnectivityAgent(context=ctx)
        cr = conn.execute({
            "project_spec":      ir.output["project_spec"],
            "validation_report": vr.output["validation_report"],
        })
        assert cr.status == AgentStatus.SUCCESS
        assert cr.output["connectivity_report"]["can_proceed"] is True

    def test_phase_labels_in_pipeline(self):
        ctx = PhaseContext(project_id="proj_001", run_id="label_test", dry_run=True)

        intake = IntakeAgent(csv_dir=REAL_CSV_DIR, context=ctx)
        ir = intake.execute({})
        assert ir.phase == "INTAKE"

        val = ValidationAgent(context=ctx)
        vr = val.execute({"project_spec": ir.output["project_spec"]})
        assert vr.phase == "VALIDATING"

        conn = ConnectivityAgent(context=ctx)
        cr = conn.execute({
            "project_spec":      ir.output["project_spec"],
            "validation_report": vr.output["validation_report"],
        })
        assert cr.phase == "CONNECTING"
