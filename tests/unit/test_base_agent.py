# tests/unit/test_base_agent.py
"""
Unit Tests — BaseAgent, AgentResult, AgentError, PhaseContext, LLMRouter
=========================================================================

Run from project root with venv active:
    pytest tests/unit/test_base_agent.py -v

Tests do NOT make live API calls — all LLM calls are mocked.
"""

import os
import sys
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock
from typing import Any, Dict, List

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from agents.base_agent import (
    AgentError,
    AgentResult,
    AgentStatus,
    BaseAgent,
    ErrorSeverity,
    PhaseContext,
)


# ══════════════════════════════════════════════════════════════════════════════
# Fixtures
# ══════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def sample_context():
    return PhaseContext(
        project_id="proj_test",
        run_id="run_001",
        environment="dev",
        dry_run=False,
    )


@pytest.fixture
def minimal_state():
    return {
        "project_spec": {"project_id": "proj_test"},
        "validation_report": {},
    }


class ConcreteAgent(BaseAgent):
    """Minimal concrete agent for testing BaseAgent functionality."""

    def __init__(self, *args, should_fail=False, should_raise=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.should_fail  = should_fail
        self.should_raise = should_raise
        self.run_called   = False

    def validate_input(self, state: Dict[str, Any]) -> List[str]:
        errors = []
        if not state.get("project_spec"):
            errors.append("project_spec is required")
        return errors

    def run(self, state: Dict[str, Any]) -> AgentResult:
        self.log_start()
        self.run_called = True
        result = AgentResult(agent_id=self.agent_id, phase=self.phase)

        if self.should_raise:
            raise ValueError("intentional test exception")

        if self.should_fail:
            result.add_error("intentional failure", severity=ErrorSeverity.CRITICAL)
            return self.log_complete(result)

        result.output = {"processed": True}
        result.status = AgentStatus.SUCCESS
        return self.log_complete(result)


# ══════════════════════════════════════════════════════════════════════════════
# AgentStatus
# ══════════════════════════════════════════════════════════════════════════════

class TestAgentStatus:
    def test_all_status_values_exist(self):
        assert AgentStatus.SUCCESS  == "success"
        assert AgentStatus.WARNING  == "warning"
        assert AgentStatus.FAILED   == "failed"
        assert AgentStatus.SKIPPED  == "skipped"
        assert AgentStatus.PENDING  == "pending"

    def test_status_is_string_comparable(self):
        result = AgentResult(agent_id="a", phase="p")
        assert result.status == AgentStatus.PENDING
        assert result.status == "pending"


# ══════════════════════════════════════════════════════════════════════════════
# AgentError
# ══════════════════════════════════════════════════════════════════════════════

class TestAgentError:
    def test_critical_error_is_blocking(self):
        err = AgentError("critical issue", severity=ErrorSeverity.CRITICAL)
        assert err.is_blocking is True

    def test_high_error_is_blocking(self):
        err = AgentError("high issue", severity=ErrorSeverity.HIGH)
        assert err.is_blocking is True

    def test_medium_error_is_not_blocking(self):
        err = AgentError("medium issue", severity=ErrorSeverity.MEDIUM)
        assert err.is_blocking is False

    def test_low_error_is_not_blocking(self):
        err = AgentError("low issue", severity=ErrorSeverity.LOW)
        assert err.is_blocking is False

    def test_to_dict_contains_required_keys(self):
        err = AgentError("test", severity=ErrorSeverity.HIGH, field="col_001")
        d = err.to_dict()
        assert "message" in d
        assert "severity" in d
        assert "field" in d
        assert "timestamp" in d
        assert d["field"] == "col_001"
        assert d["severity"] == "HIGH"

    def test_timestamp_is_set_automatically(self):
        err = AgentError("test")
        assert err.timestamp is not None
        assert "T" in err.timestamp  # ISO format


# ══════════════════════════════════════════════════════════════════════════════
# PhaseContext
# ══════════════════════════════════════════════════════════════════════════════

class TestPhaseContext:
    def test_correlation_id_auto_generated(self):
        ctx = PhaseContext(project_id="proj_x", run_id="run_99")
        assert ctx.correlation_id == "proj_x_run_99"

    def test_correlation_id_explicit_override(self):
        ctx = PhaseContext(
            project_id="proj_x",
            run_id="run_99",
            correlation_id="custom_id",
        )
        assert ctx.correlation_id == "custom_id"

    def test_defaults(self):
        ctx = PhaseContext(project_id="p", run_id="r")
        assert ctx.environment == "dev"
        assert ctx.dry_run is False
        assert ctx.allow_overwrite is False


# ══════════════════════════════════════════════════════════════════════════════
# AgentResult
# ══════════════════════════════════════════════════════════════════════════════

class TestAgentResult:
    def test_initial_status_is_pending(self):
        r = AgentResult(agent_id="a", phase="p")
        assert r.status == AgentStatus.PENDING

    def test_add_critical_error_sets_failed(self):
        r = AgentResult(agent_id="a", phase="p")
        r.add_error("critical", severity=ErrorSeverity.CRITICAL)
        assert r.status == AgentStatus.FAILED
        assert r.error_count == 1

    def test_add_high_error_sets_failed(self):
        r = AgentResult(agent_id="a", phase="p")
        r.add_error("high", severity=ErrorSeverity.HIGH)
        assert r.status == AgentStatus.FAILED

    def test_add_warning_sets_warning_on_success(self):
        r = AgentResult(agent_id="a", phase="p", status=AgentStatus.SUCCESS)
        r.add_warning("minor issue")
        assert r.status == AgentStatus.WARNING
        assert r.warning_count == 1

    def test_add_warning_does_not_override_failed(self):
        r = AgentResult(agent_id="a", phase="p", status=AgentStatus.FAILED)
        r.add_warning("minor issue")
        assert r.status == AgentStatus.FAILED  # failed stays failed

    def test_fluent_chaining(self):
        r = (
            AgentResult(agent_id="a", phase="p")
            .add_error("err1", severity=ErrorSeverity.HIGH)
            .add_warning("warn1")
        )
        assert r.error_count == 1
        assert r.warning_count == 1

    def test_has_blocking_errors_true(self):
        r = AgentResult(agent_id="a", phase="p")
        r.add_error("blocking", severity=ErrorSeverity.CRITICAL)
        assert r.has_blocking_errors is True

    def test_has_blocking_errors_false_for_medium(self):
        r = AgentResult(agent_id="a", phase="p")
        r.errors.append(AgentError("medium", severity=ErrorSeverity.MEDIUM))
        assert r.has_blocking_errors is False

    def test_to_dict_structure(self):
        r = AgentResult(agent_id="intake_agent", phase="INTAKE",
                        status=AgentStatus.SUCCESS)
        r.output = {"project_spec": {"id": "x"}}
        d = r.to_dict()
        assert d["agent_id"]   == "intake_agent"
        assert d["phase"]      == "INTAKE"
        assert d["status"]     == "success"
        assert "output_keys"   in d
        assert "project_spec"  in d["output_keys"]
        assert "duration_ms"   in d
        assert "timestamp"     in d

    def test_summary_contains_agent_id(self):
        r = AgentResult(agent_id="my_agent", phase="TEST",
                        status=AgentStatus.SUCCESS)
        s = r.summary()
        assert "my_agent" in s
        assert "SUCCESS"  in s

    def test_summary_icons(self):
        for status, icon in [
            (AgentStatus.SUCCESS, "✓"),
            (AgentStatus.WARNING, "⚠"),
            (AgentStatus.FAILED,  "✗"),
            (AgentStatus.SKIPPED, "–"),
        ]:
            r = AgentResult(agent_id="a", phase="p", status=status)
            assert icon in r.summary()


# ══════════════════════════════════════════════════════════════════════════════
# BaseAgent (via ConcreteAgent)
# ══════════════════════════════════════════════════════════════════════════════

class TestBaseAgent:
    def test_repr(self, sample_context):
        agent = ConcreteAgent("my_agent", "TESTING", context=sample_context)
        r = repr(agent)
        assert "my_agent"     in r
        assert "TESTING"      in r
        assert "ConcreteAgent" in r

    def test_execute_success(self, sample_context, minimal_state):
        agent  = ConcreteAgent("test_agent", "TESTING", context=sample_context)
        result = agent.execute(minimal_state)
        assert result.status  == AgentStatus.SUCCESS
        assert agent.run_called is True
        assert result.output.get("processed") is True
        assert result.duration_ms > 0

    def test_execute_sets_timing(self, sample_context, minimal_state):
        agent  = ConcreteAgent("test_agent", "TESTING", context=sample_context)
        result = agent.execute(minimal_state)
        assert result.duration_ms >= 0
        assert isinstance(result.duration_ms, float)

    def test_execute_validation_failure_blocks_run(self, sample_context):
        agent  = ConcreteAgent("test_agent", "TESTING", context=sample_context)
        result = agent.execute({})  # missing project_spec
        assert result.status              == AgentStatus.FAILED
        assert agent.run_called           is False  # run() was NOT called
        assert result.error_count         >= 1
        assert result.has_blocking_errors is True

    def test_execute_handles_unhandled_exception(self, sample_context, minimal_state):
        agent  = ConcreteAgent("test_agent", "TESTING", context=sample_context,
                               should_raise=True)
        result = agent.execute(minimal_state)
        assert result.status         == AgentStatus.FAILED
        assert result.error_count    >= 1
        assert "intentional test exception" in result.errors[0].message
        assert "traceback" in result.metadata

    def test_execute_intentional_failure(self, sample_context, minimal_state):
        agent  = ConcreteAgent("test_agent", "TESTING", context=sample_context,
                               should_fail=True)
        result = agent.execute(minimal_state)
        assert result.status == AgentStatus.FAILED

    def test_agent_id_always_set_on_result(self, sample_context, minimal_state):
        agent  = ConcreteAgent("my_special_agent", "TESTING",
                               context=sample_context)
        result = agent.execute(minimal_state)
        assert result.agent_id == "my_special_agent"
        assert result.phase    == "TESTING"

    def test_get_required_raises_on_missing(self):
        with pytest.raises(KeyError, match="required_field"):
            BaseAgent.get_required({"other": "value"}, "required_field")

    def test_get_required_raises_on_none(self):
        with pytest.raises(KeyError):
            BaseAgent.get_required({"key": None}, "key")

    def test_get_required_returns_value(self):
        val = BaseAgent.get_required({"key": "hello"}, "key")
        assert val == "hello"

    def test_get_optional_returns_default(self):
        val = BaseAgent.get_optional({}, "missing", default=42)
        assert val == 42

    def test_get_optional_returns_value(self):
        val = BaseAgent.get_optional({"key": "found"}, "key", default="default")
        assert val == "found"

    def test_on_start_hook_called(self, sample_context, minimal_state):
        hook_called = []

        class HookAgent(ConcreteAgent):
            def on_start(self, state):
                hook_called.append(True)

        agent  = HookAgent("hook_agent", "TESTING", context=sample_context)
        result = agent.execute(minimal_state)
        assert len(hook_called) == 1

    def test_on_complete_hook_called(self, sample_context, minimal_state):
        hook_called = []

        class HookAgent(ConcreteAgent):
            def on_complete(self, result):
                hook_called.append(result.status)

        agent  = HookAgent("hook_agent", "TESTING", context=sample_context)
        result = agent.execute(minimal_state)
        assert len(hook_called) == 1
        assert hook_called[0]   == AgentStatus.SUCCESS

    def test_on_start_exception_does_not_break_execution(
        self, sample_context, minimal_state
    ):
        class BrokenHookAgent(ConcreteAgent):
            def on_start(self, state):
                raise RuntimeError("hook failure")

        agent  = BrokenHookAgent("hook_agent", "TESTING", context=sample_context)
        result = agent.execute(minimal_state)
        # run() should still have been called despite on_start failing
        assert agent.run_called is True


# ══════════════════════════════════════════════════════════════════════════════
# LLMRouter
# ══════════════════════════════════════════════════════════════════════════════

class TestLLMRouter:
    """
    Tests for LLMRouter — all LLM model instantiation is mocked.
    No live API calls are made.
    """

    @pytest.fixture
    def mock_settings(self, tmp_path):
        """Write a minimal settings.yaml to a temp directory."""
        settings = {
            "llm_routing": {
                "intake_agent":         "gemini",
                "validation_agent":     "claude",
                "conversion_agent":     "gemini",
                "semantic_agent":       "claude",
                "metric_agent":         "claude",
                "tableau_model_agent":  "claude",
                "dashboard_agent":      "claude",
            },
            "llm_models": {
                "claude": {
                    "model_id":         "claude-sonnet-4-20250514",
                    "temperature":      0,
                    "max_tokens":       8192,
                    "timeout_seconds":  60,
                },
                "gemini": {
                    "model_id":         "gemini-2.5-flash",
                    "temperature":      0.1,
                    "max_tokens":       8192,
                    "timeout_seconds":  60,
                    "model_id_fallbacks": ["gemini-2.0-flash", "gemini-1.5-flash"],
                },
            },
        }
        import yaml
        settings_file = tmp_path / "settings.yaml"
        llm_file      = tmp_path / "llm_config.yaml"
        with open(settings_file, "w") as f:
            yaml.dump(settings, f)
        with open(llm_file, "w") as f:
            yaml.dump({}, f)
        return tmp_path

    @patch.dict(os.environ, {
        "ANTHROPIC_API_KEY": "sk-test-anthropic",
        "GOOGLE_API_KEY":    "test-google-key",
    })
    def test_routing_table_loaded(self, mock_settings):
        from agents.llm_router import LLMRouter
        router = LLMRouter(
            settings_path=mock_settings / "settings.yaml",
            llm_config_path=mock_settings / "llm_config.yaml",
        )
        table = router.routing_table()
        assert table["conversion_agent"]    == "gemini"
        assert table["semantic_agent"]      == "claude"
        assert table["tableau_model_agent"] == "claude"

    @patch.dict(os.environ, {
        "ANTHROPIC_API_KEY": "sk-test-anthropic",
        "GOOGLE_API_KEY":    "test-google-key",
    })
    def test_get_provider_claude(self, mock_settings):
        from agents.llm_router import LLMRouter
        router = LLMRouter(
            settings_path=mock_settings / "settings.yaml",
            llm_config_path=mock_settings / "llm_config.yaml",
        )
        assert router.get_provider("semantic_agent") == "claude"
        assert router.get_provider("metric_agent")   == "claude"

    @patch.dict(os.environ, {
        "ANTHROPIC_API_KEY": "sk-test-anthropic",
        "GOOGLE_API_KEY":    "test-google-key",
    })
    def test_get_provider_gemini(self, mock_settings):
        from agents.llm_router import LLMRouter
        router = LLMRouter(
            settings_path=mock_settings / "settings.yaml",
            llm_config_path=mock_settings / "llm_config.yaml",
        )
        assert router.get_provider("conversion_agent") == "gemini"
        assert router.get_provider("intake_agent")     == "gemini"

    @patch.dict(os.environ, {
        "ANTHROPIC_API_KEY": "sk-test-anthropic",
        "GOOGLE_API_KEY":    "test-google-key",
    })
    def test_get_provider_unknown_defaults_to_claude(self, mock_settings):
        from agents.llm_router import LLMRouter
        router = LLMRouter(
            settings_path=mock_settings / "settings.yaml",
            llm_config_path=mock_settings / "llm_config.yaml",
        )
        assert router.get_provider("nonexistent_agent") == "claude"

    @patch.dict(os.environ, {
        "ANTHROPIC_API_KEY": "sk-test-anthropic",
        "GOOGLE_API_KEY":    "test-google-key",
    })
    def test_get_model_name_claude(self, mock_settings):
        from agents.llm_router import LLMRouter
        router = LLMRouter(
            settings_path=mock_settings / "settings.yaml",
            llm_config_path=mock_settings / "llm_config.yaml",
        )
        name = router.get_model_name("semantic_agent")
        assert "claude" in name.lower()

    @patch.dict(os.environ, {
        "ANTHROPIC_API_KEY": "sk-test-anthropic",
        "GOOGLE_API_KEY":    "test-google-key",
    })
    def test_missing_anthropic_key_raises(self, mock_settings):
        from agents.llm_router import LLMRouter
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": ""}):
            router = LLMRouter(
                settings_path=mock_settings / "settings.yaml",
                llm_config_path=mock_settings / "llm_config.yaml",
            )
            with pytest.raises(EnvironmentError, match="ANTHROPIC_API_KEY"):
                router.get_llm("semantic_agent")

    @patch.dict(os.environ, {
        "ANTHROPIC_API_KEY": "sk-test-anthropic",
        "GOOGLE_API_KEY":    "test-google-key",
    })
    def test_missing_google_key_raises(self, mock_settings):
        from agents.llm_router import LLMRouter
        with patch.dict(os.environ, {"GOOGLE_API_KEY": ""}):
            router = LLMRouter(
                settings_path=mock_settings / "settings.yaml",
                llm_config_path=mock_settings / "llm_config.yaml",
            )
            with pytest.raises(EnvironmentError, match="GOOGLE_API_KEY"):
                router.get_llm("conversion_agent")

    @patch.dict(os.environ, {
        "ANTHROPIC_API_KEY": "sk-test-anthropic",
        "GOOGLE_API_KEY":    "test-google-key",
    })
    @patch("agents.llm_router.ChatAnthropic")
    def test_get_llm_returns_cached_instance(self, mock_claude_cls, mock_settings):
        """Same model instance returned on repeated calls (no re-instantiation)."""
        from agents.llm_router import LLMRouter
        mock_instance = MagicMock()
        mock_claude_cls.return_value = mock_instance

        router = LLMRouter(
            settings_path=mock_settings / "settings.yaml",
            llm_config_path=mock_settings / "llm_config.yaml",
        )
        llm1 = router.get_llm("semantic_agent")
        llm2 = router.get_llm("metric_agent")

        # Both claude agents return same cached object
        assert llm1 is llm2
        # Constructor called only once despite two get_llm() calls
        assert mock_claude_cls.call_count == 1

    @patch.dict(os.environ, {
        "ANTHROPIC_API_KEY": "sk-test-anthropic",
        "GOOGLE_API_KEY":    "test-google-key",
    })
    @patch("agents.llm_router.ChatAnthropic")
    def test_invoke_returns_response_text(self, mock_claude_cls, mock_settings):
        """invoke() returns string content from LLM response."""
        from agents.llm_router import LLMRouter
        mock_response         = MagicMock()
        mock_response.content = "Generated XML here"
        mock_instance         = MagicMock()
        mock_instance.invoke  = MagicMock(return_value=mock_response)
        mock_claude_cls.return_value = mock_instance

        router = LLMRouter(
            settings_path=mock_settings / "settings.yaml",
            llm_config_path=mock_settings / "llm_config.yaml",
        )
        text = router.invoke("semantic_agent", "Generate TDS XML for orders")
        assert text == "Generated XML here"

    @patch.dict(os.environ, {
        "ANTHROPIC_API_KEY": "sk-test-anthropic",
        "GOOGLE_API_KEY":    "test-google-key",
    })
    @patch("agents.llm_router.ChatAnthropic")
    def test_invoke_retries_on_failure(self, mock_claude_cls, mock_settings):
        """invoke() retries up to max_retries times before raising."""
        from agents.llm_router import LLMRouter
        mock_instance        = MagicMock()
        mock_instance.invoke = MagicMock(side_effect=ConnectionError("timeout"))
        mock_claude_cls.return_value = mock_instance

        router = LLMRouter(
            settings_path=mock_settings / "settings.yaml",
            llm_config_path=mock_settings / "llm_config.yaml",
        )
        with pytest.raises(RuntimeError, match="after 2 attempts"):
            router.invoke("semantic_agent", "test", max_retries=2)

        assert mock_instance.invoke.call_count == 2

    @patch.dict(os.environ, {
        "ANTHROPIC_API_KEY": "sk-test-anthropic",
        "GOOGLE_API_KEY":    "test-google-key",
    })
    def test_status_dict_structure(self, mock_settings):
        from agents.llm_router import LLMRouter
        router = LLMRouter(
            settings_path=mock_settings / "settings.yaml",
            llm_config_path=mock_settings / "llm_config.yaml",
        )
        s = router.status()
        assert "routing_table"      in s
        assert "cached_providers"   in s
        assert "anthropic_available" in s
        assert "google_available"   in s

    @patch.dict(os.environ, {
        "ANTHROPIC_API_KEY": "sk-test-anthropic",
        "GOOGLE_API_KEY":    "test-google-key",
    })
    def test_settings_file_not_found_raises(self, tmp_path):
        from agents.llm_router import LLMRouter
        with pytest.raises(FileNotFoundError):
            LLMRouter(settings_path=tmp_path / "nonexistent.yaml")
