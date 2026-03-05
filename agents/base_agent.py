# agents/base_agent.py
"""
BaseAgent — Abstract Base Class for All Tableau Multi-Agent System Agents
==========================================================================

Every agent in the system inherits from BaseAgent and implements:
  - run(state)           → executes the agent's core logic
  - validate_input(state) → checks preconditions before run()

AgentResult is the standard return type for every agent.
PhaseContext carries read-only metadata about the current workflow phase.

Structured logging via structlog produces JSON log entries that are
audit-ready and compatible with any log aggregation system.

Usage:
    class MyAgent(BaseAgent):
        def validate_input(self, state):
            errors = []
            if not state.get("required_field"):
                errors.append("required_field is missing")
            return errors

        def run(self, state):
            self.log_start()
            result = AgentResult(agent_id=self.agent_id, phase=self.phase)
            try:
                # ... do work ...
                result.output = {"key": "value"}
                result.status = AgentStatus.SUCCESS
            except Exception as e:
                result.add_error(str(e))
            return self.log_complete(result)
"""

from __future__ import annotations

import time
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass, field as dc_field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

import structlog

# ── Logging configuration ──────────────────────────────────────────────────────

def _configure_structlog() -> None:
    """Configure structlog with JSON output for Python 3.12+ / structlog 24+."""
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.ExceptionRenderer(),
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

_configure_structlog()

# ── Enums ──────────────────────────────────────────────────────────────────────

class AgentStatus(str, Enum):
    """Standard status values for AgentResult."""
    SUCCESS  = "success"   # Agent completed without errors
    WARNING  = "warning"   # Agent completed with non-blocking warnings
    FAILED   = "failed"    # Agent encountered a blocking error
    SKIPPED  = "skipped"   # Agent was intentionally skipped (precondition not met)
    PENDING  = "pending"   # Agent has not yet run (initial state)


class ErrorSeverity(str, Enum):
    """Severity levels for errors and warnings — mirrors the test framework."""
    CRITICAL = "CRITICAL"  # Blocks deployment
    HIGH     = "HIGH"      # Blocks with manual override
    MEDIUM   = "MEDIUM"    # Warning only
    LOW      = "LOW"       # Informational


# ── AgentError ─────────────────────────────────────────────────────────────────

@dataclass
class AgentError:
    """
    A structured error or warning recorded during agent execution.
    Tracks severity, optional field/phase context, and whether it blocks the workflow.
    """
    message:   str
    severity:  ErrorSeverity = ErrorSeverity.HIGH
    field:     Optional[str] = None        # Which CSV field or XML element
    phase:     Optional[str] = None        # Which workflow phase
    exception: Optional[str] = None        # Stringified exception if applicable
    timestamp: str = dc_field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    @property
    def is_blocking(self) -> bool:
        """CRITICAL and HIGH errors block workflow progression."""
        return self.severity in (ErrorSeverity.CRITICAL, ErrorSeverity.HIGH)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "message":   self.message,
            "severity":  self.severity.value,
            "field":     self.field,
            "phase":     self.phase,
            "exception": self.exception,
            "timestamp": self.timestamp,
        }


# ── PhaseContext ───────────────────────────────────────────────────────────────

@dataclass
class PhaseContext:
    """
    Read-only context injected into each agent by the orchestrator.
    Carries run-level metadata used in logging and audit trails.
    """
    project_id:     str
    run_id:         str
    environment:    str = "dev"
    dry_run:        bool = False
    allow_overwrite: bool = False
    correlation_id: str = ""

    def __post_init__(self) -> None:
        if not self.correlation_id:
            self.correlation_id = f"{self.project_id}_{self.run_id}"


# ── AgentResult ───────────────────────────────────────────────────────────────

@dataclass
class AgentResult:
    """
    Standard return type for every agent's run() method.

    The orchestrator reads:
      - status        → route the DAG (continue / retry / fail)
      - output        → pass to next agent via WorkbookState
      - errors        → accumulate in WorkbookState.errors
      - warnings      → log but don't block
      - duration_ms   → performance observability

    Example:
        result = AgentResult(agent_id="intake_agent", phase="INTAKE")
        result.output = {"project_spec": {...}}
        result.status = AgentStatus.SUCCESS
    """
    agent_id:     str
    phase:        str
    status:       AgentStatus             = AgentStatus.PENDING
    output:       Dict[str, Any]          = dc_field(default_factory=dict)
    errors:       List[AgentError]        = dc_field(default_factory=list)
    warnings:     List[AgentError]        = dc_field(default_factory=list)
    duration_ms:  float                   = 0.0
    timestamp:    str                     = dc_field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    metadata:     Dict[str, Any]          = dc_field(default_factory=dict)

    # ── Convenience helpers ──────────────────────────────────────────────────

    def add_error(
        self,
        message: str,
        severity: ErrorSeverity = ErrorSeverity.HIGH,
        field: Optional[str] = None,
        exc: Optional[Exception] = None,
    ) -> "AgentResult":
        """Add a structured error. Automatically sets status to FAILED for blocking errors."""
        err = AgentError(
            message=message,
            severity=severity,
            field=field,
            phase=self.phase,
            exception=traceback.format_exc() if exc else None,
        )
        self.errors.append(err)
        if err.is_blocking:
            self.status = AgentStatus.FAILED
        return self  # fluent

    def add_warning(
        self,
        message: str,
        field: Optional[str] = None,
    ) -> "AgentResult":
        """Add a non-blocking warning. Sets status to WARNING if currently SUCCESS."""
        warn = AgentError(
            message=message,
            severity=ErrorSeverity.MEDIUM,
            field=field,
            phase=self.phase,
        )
        self.warnings.append(warn)
        if self.status == AgentStatus.SUCCESS:
            self.status = AgentStatus.WARNING
        return self  # fluent

    @property
    def has_blocking_errors(self) -> bool:
        return any(e.is_blocking for e in self.errors)

    @property
    def error_count(self) -> int:
        return len(self.errors)

    @property
    def warning_count(self) -> int:
        return len(self.warnings)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "agent_id":    self.agent_id,
            "phase":       self.phase,
            "status":      self.status.value,
            "duration_ms": round(self.duration_ms, 2),
            "timestamp":   self.timestamp,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "errors":      [e.to_dict() for e in self.errors],
            "warnings":    [w.to_dict() for w in self.warnings],
            "output_keys": list(self.output.keys()),
            "metadata":    self.metadata,
        }

    def summary(self) -> str:
        """One-line human-readable summary for CLI output."""
        icon = {
            AgentStatus.SUCCESS: "✓",
            AgentStatus.WARNING: "⚠",
            AgentStatus.FAILED:  "✗",
            AgentStatus.SKIPPED: "–",
            AgentStatus.PENDING: "○",
        }.get(self.status, "?")
        return (
            f"{icon} [{self.agent_id}] {self.status.value.upper()} "
            f"({self.duration_ms:.0f}ms) "
            f"| {self.error_count} error(s), {self.warning_count} warning(s)"
        )


# ── BaseAgent ─────────────────────────────────────────────────────────────────

class BaseAgent(ABC):
    """
    Abstract base class for all agents in the Tableau Multi-Agent System.

    Subclasses must implement:
        validate_input(state) → List[str]   (return empty list if valid)
        run(state)            → AgentResult

    Subclasses optionally override:
        on_start(state)   → called before run(), after validate_input()
        on_complete(result) → called after run() completes or fails

    The base class provides:
        - Structured JSON logging (self.logger)
        - Timing via log_start() / log_complete()
        - Input validation gate (runs before run())
        - Safe execution wrapper (catches unhandled exceptions)
        - Phase context injection (self.context)
    """

    def __init__(
        self,
        agent_id: str,
        phase: str,
        config: Optional[Dict[str, Any]] = None,
        context: Optional[PhaseContext] = None,
    ) -> None:
        self.agent_id = agent_id
        self.phase    = phase
        self.config   = config or {}
        self.context  = context
        self._start_time: float = 0.0

        # Bind structured logger with agent identity
        self.logger = structlog.get_logger().bind(
            agent=agent_id,
            phase=phase,
            project_id=context.project_id if context else "unknown",
            run_id=context.run_id if context else "unknown",
            correlation_id=context.correlation_id if context else "",
        )

    # ── Abstract interface ─────────────────────────────────────────────────

    @abstractmethod
    def validate_input(self, state: Dict[str, Any]) -> List[str]:
        """
        Validate preconditions before running the agent.

        Returns:
            List of error message strings. Empty list means valid.
            These errors are added as CRITICAL AgentErrors and prevent run().
        """
        ...

    @abstractmethod
    def run(self, state: Dict[str, Any]) -> AgentResult:
        """
        Execute the agent's core logic.

        Args:
            state: The full LangGraph WorkbookState dict.

        Returns:
            AgentResult with status, output, errors, and warnings.
        """
        ...

    # ── Optional hooks ─────────────────────────────────────────────────────

    def on_start(self, state: Dict[str, Any]) -> None:
        """Hook called after validation passes, before run(). Override as needed."""
        pass

    def on_complete(self, result: AgentResult) -> None:
        """Hook called after run() completes (success or failure). Override as needed."""
        pass

    # ── Safe execution wrapper ─────────────────────────────────────────────

    def execute(self, state: Dict[str, Any]) -> AgentResult:
        """
        Full agent execution pipeline with validation, timing, and error safety.

        Orchestrator calls execute(), NOT run() directly.
        This ensures validation always runs and timing is always captured.

        Pipeline:
            1. validate_input()  → if errors: return FAILED result immediately
            2. on_start()
            3. run()             → if unhandled exception: catch and return FAILED
            4. on_complete()
            5. log result
        """
        result = AgentResult(agent_id=self.agent_id, phase=self.phase)
        self._start_time = time.perf_counter()

        # Step 1: validate_input
        try:
            validation_errors = self.validate_input(state)
        except Exception as e:
            validation_errors = [f"validate_input() raised exception: {e}"]

        if validation_errors:
            for msg in validation_errors:
                result.add_error(
                    message=msg,
                    severity=ErrorSeverity.CRITICAL,
                )
            result.status = AgentStatus.FAILED
            result.duration_ms = self._elapsed_ms()
            self._log_result(result, validation_failed=True)
            return result

        # Step 2: on_start hook
        try:
            self.on_start(state)
        except Exception as e:
            self.logger.warning("on_start_hook_failed", error=str(e))

        # Step 3: run()
        try:
            result = self.run(state)
            # Ensure agent_id and phase are always set (in case subclass forgets)
            result.agent_id = self.agent_id
            result.phase    = self.phase
        except Exception as e:
            result.add_error(
                message=f"Unhandled exception in run(): {e}",
                severity=ErrorSeverity.CRITICAL,
                exc=e,
            )
            result.status     = AgentStatus.FAILED
            result.metadata["traceback"] = traceback.format_exc()
            self.logger.exception("agent_run_unhandled_exception",
                                  error=str(e),
                                  agent=self.agent_id)

        # Step 4: timing
        result.duration_ms = self._elapsed_ms()

        # Step 5: on_complete hook
        try:
            self.on_complete(result)
        except Exception as e:
            self.logger.warning("on_complete_hook_failed", error=str(e))

        # Step 6: log result
        self._log_result(result)

        return result

    # ── Logging helpers ────────────────────────────────────────────────────

    def log_start(self) -> None:
        """Call at the beginning of run() for explicit start logging."""
        self._start_time = self._start_time or time.perf_counter()
        self.logger.info(
            "agent_start",
            dry_run=self.context.dry_run if self.context else False,
        )

    def log_complete(self, result: AgentResult) -> AgentResult:
        """
        Call at the end of run() to log completion and return result.
        Convenience pattern: return self.log_complete(result)
        """
        result.duration_ms = self._elapsed_ms()
        self._log_result(result)
        return result

    def _log_result(
        self,
        result: AgentResult,
        validation_failed: bool = False,
    ) -> None:
        """Internal: emit structured log entry for agent completion."""
        event = "agent_validation_failed" if validation_failed else "agent_complete"
        log_kw = {
            "status":        result.status.value,
            "duration_ms":   round(result.duration_ms, 2),
            "error_count":   result.error_count,
            "warning_count": result.warning_count,
            "output_keys":   list(result.output.keys()),
        }
        if result.status == AgentStatus.SUCCESS:
            self.logger.info(event, **log_kw)
        elif result.status == AgentStatus.WARNING:
            self.logger.warning(event, warnings=[w.message for w in result.warnings],
                                **log_kw)
        else:
            self.logger.error(event, errors=[e.message for e in result.errors],
                              **log_kw)

    def _elapsed_ms(self) -> float:
        """Returns milliseconds since log_start() or execute() was called."""
        if self._start_time == 0.0:
            return 0.0
        return (time.perf_counter() - self._start_time) * 1000

    # ── State helpers ──────────────────────────────────────────────────────

    @staticmethod
    def get_required(state: Dict[str, Any], key: str) -> Any:
        """
        Get a required field from state.
        Raises KeyError with clear message if missing.
        Use inside validate_input() to check preconditions.
        """
        if key not in state or state[key] is None:
            raise KeyError(f"Required state field '{key}' is missing or None")
        return state[key]

    @staticmethod
    def get_optional(
        state: Dict[str, Any],
        key: str,
        default: Any = None,
    ) -> Any:
        """Get an optional field from state with a default."""
        return state.get(key, default)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(agent_id={self.agent_id!r}, phase={self.phase!r})"
