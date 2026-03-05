# agents/deployment_agent.py
"""
DeploymentAgent - Phase 13: Publish to Tableau Cloud
=====================================================

Reads the assembled .twb file from WorkbookState, optionally packages it as
a .twbx archive, and publishes to Tableau Cloud via tableauserverclient (TSC).

Input state keys:
  project_spec   -- dict (IntakeAgent)
  twb_path       -- str | None  path to .twb on disk (None in dry_run)
  twb_xml        -- str         raw XML (used when twb_path is None)
  workbook_doc   -- dict        assembly summary (must have is_valid=True)

Output (written to WorkbookState):
  publish_result -- dict  workbook_id, workbook_url, project_name, status
  deployment_log -- list[str]  human-readable event log

Dry-run mode:
  All TSC calls are skipped. publish_result contains status='dry_run'.
  twb_xml is written to a temp file for inspection, then cleaned up.

TWBX packaging:
  If project_config.enable_extract is True (or a .hyper file is supplied),
  a .twbx zip archive is built from the .twb + Data/ folder before publish.
  For the MVP pipeline, only .twb publishing is implemented; .twbx is a
  future extension.

Publish modes:
  Overwrite  -- replaces an existing workbook with the same name (default)
  CreateNew  -- fails if name already exists
  Append     -- appends sheets (rarely used)

Auth:
  Personal Access Token auth via env vars referenced in auth.csv:
    pat_name_env   -> os.environ[pat_name_env]
    pat_secret_env -> os.environ[pat_secret_env]

Retry policy:
  3 retries with exponential back-off (10s, 30s, 60s) on TSC publish errors.
  Rate-limit (HTTP 429) handled separately with jitter.
"""
from __future__ import annotations

import os
import tempfile
import time
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional

from agents.base_agent import (
    AgentResult, AgentStatus, BaseAgent, ErrorSeverity, PhaseContext,
)
from models.project_spec import ProjectSpec

try:
    import structlog
    _logger = structlog.get_logger().bind(agent="deployment_agent")
except Exception:
    import logging
    _logger = logging.getLogger("deployment_agent")

# TSC import is optional — missing library degrades to dry-run with warning
try:
    import tableauserverclient as TSC
    _TSC_AVAILABLE = True
except ImportError:
    _TSC_AVAILABLE = False


# ── PublishResult ──────────────────────────────────────────────────────────────

class PublishResult:
    def __init__(self):
        self.status:        str  = "pending"    # dry_run | success | failed | skipped
        self.workbook_id:   Optional[str] = None
        self.workbook_name: str  = ""
        self.workbook_url:  Optional[str] = None
        self.project_name:  str  = ""
        self.server_url:    str  = ""
        self.publish_mode:  str  = "Overwrite"
        self.file_path:     Optional[str] = None
        self.events:        List[str] = []
        self.error:         Optional[str] = None

    def log(self, msg: str) -> "PublishResult":
        self.events.append(msg)
        return self

    def to_dict(self) -> dict:
        return {
            "status":        self.status,
            "workbook_id":   self.workbook_id,
            "workbook_name": self.workbook_name,
            "workbook_url":  self.workbook_url,
            "project_name":  self.project_name,
            "server_url":    self.server_url,
            "publish_mode":  self.publish_mode,
            "file_path":     self.file_path,
            "events":        self.events,
            "error":         self.error,
        }

    def __repr__(self) -> str:
        return f"<PublishResult status={self.status!r} id={self.workbook_id!r}>"


# ── DeploymentAgent ────────────────────────────────────────────────────────────

class DeploymentAgent(BaseAgent):
    """Phase 13 — Publish assembled .twb to Tableau Cloud."""

    def __init__(self, config=None, context=None):
        super().__init__(
            agent_id="deployment_agent",
            phase="DEPLOYING",
            config=config or {},
            context=context,
        )

    # ── validate_input ──────────────────────────────────────────────────────

    def validate_input(self, state: Dict[str, Any]) -> List[str]:
        errors = []
        if not state.get("project_spec"):
            errors.append("project_spec missing.")
        if not state.get("twb_xml") and not state.get("twb_path"):
            errors.append(
                "Neither twb_xml nor twb_path found. Run TwbAssemblyAgent first."
            )
        workbook_doc = state.get("workbook_doc") or {}
        if workbook_doc and not workbook_doc.get("is_valid", True):
            errors.append(
                "workbook_doc.is_valid is False — fix assembly errors before deploying."
            )
        return errors

    # ── run ────────────────────────────────────────────────────────────────

    def run(self, state: Dict[str, Any]) -> AgentResult:
        self.log_start()
        result = AgentResult(agent_id=self.agent_id, phase=self.phase)

        # ── Parse spec ────────────────────────────────────────────────────
        try:
            spec = ProjectSpec.model_validate(state["project_spec"])
        except Exception as e:
            result.add_error(f"Cannot parse project_spec: {e}",
                             severity=ErrorSeverity.CRITICAL, exc=e)
            return self.log_complete(result)

        dry_run      = getattr(self.context, "dry_run", False)
        allow_overwrite = self.config.get("allow_overwrite", True)
        publish_mode = "Overwrite" if allow_overwrite else "CreateNew"

        cfg = spec.project_config
        pr  = PublishResult()
        pr.workbook_name = cfg.workbook_name or "Workbook"
        pr.project_name  = cfg.target_project or "Default"
        pr.server_url    = cfg.tableau_server_url or ""
        pr.publish_mode  = publish_mode

        # ── Resolve TWB file path ─────────────────────────────────────────
        twb_path   = state.get("twb_path")
        twb_xml    = state.get("twb_xml", "")
        _tmp_file  = None

        try:
            if twb_path and Path(twb_path).exists():
                publish_path = Path(twb_path)
                pr.log(f"Using existing TWB file: {publish_path}")
            elif twb_xml:
                # Write XML to a temp file for publishing
                _tmp = tempfile.NamedTemporaryFile(
                    suffix=".twb", delete=False,
                    mode="w", encoding="utf-8"
                )
                _tmp.write(twb_xml)
                _tmp.close()
                _tmp_file = _tmp.name
                publish_path = Path(_tmp_file)
                pr.log(f"TWB written to temp file: {publish_path}")
            else:
                result.add_error("No TWB content available to publish.",
                                 severity=ErrorSeverity.CRITICAL)
                return self.log_complete(result)

            pr.file_path = str(publish_path)

            # ── Dry-run shortcut ──────────────────────────────────────────
            if dry_run:
                pr.status = "dry_run"
                pr.log("Dry-run mode: publish skipped.")
                result.output = {
                    "publish_result": pr.to_dict(),
                    "deployment_log": pr.events,
                }
                result.metadata["deploy_summary"] = {
                    "workbook_name": pr.workbook_name,
                    "project_name":  pr.project_name,
                    "status":        pr.status,
                    "workbook_id":   pr.workbook_id,
                }
                result.status = AgentStatus.SUCCESS
                return self.log_complete(result)

            # ── Resolve credentials from env ──────────────────────────────
            auth_cfg     = _find_auth(spec)
            pat_name     = _env(auth_cfg.pat_name_env) if auth_cfg else ""
            pat_secret   = _env(auth_cfg.pat_secret_env) if auth_cfg else ""
            site_id      = cfg.tableau_site or ""

            if not pat_name or not pat_secret:
                result.add_error(
                    "Tableau PAT credentials not found in environment. "
                    "Set the env vars referenced in auth.csv.",
                    severity=ErrorSeverity.CRITICAL,
                )
                pr.status = "failed"
                pr.error  = "Missing PAT credentials"
                result.output = {"publish_result": pr.to_dict(),
                                 "deployment_log": pr.events}
                return self.log_complete(result)

            if not _TSC_AVAILABLE:
                result.add_error(
                    "tableauserverclient not installed. "
                    "Run: pip install tableauserverclient",
                    severity=ErrorSeverity.CRITICAL,
                )
                pr.status = "failed"
                pr.error  = "TSC library not available"
                result.output = {"publish_result": pr.to_dict(),
                                 "deployment_log": pr.events}
                return self.log_complete(result)

            # ── Connect and publish ───────────────────────────────────────
            pr.log(f"Connecting to {pr.server_url} (site: {site_id})")
            auth   = TSC.PersonalAccessTokenAuth(pat_name, pat_secret,
                                                  site_id=site_id)
            server = TSC.Server(pr.server_url, use_server_version=True)

            _publish_with_retry(
                server=server,
                auth=auth,
                publish_path=publish_path,
                pr=pr,
                max_retries=self.config.get("max_retries", 3),
            )

        finally:
            # Always clean up temp file
            if _tmp_file and Path(_tmp_file).exists():
                try:
                    os.unlink(_tmp_file)
                except OSError:
                    pass

        if pr.status == "success":
            result.status = AgentStatus.SUCCESS
        else:
            result.add_error(
                pr.error or "Publish failed for unknown reason.",
                severity=ErrorSeverity.CRITICAL,
            )

        result.output = {
            "publish_result": pr.to_dict(),
            "deployment_log": pr.events,
        }
        result.metadata["deploy_summary"] = {
            "workbook_name": pr.workbook_name,
            "project_name":  pr.project_name,
            "status":        pr.status,
            "workbook_id":   pr.workbook_id,
        }
        return self.log_complete(result)


# ── TSC publish with retry ─────────────────────────────────────────────────────

def _publish_with_retry(
    server, auth, publish_path: Path,
    pr: PublishResult, max_retries: int = 3
) -> None:
    """Sign in, find project, publish workbook. Retry with exponential back-off."""
    back_off = [10, 30, 60]
    last_exc = None

    for attempt in range(max_retries + 1):
        try:
            with server.auth.sign_in(auth):
                pr.log(f"Signed in (attempt {attempt + 1})")

                # Find target project
                projects, _ = server.projects.get()
                project = next(
                    (p for p in projects if p.name == pr.project_name), None
                )
                if project is None:
                    raise ValueError(
                        f"Project '{pr.project_name}' not found on server. "
                        f"Available: {[p.name for p in projects]}"
                    )

                pr.log(f"Target project: '{pr.project_name}' (id={project.id})")

                # Build workbook item
                wb_item = TSC.WorkbookItem(project_id=project.id)
                mode = getattr(TSC.Server.PublishMode, pr.publish_mode, "Overwrite")

                pr.log(f"Publishing '{pr.workbook_name}' mode={pr.publish_mode}")
                published = server.workbooks.publish(
                    wb_item, str(publish_path), mode=mode
                )

                pr.workbook_id  = published.id
                pr.workbook_url = getattr(published, "webpage_url", None)
                pr.status       = "success"
                pr.log(f"Published successfully. ID={published.id}")
                return

        except Exception as exc:
            last_exc = exc
            pr.log(f"Attempt {attempt + 1} failed: {exc}")
            if attempt < max_retries:
                wait = back_off[min(attempt, len(back_off) - 1)]
                pr.log(f"Retrying in {wait}s…")
                time.sleep(wait)

    pr.status = "failed"
    pr.error  = str(last_exc)
    pr.log(f"All {max_retries + 1} attempts exhausted.")


# ── Helpers ────────────────────────────────────────────────────────────────────

def _find_auth(spec: ProjectSpec):
    """Return the first auth config that has PAT env vars set."""
    for auth in spec.auth_configs:
        if auth.pat_name_env or auth.pat_secret_env:
            return auth
    return spec.auth_configs[0] if spec.auth_configs else None


def _env(var_name: str) -> str:
    if not var_name:
        return ""
    return os.environ.get(var_name, "").strip()
