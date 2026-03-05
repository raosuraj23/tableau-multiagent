"""
Deployment Agent
================
Phase 4 (final) of the pipeline.

Responsibilities:
- Package the generated .twb file into a .twbx archive
- Authenticate to Tableau Cloud using a Personal Access Token
- Publish the workbook via tableauserverclient (TSC)
- Handle publish modes (CreateNew / Overwrite)
- Return the published workbook URL and ID
"""

from __future__ import annotations

import os
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional

from agents.base_agent import AgentResult, BaseAgent


class DeploymentAgent(BaseAgent):
    """Packages the TWB as TWBX and publishes to Tableau Cloud via TSC."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__("deployment_agent", config)
        self.allow_overwrite: bool = config.get("allow_overwrite", False)

    # ──────────────────────────────────────────
    # BaseAgent interface
    # ──────────────────────────────────────────

    def validate_input(self, state: Dict[str, Any]) -> List[str]:
        errors: List[str] = []
        if not state.get("twb_path"):
            errors.append("twb_path missing — run TwbGeneratorAgent first")
        spec = state.get("project_spec", {})
        for key in ("tableau_server_url", "tableau_site", "target_project"):
            if not spec.get(key):
                errors.append(f"project_spec.{key} is required for deployment")

        pat_name = os.environ.get("TAB_PAT_NAME")
        pat_secret = os.environ.get("TAB_PAT_SECRET")
        if not pat_name or not pat_secret:
            errors.append(
                "TAB_PAT_NAME and TAB_PAT_SECRET environment variables must be set"
            )
        return errors

    def run(self, state: Dict[str, Any]) -> AgentResult:
        spec: Dict[str, Any] = state["project_spec"]
        twb_path = Path(state["twb_path"])
        warnings: List[str] = []

        pat_name = os.environ["TAB_PAT_NAME"]
        pat_secret = os.environ["TAB_PAT_SECRET"]

        # ── Step 1: Package as TWBX ──
        twbx_path, pkg_errors = self._package_twbx(twb_path)
        if pkg_errors:
            return AgentResult(
                agent_id=self.agent_id,
                phase="deployment",
                status="failed",
                errors=pkg_errors,
            )

        # ── Step 2: Publish to Tableau Cloud ──
        result, pub_errors = self._publish(
            twbx_path=twbx_path,
            server_url=spec["tableau_server_url"],
            site_id=spec["tableau_site"],
            project_name=spec["target_project"],
            pat_name=pat_name,
            pat_secret=pat_secret,
        )

        # Clean up temp TWBX
        try:
            twbx_path.unlink()
        except OSError:
            pass

        if pub_errors:
            return AgentResult(
                agent_id=self.agent_id,
                phase="deployment",
                status="failed",
                errors=pub_errors,
                warnings=warnings,
            )

        self.logger.info(
            "workbook_published",
            workbook_id=result.get("workbook_id"),
            url=result.get("url"),
        )
        return AgentResult(
            agent_id=self.agent_id,
            phase="deployment",
            status="success",
            output={"publish_result": result},
            warnings=warnings,
        )

    # ──────────────────────────────────────────
    # TWBX packaging
    # ──────────────────────────────────────────

    def _package_twbx(
        self, twb_path: Path
    ) -> tuple[Path, List[str]]:
        """Wrap the .twb in a .twbx ZIP archive (no embedded extracts)."""
        errors: List[str] = []
        if not twb_path.exists():
            return twb_path, [f"TWB file not found: {twb_path}"]

        twbx_path = twb_path.with_suffix(".twbx")
        try:
            with zipfile.ZipFile(twbx_path, "w", zipfile.ZIP_DEFLATED) as zf:
                zf.write(twb_path, arcname=twb_path.name)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"Failed to package TWBX: {exc}")
            return twb_path, errors

        return twbx_path, errors

    # ──────────────────────────────────────────
    # Tableau Cloud publish via TSC
    # ──────────────────────────────────────────

    def _publish(
        self,
        twbx_path: Path,
        server_url: str,
        site_id: str,
        project_name: str,
        pat_name: str,
        pat_secret: str,
    ) -> tuple[Dict[str, Any], List[str]]:
        errors: List[str] = []

        try:
            import tableauserverclient as TSC
        except ImportError:
            return {}, ["tableauserverclient not installed — run pip install tableauserverclient"]

        auth = TSC.PersonalAccessTokenAuth(pat_name, pat_secret, site_id=site_id)
        server = TSC.Server(server_url, use_server_version=True)

        try:
            with server.auth.sign_in(auth):
                # Find target project
                projects, _ = server.projects.get()
                target = next(
                    (p for p in projects if p.name == project_name), None
                )
                if not target:
                    errors.append(
                        f"Project '{project_name}' not found on Tableau Cloud. "
                        f"Available: {[p.name for p in projects]}"
                    )
                    return {}, errors

                # Determine publish mode
                if self.allow_overwrite:
                    mode = TSC.Server.PublishMode.Overwrite
                else:
                    mode = TSC.Server.PublishMode.CreateNew

                wb_item = TSC.WorkbookItem(project_id=target.id)
                wb_item = server.workbooks.publish(
                    wb_item,
                    str(twbx_path),
                    mode=mode,
                )

                url = (
                    f"{server_url}/#/{site_id}/workbooks/{wb_item.id}/views"
                )
                return {
                    "workbook_id": wb_item.id,
                    "workbook_name": wb_item.name,
                    "project": project_name,
                    "url": url,
                    "publish_mode": mode,
                }, errors

        except Exception as exc:  # noqa: BLE001
            errors.append(f"Tableau Cloud publish failed: {exc}")
            return {}, errors
