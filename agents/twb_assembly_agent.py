# agents/twb_assembly_agent.py
"""
TwbAssemblyAgent - Phase 10/11: Tableau Workbook XML Assembly
=============================================================

Reads all XML fragments from WorkbookState and produces a single valid
.twb file. This is the final XML-generation step before the DeploymentAgent
publishes to Tableau Cloud.

Input state keys:
  project_spec    -- dict (IntakeAgent)
  tds_documents   -- list[dict] (SemanticModelAgent)
  worksheets_xml  -- list[dict] (TableauModelAgent)
  dashboards_xml  -- list[dict] (DashboardGenAgent)

Output (written to WorkbookState):
  twb_path        -- str path to the written .twb file
  twb_xml         -- full XML string (for downstream inspection/testing)
  workbook_doc    -- WorkbookDocument.to_dict() summary

File written to: tableau/output/<WorkbookName>.twb
In dry_run mode: twb_xml is populated but file is NOT written.

Gate condition for DeploymentAgent (Phase 13):
  workbook_doc["is_valid"] == True
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from agents.base_agent import (
    AgentResult, AgentStatus, BaseAgent, ErrorSeverity, PhaseContext,
)
from models.project_spec import ProjectSpec
from models.workbook_spec import WorkbookDocument

try:
    import structlog
    _logger = structlog.get_logger().bind(agent="twb_assembly_agent")
except Exception:
    import logging
    _logger = logging.getLogger("twb_assembly_agent")

DEFAULT_OUTPUT_DIR = Path("tableau/output")


class TwbAssemblyAgent(BaseAgent):
    """Phase 10/11 - TWB XML Assembly."""

    def __init__(self, config=None, context=None):
        super().__init__(
            agent_id="twb_assembly_agent",
            phase="GENERATING",
            config=config or {},
            context=context,
        )

    def validate_input(self, state: Dict[str, Any]) -> List[str]:
        errors = []
        if not state.get("project_spec"):
            errors.append("project_spec missing.")
        if not state.get("tds_documents"):
            errors.append("tds_documents missing. Run SemanticModelAgent first.")
        if not state.get("worksheets_xml"):
            errors.append("worksheets_xml missing. Run TableauModelAgent first.")
        if not state.get("dashboards_xml"):
            errors.append("dashboards_xml missing. Run DashboardGenAgent first.")
        return errors

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

        dry_run   = getattr(self.context, "dry_run", False)
        wb_name   = spec.project_config.workbook_name or "Workbook"
        out_dir   = Path(self.config.get("output_dir", DEFAULT_OUTPUT_DIR))

        # ── Collect XML fragments ─────────────────────────────────────────
        tds_xml_list    = _extract_xml_list(state.get("tds_documents", []))
        ws_xml_list     = _extract_xml_list(state.get("worksheets_xml", []))
        db_xml_list     = _extract_xml_list(state.get("dashboards_xml", []))
        palette_xml_list: List[str] = []  # Phase 10 Figma tokens (future)

        if not tds_xml_list:
            result.add_error("No TDS XML fragments available.",
                             severity=ErrorSeverity.CRITICAL)
            return self.log_complete(result)

        # ── Build workbook document ───────────────────────────────────────
        doc = WorkbookDocument(
            name=wb_name,
            tds_xml_list=tds_xml_list,
            worksheet_xml_list=ws_xml_list,
            dashboard_xml_list=db_xml_list,
            palette_xml_list=palette_xml_list,
        )

        try:
            doc.build()
        except Exception as e:
            result.add_error(f"TWB assembly failed: {e}",
                             severity=ErrorSeverity.CRITICAL, exc=e)
            return self.log_complete(result)

        # ── Validate ──────────────────────────────────────────────────────
        validation = doc.validate()
        for err in validation.errors:
            result.add_error(err, severity=ErrorSeverity.CRITICAL)
        for warn in validation.warnings:
            result.add_warning(warn)

        # ── Write file ────────────────────────────────────────────────────
        if not dry_run and not result.has_blocking_errors():
            try:
                twb_path = doc.write(out_dir, dry_run=False)
                try:
                    _logger.info("twb_written", path=str(twb_path),
                                 size=len(doc.twb_xml))
                except Exception:
                    pass
            except Exception as e:
                result.add_error(f"Failed to write TWB file: {e}",
                                 severity=ErrorSeverity.HIGH, exc=e)

        result.output = {
            "twb_xml":      doc.twb_xml,
            "twb_path":     str(doc.twb_path) if doc.twb_path else None,
            "workbook_doc": doc.to_dict(),
        }
        result.metadata["assembly_summary"] = {
            "workbook_name":    wb_name,
            "datasources":      len(tds_xml_list),
            "worksheets":       len(ws_xml_list),
            "dashboards":       len(db_xml_list),
            "xml_length":       len(doc.twb_xml or ""),
            "is_valid":         doc.is_valid,
            "dry_run":          dry_run,
        }

        if result.status == AgentStatus.PENDING:
            result.status = AgentStatus.SUCCESS

        return self.log_complete(result)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _extract_xml_list(docs: List[dict]) -> List[str]:
    """Extract 'xml' field from a list of to_dict() results."""
    return [d["xml"] for d in docs if isinstance(d, dict) and d.get("xml")]
