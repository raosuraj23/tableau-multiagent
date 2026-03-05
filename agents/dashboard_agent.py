# agents/dashboard_agent.py
"""
DashboardGenAgent — Phase 09: Dashboard XML Generation
=======================================================

Translates every dashboard row in dashboard_requirements.csv into a
valid Tableau <dashboard> XML block using tiled zone grids.

Input state keys expected:
  project_spec    — dict from IntakeAgent
  worksheets_xml  — list[dict] from TableauModelAgent (Phase 08)
  design_tokens   — optional dict from FigmaDesignAgent (layout tokens)

Output (written to WorkbookState):
  dashboards_xml  — list[dict], one DashboardDocument.to_dict() per dashboard

Gate condition for Phase 10 (TWB Assembler):
  All DashboardDocument.is_valid == True

Processing pipeline (per dashboard):
  1. Read views_in_dashboard → ordered sheet name list
  2. Validate all referenced worksheets exist in worksheets_xml
  3. If design_tokens present → try figma zone layout
  4. Otherwise → compute grid layout from dashboard_layout field
  5. Build filter field references from filter_fields column
  6. Emit DashboardDocument → to_xml()

Error handling:
  - Missing worksheets: add HIGH error, still build dashboard with available sheets
  - Unknown layout type: fallback to grid-2x2
  - Invalid figma tokens: fallback to computed grid
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from agents.base_agent import (
    AgentResult,
    AgentStatus,
    BaseAgent,
    ErrorSeverity,
    PhaseContext,
)
from models.project_spec import ProjectSpec
from models.dashboard_spec import (
    DashboardDocument,
    ZoneSpec,
    GRID_LAYOUTS,
    build_figma_zones,
    build_grid_zones,
)

try:
    import structlog
    _logger = structlog.get_logger().bind(agent="dashboard_agent")
except Exception:
    import logging
    _logger = logging.getLogger("dashboard_agent")


# ── DashboardGenAgent ──────────────────────────────────────────────────────────

class DashboardGenAgent(BaseAgent):
    """Phase 09 — Dashboard XML Generation."""

    def __init__(
        self,
        config:  Optional[Dict[str, Any]] = None,
        context: Optional[PhaseContext]   = None,
    ) -> None:
        super().__init__(
            agent_id="dashboard_agent",
            phase="GENERATING",
            config=config or {},
            context=context,
        )

    # ── validate_input ──────────────────────────────────────────────────────

    def validate_input(self, state: Dict[str, Any]) -> List[str]:
        errors: List[str] = []
        if not state.get("project_spec"):
            errors.append("project_spec missing. Run IntakeAgent first.")
        if "worksheets_xml" not in state:
            errors.append(
                "worksheets_xml missing. Run TableauModelAgent (Phase 08) first."
            )
        return errors

    # ── run ────────────────────────────────────────────────────────────────

    def run(self, state: Dict[str, Any]) -> AgentResult:
        self.log_start()
        result = AgentResult(agent_id=self.agent_id, phase=self.phase)

        # ── Parse inputs ───────────────────────────────────────────────────
        try:
            spec: ProjectSpec = ProjectSpec.model_validate(state["project_spec"])
        except Exception as e:
            result.add_error(f"Cannot parse project_spec: {e}",
                             severity=ErrorSeverity.CRITICAL, exc=e)
            return self.log_complete(result)

        worksheets_xml: List[dict] = state.get("worksheets_xml", [])
        design_tokens:  Optional[dict] = state.get("design_tokens")

        # Build worksheet name → dict index for fast lookup
        ws_by_name: Dict[str, dict] = {
            ws["name"]: ws for ws in worksheets_xml
        }

        # ── Process each dashboard ─────────────────────────────────────────
        dashboards_xml: List[dict] = []

        for dash_req in spec.get_dashboards():
            view_ids  = dash_req.dashboard_view_ids           # ordered view_id list
            ws_names  = _resolve_sheet_names(view_ids, ws_by_name, spec)
            layout    = (dash_req.dashboard_layout or "grid-2x2").strip()
            width_px  = int(dash_req.width_px  or 1366)
            height_px = int(dash_req.height_px or 768)

            # Warn about missing referenced worksheets
            for vid in view_ids:
                matched = _find_ws_name_for_view_id(vid, spec, ws_by_name)
                if matched is None:
                    result.add_error(
                        f"Dashboard '{dash_req.view_name}' references view_id '{vid}' "
                        f"which has no matching worksheet XML. Skipping that slot.",
                        severity=ErrorSeverity.HIGH,
                    )

            available_names = [n for n in ws_names if n is not None]

            # ── Build zone layout ──────────────────────────────────────────
            if design_tokens and design_tokens.get("layouts"):
                figma_toks = design_tokens.get("layouts", [])
                zones = build_figma_zones(available_names, figma_toks)
            else:
                safe_layout = layout if layout in GRID_LAYOUTS else "grid-2x2"
                if layout not in GRID_LAYOUTS:
                    result.add_error(
                        f"Unknown dashboard layout '{layout}' for "
                        f"'{dash_req.view_name}'. Falling back to grid-2x2.",
                        severity=ErrorSeverity.LOW,
                    )
                zones = build_grid_zones(available_names, safe_layout)

            # ── Build DashboardDocument ────────────────────────────────────
            doc = DashboardDocument(
                name=dash_req.view_name,
                layout=layout,
                width_px=width_px,
                height_px=height_px,
                view_id=dash_req.view_id,
                zones=zones,
            )

            if not doc.is_valid:
                result.add_error(
                    f"Dashboard '{dash_req.view_name}' is invalid after generation.",
                    severity=ErrorSeverity.HIGH,
                )

            dashboards_xml.append(doc.to_dict())

            try:
                _logger.info("dashboard_generated", **doc.summary())
            except Exception:
                pass

        result.output = {"dashboards_xml": dashboards_xml}
        result.metadata["dashboards_summary"] = {
            "dashboards_generated": len(dashboards_xml),
            "all_valid": all(d["is_valid"] for d in dashboards_xml) if dashboards_xml else False,
        }

        if result.status == AgentStatus.PENDING:
            result.status = AgentStatus.SUCCESS

        return self.log_complete(result)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _find_ws_name_for_view_id(
    view_id:    str,
    spec:       ProjectSpec,
    ws_by_name: Dict[str, dict],
) -> Optional[str]:
    """
    Resolve a view_id from dashboard_requirements.csv to the worksheet
    display name used in worksheets_xml.

    Strategy:
      1. Direct match by view_id in ws_by_name (if names match view_ids)
      2. Lookup DashboardRequirement.view_name for the matching view_id
         in project_spec, then match against ws_by_name
    """
    # Try direct name match (view_id might happen to equal display name)
    if view_id in ws_by_name:
        return view_id

    # Lookup via project_spec requirements
    for req in spec.get_worksheets():
        if req.view_id == view_id:
            name = req.view_name
            if name in ws_by_name:
                return name
    return None


def _resolve_sheet_names(
    view_ids:   List[str],
    ws_by_name: Dict[str, dict],
    spec:       ProjectSpec,
) -> List[Optional[str]]:
    """
    Map ordered list of view_ids → worksheet display names.
    Returns None for unresolvable view_ids.
    """
    return [
        _find_ws_name_for_view_id(vid, spec, ws_by_name)
        for vid in view_ids
    ]
