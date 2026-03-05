# agents/tableau_model_agent.py
"""
TableauModelAgent — Phase 08: Tableau Model / Worksheet XML Generation
======================================================================

Translates every worksheet row in dashboard_requirements.csv into a
valid Tableau <worksheet> XML block, using the TDS column registry built
by SemanticModelAgent (Phase 06) to resolve field references.

Input state keys expected:
  project_spec     — dict from IntakeAgent
  tds_documents    — list[dict] from SemanticModelAgent (Phase 06)

Output (written to WorkbookState):
  worksheets_xml   — list[dict], one WorksheetDocument.to_dict() per worksheet

Gate condition for Phase 09 (DashboardGenAgent):
  All WorksheetDocument.is_valid == True

Processing pipeline (per worksheet):
  1. Lookup ds_name from tds_documents by datasource_id
  2. Build ColumnRegistry from tds XML
  3. Parse rows/cols/color/size/label fields → shelf references
  4. Build sort specs from sort_by/sort_direction
  5. Build filter specs from filter_fields
  6. Emit WorksheetDocument → to_xml()
  7. Accumulate in worksheets_xml output list

Dashboards (view_type='dashboard') are SKIPPED — handled by Phase 09.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from agents.base_agent import (
    AgentResult,
    AgentStatus,
    BaseAgent,
    ErrorSeverity,
    PhaseContext,
)
from models.project_spec import ProjectSpec
from models.worksheet_spec import (
    ColumnRegistry,
    FilterSpec,
    SortSpec,
    WorksheetDocument,
    chart_type_to_mark,
    resolve_field_ref,
)

try:
    import structlog
    _logger = structlog.get_logger().bind(agent="tableau_model_agent")
except Exception:
    import logging
    _logger = logging.getLogger("tableau_model_agent")


# ── TableauModelAgent ──────────────────────────────────────────────────────────

class TableauModelAgent(BaseAgent):
    """Phase 08 — Tableau Model / Worksheet XML Generation."""

    def __init__(
        self,
        config:  Optional[Dict[str, Any]] = None,
        context: Optional[PhaseContext]   = None,
    ) -> None:
        super().__init__(
            agent_id="tableau_model_agent",
            phase="GENERATING",
            config=config or {},
            context=context,
        )
        self._output_dir = Path(
            self.config.get("ws_output_dir", "models/twb")
        )

    # ── validate_input ──────────────────────────────────────────────────────

    def validate_input(self, state: Dict[str, Any]) -> List[str]:
        errors: List[str] = []
        if not state.get("project_spec"):
            errors.append("project_spec missing. Run IntakeAgent first.")
        if not state.get("tds_documents"):
            errors.append(
                "tds_documents missing. Run SemanticModelAgent (Phase 06) first."
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

        tds_docs: List[dict] = state.get("tds_documents", [])

        # ── Build ColumnRegistry per datasource ────────────────────────────
        registries = _build_registries(tds_docs)

        # ── Process each worksheet ─────────────────────────────────────────
        worksheets_xml: List[dict] = []

        for req in spec.get_worksheets():
            ds_id = req.datasource_id
            tds   = _find_tds(tds_docs, ds_id)

            if tds is None:
                result.add_error(
                    f"No TDS document found for datasource '{ds_id}' "
                    f"(worksheet '{req.view_name}'). Skipping.",
                    severity=ErrorSeverity.HIGH,
                )
                continue

            ds_name  = tds.get("ds_name", f"federated.{ds_id}")
            registry = registries.get(ds_id) or ColumnRegistry(ds_name=ds_name)

            try:
                doc = _build_worksheet(req, ds_name, ds_id, registry)
            except Exception as e:
                result.add_error(
                    f"Worksheet XML generation failed for '{req.view_name}': {e}",
                    severity=ErrorSeverity.HIGH, exc=e,
                )
                continue

            if not doc.is_valid:
                result.add_error(
                    f"Worksheet '{req.view_name}' is invalid after generation.",
                    severity=ErrorSeverity.HIGH,
                )

            worksheets_xml.append(doc.to_dict())

            try:
                _logger.info("worksheet_generated", **doc.summary())
            except Exception:
                pass

        result.output = {"worksheets_xml": worksheets_xml}
        result.metadata["worksheets_summary"] = {
            "worksheets_generated": len(worksheets_xml),
            "all_valid": all(w["is_valid"] for w in worksheets_xml) if worksheets_xml else False,
        }

        if result.status == AgentStatus.PENDING:
            result.status = AgentStatus.SUCCESS

        return self.log_complete(result)


# ── Builder helpers ────────────────────────────────────────────────────────────

def _build_registries(tds_docs: List[dict]) -> Dict[str, ColumnRegistry]:
    """Build one ColumnRegistry per datasource from tds_documents list."""
    regs: Dict[str, ColumnRegistry] = {}
    for tds in tds_docs:
        ds_id   = tds.get("datasource_id", "")
        ds_name = tds.get("ds_name", f"federated.{ds_id}")
        xml     = tds.get("xml", "")
        if ds_id and xml:
            regs[ds_id] = ColumnRegistry.from_tds_xml(ds_name, xml)
    return regs


def _find_tds(tds_docs: List[dict], ds_id: str) -> Optional[dict]:
    return next((t for t in tds_docs if t.get("datasource_id") == ds_id), None)


def _split_pipe(value: str) -> List[str]:
    """Split a pipe-separated field list, stripping blanks."""
    if not value:
        return []
    return [v.strip() for v in value.split("|") if v.strip()]


def _build_worksheet(
    req:      Any,           # DashboardRequirement
    ds_name:  str,
    ds_id:    str,
    registry: ColumnRegistry,
) -> WorksheetDocument:
    """Build a WorksheetDocument from a DashboardRequirement."""

    mark_class = chart_type_to_mark(req.chart_type or "Bar")

    doc = WorksheetDocument(
        name=req.view_name,
        ds_name=ds_name,
        mark_class=mark_class,
        datasource_id=ds_id,
        view_id=req.view_id,
    )

    # ── Rows shelf ─────────────────────────────────────────────────────────
    for raw in _split_pipe(req.rows or ""):
        ref = resolve_field_ref(raw, registry, ds_name)
        if ref:
            doc.row_refs.append(ref)

    # ── Columns shelf ──────────────────────────────────────────────────────
    for raw in _split_pipe(req.columns or ""):
        ref = resolve_field_ref(raw, registry, ds_name)
        if ref:
            doc.col_refs.append(ref)

    # ── Color encoding ─────────────────────────────────────────────────────
    if req.color:
        doc.color_ref = resolve_field_ref(req.color, registry, ds_name)

    # ── Size encoding ──────────────────────────────────────────────────────
    if req.size:
        doc.size_ref = resolve_field_ref(req.size, registry, ds_name)

    # ── Label/Text encoding ────────────────────────────────────────────────
    if req.label:
        ref = resolve_field_ref(req.label, registry, ds_name)
        if mark_class == "Text":
            doc.text_ref = ref
        else:
            doc.label_ref = ref

    # ── Sorts ──────────────────────────────────────────────────────────────
    if req.sort_by:
        direction = "descending" if (req.sort_direction or "desc").lower() == "desc" else "ascending"
        # Sort is always on a measure → force sum aggregation
        sort_ref = resolve_field_ref(req.sort_by, registry, ds_name, force_agg="sum")
        doc.sorts.append(SortSpec(field_ref=sort_ref, direction=direction))

    # ── Filters ────────────────────────────────────────────────────────────
    for raw in _split_pipe(req.filter_fields or ""):
        entry = registry.lookup(raw)
        role  = entry["role"] if entry else "dimension"
        ref   = resolve_field_ref(raw, registry, ds_name)
        if ref:
            doc.filters.append(FilterSpec(field_ref=ref, field_role=role))

    return doc
