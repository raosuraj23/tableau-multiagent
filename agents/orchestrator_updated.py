"""
Master Orchestrator  v2
=======================
LangGraph DAG — uses DatasourceAgent (replaces MstrMetricMapperAgent).
Supports both Direct mode (published DS → TWB) and MSTR mode.

Pipeline:
    START → intake → [datasource_agent ‖ figma_design] → twb_generator
          → (deploy | abort) → END
"""

from __future__ import annotations

from operator import add
from pathlib import Path
from typing import Annotated, Any, Dict, List, Optional

from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

from agents.datasource_agent import DatasourceAgent
from agents.figma_agent import FigmaDesignAgent
from agents.intake_agent import InputValidationAgent
from agents.twb_generator_agent import TwbGeneratorAgent
from agents.deployment_agent import DeploymentAgent


class WorkbookState(TypedDict):
    project_spec: Dict[str, Any]
    figma_image_path: Optional[str]
    tableau_auth_token: Optional[str]
    field_mapping: Dict[str, Any]
    design_tokens: Dict[str, Any]
    twb_path: Optional[str]
    publish_result: Dict[str, Any]
    phase: str
    errors: Annotated[List[str], add]
    warnings: Annotated[List[str], add]
    approved: bool


def _empty_state() -> WorkbookState:
    return WorkbookState(
        project_spec={}, figma_image_path=None, tableau_auth_token=None,
        field_mapping={}, design_tokens={}, twb_path=None, publish_result={},
        phase="IDLE", errors=[], warnings=[], approved=False,
    )


def build_workflow(config: Optional[Dict[str, Any]] = None) -> Any:
    cfg = config or {}

    intake_agent     = InputValidationAgent(cfg)
    ds_agent         = DatasourceAgent(cfg)
    figma_agent      = FigmaDesignAgent(cfg)
    twb_agent        = TwbGeneratorAgent({
        **cfg,
        "twb_output_dir": cfg.get("twb_output_dir", "models/twb"),
        "tableau_version": cfg.get("tableau_version", "18.1"),
    })
    deploy_agent     = DeploymentAgent({
        **cfg,
        "allow_overwrite": cfg.get("allow_overwrite", False),
    })

    # ── Node functions ──────────────────────────────────

    def intake_node(state: WorkbookState) -> WorkbookState:
        result = intake_agent.execute(state)
        return {**state,
                "project_spec": result.output.get("project_spec", {}),
                "phase": "VALIDATING",
                "errors": result.errors, "warnings": result.warnings}

    def datasource_node(state: WorkbookState) -> WorkbookState:
        result = ds_agent.execute(state)
        return {**state,
                "field_mapping": result.output.get("field_mapping", {}),
                "errors": result.errors, "warnings": result.warnings}

    def figma_node(state: WorkbookState) -> WorkbookState:
        result = figma_agent.execute(state)
        return {**state,
                "design_tokens": result.output.get("design_tokens", {}),
                "errors": result.errors, "warnings": result.warnings}

    def twb_generator_node(state: WorkbookState) -> WorkbookState:
        result = twb_agent.execute(state)
        return {**state,
                "twb_path": result.output.get("twb_path"),
                "phase": "GENERATING",
                "errors": result.errors, "warnings": result.warnings}

    def deployment_node(state: WorkbookState) -> WorkbookState:
        result = deploy_agent.execute(state)
        return {**state,
                "publish_result": result.output.get("publish_result", {}),
                "phase": "COMPLETE" if result.succeeded else "FAILED",
                "errors": result.errors, "warnings": result.warnings}

    def should_deploy(state: WorkbookState) -> str:
        if not state.get("twb_path"):
            return "abort"
        critical = [e for e in state.get("errors", [])
                    if any(k in e.lower() for k in ("failed", "missing", "invalid", "not found"))]
        return "abort" if critical else "deploy"

    # ── Build graph ─────────────────────────────────────
    wf: StateGraph = StateGraph(WorkbookState)
    wf.add_node("intake",          intake_node)
    wf.add_node("datasource",      datasource_node)
    wf.add_node("figma_design",    figma_node)
    wf.add_node("twb_generator",   twb_generator_node)
    wf.add_node("deployment",      deployment_node)

    wf.add_edge(START,          "intake")
    wf.add_edge("intake",       "datasource")
    wf.add_edge("intake",       "figma_design")
    wf.add_edge("datasource",   "twb_generator")
    wf.add_edge("figma_design", "twb_generator")
    wf.add_conditional_edges(
        "twb_generator", should_deploy,
        {"deploy": "deployment", "abort": END},
    )
    wf.add_edge("deployment", END)

    try:
        from langgraph.checkpoint.sqlite import SqliteSaver
        Path("state").mkdir(exist_ok=True)
        return wf.compile(checkpointer=SqliteSaver.from_conn_string("state/state.db"))
    except Exception:
        from langgraph.checkpoint.memory import MemorySaver
        return wf.compile(checkpointer=MemorySaver())


def run_pipeline(
    csv_dir: str = "csv_inputs",
    figma_image_path: Optional[str] = None,
    tableau_auth_token: Optional[str] = None,
    allow_overwrite: bool = False,
    config_overrides: Optional[Dict[str, Any]] = None,
) -> WorkbookState:
    cfg = {"csv_dir": csv_dir, "allow_overwrite": allow_overwrite, **(config_overrides or {})}
    app = build_workflow(cfg)
    initial = {**_empty_state(),
               "figma_image_path": figma_image_path,
               "tableau_auth_token": tableau_auth_token}
    return app.invoke(initial, {"configurable": {"thread_id": "main"}})
