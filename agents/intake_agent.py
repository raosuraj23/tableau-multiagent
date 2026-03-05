"""
Input & Validation Agent
========================
Phase 1 of the pipeline.

Responsibilities:
- Ingest all CSV input files
- Validate schema, required fields, referential integrity
- Normalise into a typed ProjectSpec dict for downstream agents

MSTR CSVs (mstr_metrics, mstr_attributes) are OPTIONAL.
When absent, the pipeline runs in "direct published datasource" mode:
  - No formula translation is performed
  - Fields referenced in dashboard_requirements.csv map directly to the
    published datasource's native fields / calculated fields
  - The metrics.csv file (Tableau native calc fields) is used instead
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Dict, List

from agents.base_agent import AgentResult, BaseAgent


# ─────────────────────────────────────────────
# Required columns per CSV
# ─────────────────────────────────────────────
REQUIRED_COLUMNS: Dict[str, List[str]] = {
    "project_config.csv": [
        "project_id", "project_name", "environment",
        "tableau_site", "tableau_server_url", "target_project",
        "workbook_name",
    ],
    "dashboard_requirements.csv": [
        "view_id", "view_name", "view_type", "chart_type",
    ],
}

# Optional — loaded when present, silently skipped when absent
OPTIONAL_CSVS = {
    "mstr_metrics.csv",       # MSTR mode: raw MSTR formulas for LLM translation
    "mstr_attributes.csv",    # MSTR mode: MSTR attribute definitions
    "metrics.csv",            # Direct mode: native Tableau calculated field definitions
    "columns.csv",            # Direct mode: published datasource field metadata
    "figma_layout.csv",       # Design zone layout overrides
}


class InputValidationAgent(BaseAgent):
    """Ingests and validates all CSV inputs; emits a ProjectSpec."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__("input_validation_agent", config)
        self.csv_dir = Path(config.get("csv_dir", "csv_inputs"))

    def validate_input(self, state: Dict[str, Any]) -> List[str]:
        errors: List[str] = []
        if not self.csv_dir.exists():
            errors.append(f"CSV directory not found: {self.csv_dir}")
        return errors

    def run(self, state: Dict[str, Any]) -> AgentResult:
        errors: List[str] = []
        warnings: List[str] = []
        parsed: Dict[str, Any] = {}

        # ── Required CSVs ──
        for filename, required_cols in REQUIRED_COLUMNS.items():
            path = self.csv_dir / filename
            if not path.exists():
                errors.append(f"Missing required CSV: {filename}")
                continue
            rows, col_errors = self._load_csv(path, required_cols)
            errors.extend(col_errors)
            parsed[filename] = rows

        # ── Optional CSVs ──
        for filename in OPTIONAL_CSVS:
            path = self.csv_dir / filename
            if path.exists():
                rows, _ = self._load_csv(path, [])
                parsed[filename] = rows
                self.logger.info("optional_csv_loaded", file=filename, rows=len(rows))
            else:
                warnings.append(f"Optional CSV not present (skipped): {filename}")

        # ── Determine pipeline mode ──
        has_mstr = bool(parsed.get("mstr_metrics.csv")) or bool(parsed.get("mstr_attributes.csv"))
        has_tableau_calcs = bool(parsed.get("metrics.csv"))
        mode = "mstr" if has_mstr else "direct"
        if mode == "direct" and not has_tableau_calcs:
            warnings.append(
                "Running in direct mode (no MSTR CSVs) with no metrics.csv — "
                "only native published datasource fields will be available."
            )

        self.logger.info("pipeline_mode_detected", mode=mode)

        if errors:
            return AgentResult(
                agent_id=self.agent_id, phase="input_validation",
                status="failed", errors=errors, warnings=warnings,
            )

        project_spec = self._build_project_spec(parsed, mode)

        ref_errors = self._check_referential_integrity(parsed)
        errors.extend(ref_errors)

        status = "failed" if errors else ("warning" if warnings else "success")
        return AgentResult(
            agent_id=self.agent_id, phase="input_validation",
            status=status,
            output={"project_spec": project_spec, "raw_csv": parsed},
            errors=errors, warnings=warnings,
        )

    # ──────────────────────────────────────────
    def _load_csv(self, path: Path, required_cols: List[str]):
        errors: List[str] = []
        rows: List[Dict[str, str]] = []
        try:
            with path.open(newline="", encoding="utf-8-sig") as fh:
                reader = csv.DictReader(fh)
                headers = reader.fieldnames or []
                missing = [c for c in required_cols if c not in headers]
                if missing:
                    errors.append(f"{path.name}: missing required columns {missing}")
                    return rows, errors
                for row in reader:
                    rows.append(dict(row))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{path.name}: failed to read — {exc}")
        return rows, errors

    def _build_project_spec(self, parsed: Dict[str, Any], mode: str) -> Dict[str, Any]:
        cfg = (parsed.get("project_config.csv") or [{}])[0]
        return {
            "project_id": cfg.get("project_id", ""),
            "project_name": cfg.get("project_name", ""),
            "environment": cfg.get("environment", "dev"),
            "tableau_site": cfg.get("tableau_site", ""),
            "tableau_server_url": cfg.get("tableau_server_url", ""),
            "target_project": cfg.get("target_project", ""),
            "workbook_name": cfg.get("workbook_name", ""),
            "tableau_version": cfg.get("tableau_version", "18.1"),
            "figma_file_id": cfg.get("figma_file_id", ""),
            "published_datasource_name": cfg.get("published_datasource_name", ""),
            "enable_extract": cfg.get("enable_extract", "false").lower() == "true",
            # Pipeline mode
            "pipeline_mode": mode,           # "mstr" | "direct"
            # Collections
            "mstr_metrics": parsed.get("mstr_metrics.csv", []),
            "mstr_attributes": parsed.get("mstr_attributes.csv", []),
            "tableau_metrics": parsed.get("metrics.csv", []),  # native calc fields
            "tableau_columns": parsed.get("columns.csv", []),  # native DS columns
            "dashboard_requirements": parsed.get("dashboard_requirements.csv", []),
            "figma_layout": parsed.get("figma_layout.csv", []),
        }

    def _check_referential_integrity(self, parsed: Dict[str, Any]) -> List[str]:
        errors: List[str] = []
        reqs = parsed.get("dashboard_requirements.csv", [])
        all_view_ids = {r.get("view_id", "") for r in reqs}
        for row in reqs:
            if row.get("view_type", "").lower() == "dashboard":
                refs = [
                    v.strip()
                    for v in row.get("views_in_dashboard", "").split("|")
                    if v.strip()
                ]
                missing = [v for v in refs if v not in all_view_ids]
                if missing:
                    errors.append(
                        f"Dashboard '{row.get('view_id')}' references unknown "
                        f"view_ids: {missing}"
                    )
        return errors
