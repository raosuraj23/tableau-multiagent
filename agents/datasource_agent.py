"""
Datasource Agent
================
Phase 2-A of the pipeline (parallel with Figma Design Agent).

Operates in two modes determined by project_spec.pipeline_mode:

DIRECT MODE  (no MSTR CSVs — this Superstore use case)
  1. Call Tableau Metadata API to discover published datasource fields
  2. Load native Tableau calculated fields from metrics.csv
  3. Load column metadata from columns.csv
  4. Return a FieldMapping that maps directly to published DS field names

MSTR MODE  (when mstr_metrics.csv is present)
  1. Call Tableau Metadata API to discover published datasource fields
  2. Use Claude LLM to translate MSTR formulas → Tableau calculated field syntax
  3. Map MSTR attributes → Tableau dimension columns
  4. Return a FieldMapping ready for the TWB Generator
"""

from __future__ import annotations

import json
import os
import re
import uuid
from typing import Any, Dict, List, Optional, Tuple

import requests

from agents.base_agent import AgentResult, BaseAgent

DATATYPE_ROLE_MAP: Dict[str, Tuple[str, str]] = {
    "string":   ("dimension", "nominal"),
    "integer":  ("measure",   "quantitative"),
    "real":     ("measure",   "quantitative"),
    "boolean":  ("dimension", "nominal"),
    "date":     ("dimension", "ordinal"),
    "datetime": ("dimension", "ordinal"),
}


class DatasourceAgent(BaseAgent):
    """
    Discovers published datasource schema and builds the FieldMapping.
    Supports both Direct (native Tableau fields) and MSTR (LLM formula mapping) modes.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__("datasource_agent", config)
        self.anthropic_api_key: str = os.environ.get("ANTHROPIC_API_KEY", "")
        self.llm_model: str = config.get("llm_model", "claude-opus-4-6")

    def validate_input(self, state: Dict[str, Any]) -> List[str]:
        errors: List[str] = []
        spec = state.get("project_spec", {})
        mode = spec.get("pipeline_mode", "direct")
        if mode == "mstr" and not self.anthropic_api_key:
            errors.append("ANTHROPIC_API_KEY required for MSTR mode formula translation")
        return errors

    def run(self, state: Dict[str, Any]) -> AgentResult:
        spec: Dict[str, Any] = state["project_spec"]
        mode = spec.get("pipeline_mode", "direct")
        warnings: List[str] = []

        # ── Step 1: Discover published datasource fields via Metadata API ──
        published_ds_fields, ds_errors = self._discover_published_datasource(state, spec)
        if ds_errors:
            warnings.extend(ds_errors)

        if mode == "direct":
            # ── Direct mode: load native Tableau calc fields from metrics.csv ──
            calc_fields = self._load_native_calc_fields(spec.get("tableau_metrics", []))
            dim_columns = self._load_native_columns(spec.get("tableau_columns", []))
            self.logger.info(
                "direct_mode_field_mapping",
                calc_fields=len(calc_fields),
                dim_columns=len(dim_columns),
                published_fields=len(published_ds_fields),
            )
        else:
            # ── MSTR mode: translate formulas with Claude ──
            field_schema_summary = json.dumps(
                [{"name": f["name"], "dataType": f["dataType"]}
                 for f in published_ds_fields], indent=2
            )
            calc_fields, map_errors = self._map_mstr_metrics(
                spec.get("mstr_metrics", []), field_schema_summary
            )
            if map_errors:
                warnings.extend(map_errors)
            dim_columns = self._map_mstr_attributes(spec.get("mstr_attributes", []))

        field_mapping: Dict[str, Any] = {
            "pipeline_mode": mode,
            "published_ds_fields": published_ds_fields,
            "calculated_fields": calc_fields,
            "dimension_columns": dim_columns,
        }

        status = "warning" if warnings else "success"
        return AgentResult(
            agent_id=self.agent_id, phase="datasource_mapping",
            status=status,
            output={"field_mapping": field_mapping},
            warnings=warnings,
        )

    # ──────────────────────────────────────────
    # Tableau Metadata API
    # ──────────────────────────────────────────

    def _discover_published_datasource(
        self, state: Dict[str, Any], spec: Dict[str, Any]
    ) -> Tuple[List[Dict[str, str]], List[str]]:
        errors: List[str] = []
        fields: List[Dict[str, str]] = []
        auth_token: Optional[str] = state.get("tableau_auth_token")
        ds_name: str = spec.get("published_datasource_name", "")

        if not auth_token:
            errors.append(
                "No tableau_auth_token in state — skipping Metadata API discovery. "
                "Field names will be taken directly from columns.csv / metrics.csv."
            )
            return fields, errors

        if not ds_name:
            errors.append("published_datasource_name not set — cannot query Metadata API.")
            return fields, errors

        query = """
        query DiscoverDS($name: String!) {
          publishedDatasources(filter: { name: $name }) {
            name id
            fields {
              name dataType role isCalculated description
            }
          }
        }
        """
        url = f"{spec['tableau_server_url']}/api/metadata/graphql"
        try:
            resp = requests.post(
                url,
                json={"query": query, "variables": {"name": ds_name}},
                headers={
                    "x-tableau-auth": auth_token,
                    "content-type": "application/json",
                },
                timeout=30,
            )
            resp.raise_for_status()
            sources = resp.json().get("data", {}).get("publishedDatasources", [])
            if not sources:
                errors.append(f"No published datasource found with name '{ds_name}'.")
                return fields, errors
            for f in sources[0].get("fields", []):
                fields.append({
                    "name": f.get("name", ""),
                    "dataType": f.get("dataType", "string").lower(),
                    "role": f.get("role", "DIMENSION").upper(),
                    "isCalculated": str(f.get("isCalculated", False)),
                    "description": f.get("description", ""),
                })
            self.logger.info("metadata_api_discovery_complete",
                             datasource=ds_name, field_count=len(fields))
        except requests.RequestException as exc:
            errors.append(f"Metadata API request failed: {exc}")
        return fields, errors

    # ──────────────────────────────────────────
    # Direct mode — native Tableau fields
    # ──────────────────────────────────────────

    def _load_native_calc_fields(
        self, metrics: List[Dict[str, str]]
    ) -> List[Dict[str, str]]:
        """Convert metrics.csv rows into Tableau calculated field specs."""
        calc_fields: List[Dict[str, str]] = []
        for m in metrics:
            datatype = m.get("datatype", "real")
            role, typ = DATATYPE_ROLE_MAP.get(datatype, ("measure", "quantitative"))
            calc_fields.append({
                "calc_id": f"[{m['metric_name']}]",   # use display name directly
                "caption": m.get("metric_name", ""),
                "datatype": datatype,
                "role": role,
                "type": typ,
                "formula": m.get("formula", ""),
                "description": m.get("description", ""),
                "format_string": m.get("format_string", ""),
                "is_lod": m.get("is_lod", "false"),
            })
        return calc_fields

    def _load_native_columns(
        self, columns: List[Dict[str, str]]
    ) -> List[Dict[str, str]]:
        """Convert columns.csv rows into Tableau dimension/measure column specs."""
        dim_columns: List[Dict[str, str]] = []
        for c in columns:
            datatype = c.get("datatype", "string")
            role, typ = DATATYPE_ROLE_MAP.get(datatype, ("dimension", "nominal"))
            # Override with explicit role if provided
            if c.get("role"):
                role = c["role"]
                typ = "quantitative" if role == "measure" else (
                    "ordinal" if datatype in ("date", "datetime") else "nominal"
                )
            dim_columns.append({
                "name": f"[{c.get('column_name', '')}]",
                "caption": c.get("display_name") or c.get("column_name", ""),
                "datatype": datatype,
                "role": role,
                "type": typ,
                "description": c.get("description", ""),
                "hidden": c.get("hidden", "false"),
                "group": c.get("group", ""),
            })
        return dim_columns

    # ──────────────────────────────────────────
    # MSTR mode — LLM formula translation
    # ──────────────────────────────────────────

    def _map_mstr_metrics(
        self, mstr_metrics: List[Dict[str, str]], field_schema: str
    ) -> Tuple[List[Dict[str, str]], List[str]]:
        errors: List[str] = []
        calc_fields: List[Dict[str, str]] = []
        for metric in mstr_metrics:
            metric_name = metric.get("metric_name", "")
            mstr_formula = metric.get("mstr_formula", "")
            datatype = metric.get("datatype", "real")
            try:
                tableau_formula = self._translate_formula_with_llm(
                    metric_name, mstr_formula, datatype, field_schema
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"LLM mapping failed for '{metric_name}': {exc}")
                tableau_formula = self._fallback_formula(mstr_formula)
            role, typ = DATATYPE_ROLE_MAP.get(datatype, ("measure", "quantitative"))
            calc_fields.append({
                "calc_id": f"[Calculation_{uuid.uuid4().int % 10**19:019d}]",
                "caption": metric_name,
                "datatype": datatype,
                "role": role,
                "type": typ,
                "formula": tableau_formula,
                "description": metric.get("description", ""),
                "format_string": metric.get("format_string", ""),
                "is_lod": str("FIXED" in tableau_formula.upper()),
            })
        return calc_fields, errors

    def _translate_formula_with_llm(
        self, metric_name: str, mstr_formula: str, datatype: str, field_schema: str
    ) -> str:
        from anthropic import Anthropic
        client = Anthropic(api_key=self.anthropic_api_key)
        message = client.messages.create(
            model=self.llm_model,
            max_tokens=512,
            system=(
                "You are an expert at converting MicroStrategy metric formulas to "
                "Tableau calculated field syntax. Return ONLY the Tableau formula — "
                "no explanation, no markdown, no surrounding quotes."
            ),
            messages=[{"role": "user", "content": (
                f"Convert this MSTR metric to Tableau.\n\n"
                f"Name: {metric_name}\nFormula: {mstr_formula}\n"
                f"Datatype: {datatype}\n\nAvailable fields:\n{field_schema}"
            )}],
        )
        return message.content[0].text.strip()

    @staticmethod
    def _fallback_formula(mstr_formula: str) -> str:
        formula = mstr_formula
        for pattern, replacement in [
            (r"Sum\(", "SUM("), (r"Avg\(", "AVG("),
            (r"Count\(", "COUNT("), (r"Min\(", "MIN("), (r"Max\(", "MAX("),
        ]:
            formula = re.sub(pattern, replacement, formula, flags=re.IGNORECASE)
        return formula

    def _map_mstr_attributes(
        self, mstr_attributes: List[Dict[str, str]]
    ) -> List[Dict[str, str]]:
        dim_columns: List[Dict[str, str]] = []
        for attr in mstr_attributes:
            datatype = attr.get("datatype", "string")
            role, typ = DATATYPE_ROLE_MAP.get(datatype, ("dimension", "nominal"))
            dim_columns.append({
                "name": f"[{attr.get('attribute_name', '')}]",
                "caption": attr.get("display_name") or attr.get("attribute_name", ""),
                "datatype": datatype, "role": role, "type": typ,
                "description": attr.get("description", ""),
                "hidden": attr.get("hidden", "false"),
            })
        return dim_columns
