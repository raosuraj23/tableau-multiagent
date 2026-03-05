# agents/conversion_agent.py
"""
DataConversionAgent — Phase 05: Data Conversion
================================================

Bridges the raw schema profiles (Phase 04) and the semantic XML generators
(Phases 06-08). Produces a TableauFieldMapping: every column and metric
fully resolved with its Tableau datatype, role, type, and display name.

Input state keys expected:
  project_spec    — dict from IntakeAgent (ProjectSpec.model_dump())
  schema_profiles — list[dict] from ProfilerAgent (one per datasource)

schema_profiles element shape (from actual ProfilerAgent output):
  {
    datasource_id:  str,
    can_proceed:    bool,
    tables: [
      {
        table_id:        str,
        table_name:      str,
        schema:          str,
        profiled:        bool,
        error:           str | None,
        type_mismatches: int,
        columns: [
          {
            physical_name:    str,   # raw DB column name
            declared_name:    str,   # display name from spec
            physical_type:    str,   # raw DB type string
            tableau_datatype: str,   # DB-derived Tableau type
            declared_datatype:str,   # spec override datatype
            type_match:       bool,  # True = types agree
            nullable:         bool,
            row_count:        int|None,
            null_count:       int|None,
            null_rate:        float|None,
            sample_values:    list,
          }
        ]
      }
    ]
  }

Output (written to WorkbookState):
  field_mapping — TableauFieldMapping.to_dict()

Gate condition for Phase 06:
  field_mapping["can_proceed"] == True
"""

from __future__ import annotations

import hashlib
import re
from typing import Any, Dict, List, Optional, Set

from agents.base_agent import (
    AgentResult,
    AgentStatus,
    BaseAgent,
    ErrorSeverity,
    PhaseContext,
)
from models.field_mapping import (
    FieldMapping,
    MappingSource,
    MappingStatus,
    MetricMapping,
    TableauFieldMapping,
)
from models.project_spec import ProjectSpec

try:
    import structlog
    _logger = structlog.get_logger().bind(agent="conversion_agent")
except Exception:
    import logging
    _logger = logging.getLogger("conversion_agent")

# ── Role / type lookup tables ──────────────────────────────────────────────────

_DATATYPE_TO_ROLE: Dict[str, str] = {
    "string":   "dimension",
    "boolean":  "dimension",
    "date":     "dimension",
    "datetime": "dimension",
    "integer":  "measure",
    "real":     "measure",
}

_DATATYPE_TO_TYPE: Dict[str, str] = {
    "string":   "nominal",
    "boolean":  "nominal",
    "date":     "ordinal",
    "datetime": "ordinal",
    "integer":  "quantitative",
    "real":     "quantitative",
}

_VALID_AGGREGATIONS: Set[str] = {
    "SUM", "AVG", "COUNT", "COUNTD", "MIN", "MAX", "MEDIAN",
    "STDEV", "STDEVP", "VAR", "VARP", "NONE", "ATTR",
}

_MSTR_AGG_MAP: Dict[str, str] = {
    "Sum":        "SUM",
    "Avg":        "AVG",
    "Average":    "AVG",
    "Count":      "COUNT",
    "Min":        "MIN",
    "Max":        "MAX",
    "RunningSum": "RUNNING_SUM",
    "Rank":       "RANK",
}


# ── DataConversionAgent ────────────────────────────────────────────────────────

class DataConversionAgent(BaseAgent):
    """Phase 05 — Data Conversion."""

    def __init__(
        self,
        config:  Optional[Dict[str, Any]] = None,
        context: Optional[PhaseContext]   = None,
    ) -> None:
        super().__init__(
            agent_id="conversion_agent",
            phase="CONVERTING",
            config=config or {},
            context=context,
        )

    # ── validate_input ──────────────────────────────────────────────────────

    def validate_input(self, state: Dict[str, Any]) -> List[str]:
        errors: List[str] = []

        if not state.get("project_spec"):
            errors.append("project_spec not found. Run IntakeAgent first.")

        profiles = state.get("schema_profiles")
        if not profiles:
            errors.append(
                "schema_profiles not found. Run ProfilerAgent (Phase 04) first."
            )
        else:
            blocked = [
                p.get("datasource_id", "unknown")
                for p in (profiles if isinstance(profiles, list) else [])
                if not p.get("can_proceed", True)
            ]
            if blocked:
                errors.append(
                    f"ProfilerAgent reported can_proceed=False for "
                    f"datasource(s): {blocked}. Fix inaccessible tables first."
                )
        return errors

    # ── run ────────────────────────────────────────────────────────────────

    def run(self, state: Dict[str, Any]) -> AgentResult:
        self.log_start()
        result = AgentResult(agent_id=self.agent_id, phase=self.phase)

        try:
            spec: ProjectSpec = ProjectSpec.model_validate(state["project_spec"])
        except Exception as e:
            result.add_error(
                f"Cannot parse project_spec: {e}",
                severity=ErrorSeverity.CRITICAL, exc=e,
            )
            return self.log_complete(result)

        profiles: List[Dict[str, Any]] = state["schema_profiles"]

        mapping = TableauFieldMapping(
            project_id=spec.project_config.project_id,
            run_id=self.context.run_id if self.context else "",
        )

        # Build spec-column lookup by (table_id, column_name_upper)
        spec_col_index: Dict[tuple, Any] = {
            (c.table_id, c.column_name.upper()): c
            for c in spec.columns
        }

        # Step 1: Walk datasource → table → column
        for ds_profile in profiles:
            datasource_id = ds_profile.get("datasource_id", "")
            for table in ds_profile.get("tables", []):
                table_id   = table.get("table_id", "")
                table_name = table.get("table_name", "")
                profiled   = table.get("profiled", False)

                if not profiled:
                    for col in spec.get_columns_for_table(table_id):
                        mapping.fields.append(
                            _make_missing_field(col, table_name, datasource_id)
                        )
                        result.add_error(
                            f"Table '{table_name}' not profiled — "
                            f"column '{col.column_name}' marked MISSING",
                            severity=ErrorSeverity.HIGH,
                            field=col.column_id,
                        )
                    continue

                profiled_names: Set[str] = set()

                for col_dict in table.get("columns", []):
                    physical_name = col_dict.get("physical_name", "")
                    profiled_names.add(physical_name.upper())
                    spec_col = spec_col_index.get(
                        (table_id, physical_name.upper())
                    )
                    fm = _resolve_field(
                        col_dict=col_dict,
                        spec_col=spec_col,
                        table_id=table_id,
                        table_name=table_name,
                        datasource_id=datasource_id,
                    )
                    mapping.fields.append(fm)

                # Flag spec columns missing from the profiled column list
                for col in spec.get_columns_for_table(table_id):
                    if col.column_name.upper() not in profiled_names:
                        mapping.fields.append(
                            _make_missing_field(col, table_name, datasource_id)
                        )
                        result.add_error(
                            f"Column '{col.column_name}' declared in spec "
                            f"but absent from profiled table '{table_name}'",
                            severity=ErrorSeverity.HIGH,
                            field=col.column_id,
                        )

        # Step 2: Metric mappings from metrics.csv
        for metric in spec.metrics:
            mm = _map_metric(metric)
            mapping.metrics.append(mm)
            if not mm.formula_valid:
                result.add_error(
                    f"Metric '{metric.metric_name}' invalid formula: "
                    f"{mm.formula_errors}",
                    severity=ErrorSeverity.HIGH,
                    field=metric.metric_id,
                )

        # Step 3: MSTR metric translation (deduplicated against metrics.csv)
        existing_ids = {m.metric_id for m in mapping.metrics}
        for mstr_m in spec.mstr_metrics:
            if mstr_m.mstr_metric_id not in existing_ids:
                mapping.metrics.append(_translate_mstr_metric(mstr_m, spec))

        # Assemble output
        result.output   = {"field_mapping": mapping.to_dict()}
        result.metadata["mapping_summary"] = mapping.summary()

        if not mapping.can_proceed:
            result.add_error(
                f"Mapping blocked: {mapping.missing_count} missing field(s), "
                f"{mapping.invalid_metric_count} invalid metric(s)",
                severity=ErrorSeverity.CRITICAL,
            )
        elif mapping.conflict_count > 0:
            result.add_warning(
                f"{mapping.conflict_count} type conflict(s) — spec overrides applied"
            )

        try:
            _logger.info("conversion_complete", **mapping.summary())
        except Exception:
            pass

        if result.status == AgentStatus.PENDING:
            result.status = AgentStatus.SUCCESS

        return self.log_complete(result)


# ── Field resolution ───────────────────────────────────────────────────────────

def _resolve_field(
    col_dict:      Dict[str, Any],
    spec_col:      Any,
    table_id:      str,
    table_name:    str,
    datasource_id: str,
) -> FieldMapping:
    physical_name = col_dict.get("physical_name", "")
    declared_name = col_dict.get("declared_name") or physical_name
    physical_type = col_dict.get("physical_type", "")
    type_match    = col_dict.get("type_match", True)

    # Datatype: spec override wins
    if spec_col and spec_col.datatype:
        datatype = spec_col.datatype
        source   = MappingSource.SPEC
    else:
        datatype = (
            col_dict.get("tableau_datatype")
            or col_dict.get("declared_datatype")
            or "string"
        )
        source = MappingSource.DB

    # Role: spec override wins
    if spec_col and spec_col.role:
        role = spec_col.role
    else:
        role = _DATATYPE_TO_ROLE.get(datatype, "dimension")

    type_ = _DATATYPE_TO_TYPE.get(datatype, "nominal")

    display_name = (
        (spec_col.display_name or spec_col.column_name)
        if spec_col else declared_name
    )
    caption = display_name.replace("_", " ").title()

    aggregation = "NONE"
    if role == "measure":
        if spec_col and spec_col.aggregation:
            agg = spec_col.aggregation.upper()
            aggregation = agg if agg in _VALID_AGGREGATIONS else "SUM"
        else:
            aggregation = "SUM"

    # Status / conflict detection
    conflict_note = None
    if spec_col and spec_col.role == "measure" and _DATATYPE_TO_ROLE.get(datatype) == "dimension":
        conflict_note = (
            f"Spec role=measure but '{datatype}' is a dimension type"
        )
        status = MappingStatus.CONFLICT
    elif not type_match:
        conflict_note = (
            f"Type mismatch: physical='{physical_type}' "
            f"vs declared='{col_dict.get('declared_datatype', '')}'"
        )
        status = MappingStatus.CONFLICT
    elif source == MappingSource.SPEC:
        status = MappingStatus.OVERRIDDEN
    else:
        status = MappingStatus.INFERRED

    xml_name = f"[{display_name}]"
    col_id   = spec_col.column_id if spec_col else None

    return FieldMapping(
        field_id=col_id or f"{table_name}_{physical_name}".lower(),
        column_id=col_id,
        table_name=table_name,
        datasource_id=datasource_id,
        tableau_name=xml_name,
        xml_name=xml_name,
        caption=caption,
        datatype=datatype,
        role=role,
        type_=type_,
        aggregation=aggregation,
        hidden=spec_col.hidden if spec_col else False,
        folder_group=spec_col.group if spec_col else None,
        description=spec_col.description if spec_col else None,
        status=status,
        source=source,
        conflict_note=conflict_note,
    )


def _make_missing_field(
    spec_col: Any,
    table_name: str,
    datasource_id: str,
) -> FieldMapping:
    display_name = spec_col.display_name or spec_col.column_name
    return FieldMapping(
        field_id=spec_col.column_id or f"missing_{spec_col.column_name.lower()}",
        column_id=spec_col.column_id,
        table_name=table_name,
        datasource_id=datasource_id,
        tableau_name=f"[{display_name}]",
        xml_name=f"[{display_name}]",
        caption=display_name.replace("_", " ").title(),
        datatype=spec_col.datatype,
        role=spec_col.role,
        type_=_DATATYPE_TO_TYPE.get(spec_col.datatype, "nominal"),
        status=MappingStatus.MISSING,
        source=MappingSource.SPEC,
    )


# ── Metric helpers ─────────────────────────────────────────────────────────────

def _map_metric(metric: Any) -> MetricMapping:
    calc_id        = _generate_calc_id(metric.metric_id)
    caption        = metric.metric_name.replace("_", " ").title()
    formula_errors = _validate_formula(metric.formula)
    datatype       = metric.datatype or "real"
    return MetricMapping(
        metric_id=metric.metric_id,
        metric_name=metric.metric_name,
        datasource_id=metric.datasource_id,
        tableau_formula=metric.formula,
        datatype=datatype,
        role="measure",
        type_=_DATATYPE_TO_TYPE.get(datatype, "quantitative"),
        is_lod=metric.is_lod,
        format_string=metric.format_string,
        description=metric.description,
        calc_id=calc_id,
        caption=caption,
        formula_valid=len(formula_errors) == 0,
        formula_errors=formula_errors,
    )


def _translate_mstr_metric(mstr_m: Any, spec: ProjectSpec) -> MetricMapping:
    formula = (
        mstr_m.tableau_formula
        if mstr_m.tableau_formula
        else _translate_mstr_formula(
            mstr_m.mstr_formula or "",
            mstr_m.mstr_complexity or "simple",
        )
    )
    calc_id        = _generate_calc_id(mstr_m.mstr_metric_id)
    caption        = mstr_m.mstr_metric_name.replace("_", " ").title()
    formula_errors = _validate_formula(formula)
    ds_id          = spec.data_sources[0].datasource_id if spec.data_sources else ""
    return MetricMapping(
        metric_id=mstr_m.mstr_metric_id,
        metric_name=mstr_m.mstr_metric_name,
        datasource_id=ds_id,
        tableau_formula=formula,
        datatype="real",
        role="measure",
        type_="quantitative",
        calc_id=calc_id,
        caption=caption,
        formula_valid=len(formula_errors) == 0,
        formula_errors=formula_errors,
    )


# ── Formula utilities ──────────────────────────────────────────────────────────

def _generate_calc_id(seed: str) -> str:
    h       = hashlib.sha256(seed.encode()).hexdigest()
    numeric = int(h[:16], 16) % (10 ** 19)
    return f"[Calculation_{numeric:019d}]"


def _validate_formula(formula: str) -> List[str]:
    errors: List[str] = []
    if not formula or not formula.strip():
        errors.append("Formula is empty")
        return errors
    if formula.count("[") != formula.count("]"):
        errors.append(
            f"Unbalanced brackets: {formula.count('[')} '[' vs "
            f"{formula.count(']')} ']'"
        )
    if formula.count("(") != formula.count(")"):
        errors.append(
            f"Unbalanced parentheses: {formula.count('(')} '(' vs "
            f"{formula.count(')')} ')'"
        )
    if "{" in formula or "}" in formula:
        if formula.count("{") != formula.count("}"):
            errors.append("Unbalanced curly braces in LOD expression")
        if ":" not in formula:
            errors.append(
                "LOD expression (curly braces) missing ':' separator"
            )
    return errors


def _translate_mstr_formula(mstr_formula: str, metric_type: str) -> str:
    if not mstr_formula:
        return ""
    formula = mstr_formula.strip()

    m = re.match(r"RunningSum\((.+)\)", formula, re.IGNORECASE)
    if m:
        return f"RUNNING_SUM(SUM([{m.group(1).strip()}]))"

    m = re.match(r"Rank\((.+)\)", formula, re.IGNORECASE)
    if m:
        return f"RANK(SUM([{m.group(1).strip()}]))"

    for mstr_agg, tab_agg in _MSTR_AGG_MAP.items():
        m = re.match(rf"{mstr_agg}\((.+)\)", formula, re.IGNORECASE)
        if m:
            return f"{tab_agg}([{m.group(1).strip()}])"

    _reserved = {
        "AND", "OR", "NOT", "IF", "THEN", "ELSE", "END",
        "FIXED", "INCLUDE", "EXCLUDE", "TRUE", "FALSE", "NULL",
    }
    return re.sub(
        r"\b([A-Za-z_][A-Za-z0-9_]*)\b",
        lambda mo: f"SUM([{mo.group(1)}])"
        if mo.group(1).upper() not in _reserved else mo.group(1),
        formula,
    )
