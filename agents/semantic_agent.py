# agents/semantic_agent.py
"""
SemanticModelAgent — Phase 06: Semantic Model (TDS XML Generation)
==================================================================

Transforms the TableauFieldMapping (Phase 05) into a valid Tableau
<datasource> XML block for every datasource in the project.

Input state keys expected:
  project_spec    — dict from IntakeAgent
  field_mapping   — dict from DataConversionAgent (Phase 05)

Output (written to WorkbookState):
  tds_documents   — list[dict], one TdsDocument.to_dict() per datasource

Gate condition for Phase 07 (MetricDefinitionAgent):
  All TdsDocument.is_valid == True
  (at least one connection + at least one column per datasource)

Processing pipeline (per datasource):
  1. Build ConnectionSpec from connections.csv / data_sources.csv
  2. Build RelationSpec(s) from tables.csv + relationships.csv
  3. Build col_map entries from columns.csv
  4. Build raw ColumnSpec(s) from FieldMapping.fields
  5. Build calculated ColumnSpec(s) from FieldMapping.metrics
  6. Serialise to XML via TdsDocument.to_xml()
  7. Write TDS file to models/tds/{datasource_id}.tds  (optional)
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from agents.base_agent import (
    AgentResult,
    AgentStatus,
    BaseAgent,
    ErrorSeverity,
    PhaseContext,
)
from models.field_mapping import FieldMapping, MappingStatus, MetricMapping, TableauFieldMapping
from models.project_spec import ProjectSpec
from models.tds_document import (
    ColumnSpec,
    ConnectionSpec,
    RelationSpec,
    TdsDocument,
    _make_ds_name,
)

try:
    import structlog
    _logger = structlog.get_logger().bind(agent="semantic_agent")
except Exception:
    import logging
    _logger = logging.getLogger("semantic_agent")


# ── SemanticModelAgent ─────────────────────────────────────────────────────────

class SemanticModelAgent(BaseAgent):
    """Phase 06 — Semantic Model / TDS XML Generation."""

    def __init__(
        self,
        config:  Optional[Dict[str, Any]] = None,
        context: Optional[PhaseContext]   = None,
    ) -> None:
        super().__init__(
            agent_id="semantic_agent",
            phase="MODELING",
            config=config or {},
            context=context,
        )
        self._output_dir = Path(
            self.config.get("tds_output_dir", "models/tds")
        )

    # ── validate_input ──────────────────────────────────────────────────────

    def validate_input(self, state: Dict[str, Any]) -> List[str]:
        errors: List[str] = []
        if not state.get("project_spec"):
            errors.append("project_spec missing. Run IntakeAgent first.")
        if not state.get("field_mapping"):
            errors.append(
                "field_mapping missing. Run DataConversionAgent (Phase 05) first."
            )
        else:
            fm = state["field_mapping"]
            if isinstance(fm, dict) and not fm.get("can_proceed", True):
                errors.append(
                    "field_mapping.can_proceed is False — fix conversion errors first."
                )
        return errors

    # ── run ────────────────────────────────────────────────────────────────

    def run(self, state: Dict[str, Any]) -> AgentResult:
        self.log_start()
        result = AgentResult(agent_id=self.agent_id, phase=self.phase)

        try:
            spec: ProjectSpec = ProjectSpec.model_validate(state["project_spec"])
        except Exception as e:
            result.add_error(f"Cannot parse project_spec: {e}",
                             severity=ErrorSeverity.CRITICAL, exc=e)
            return self.log_complete(result)

        fm_dict: dict = state["field_mapping"]
        mapping = _reconstruct_mapping(fm_dict)

        tds_documents: List[dict] = []

        for ds in spec.data_sources:
            ds_id   = ds.datasource_id

            # Published datasources already exist on Tableau Cloud.
            # They use a sqlproxy connection and have no local column list.
            # Skip full TDS generation — their reference will be added by the
            # TWB assembler using the published_ds_name from data_sources.csv.
            if getattr(ds, "datasource_type", "live") == "published":
                continue

            try:
                doc = self._build_tds(spec, mapping, ds_id)
            except Exception as e:
                result.add_error(
                    f"TDS generation failed for datasource '{ds_id}': {e}",
                    severity=ErrorSeverity.CRITICAL, exc=e,
                )
                continue

            if not doc.is_valid:
                result.add_error(
                    f"TDS document for '{ds_id}' is invalid "
                    f"(connections={len(doc.connections)}, columns={len(doc.columns)})",
                    severity=ErrorSeverity.HIGH,
                )

            # Write to disk (non-blocking)
            self._write_tds(doc, result)

            tds_documents.append(doc.to_dict())

            try:
                _logger.info("tds_generated", **doc.summary())
            except Exception:
                pass

        result.output = {"tds_documents": tds_documents}
        result.metadata["tds_summary"] = {
            "datasources_processed": len(tds_documents),
            "all_valid": all(d["is_valid"] for d in tds_documents) if tds_documents else False,
        }

        if result.status == AgentStatus.PENDING:
            result.status = AgentStatus.SUCCESS

        return self.log_complete(result)

    # ── TDS construction ────────────────────────────────────────────────────

    def _build_tds(
        self,
        spec:       ProjectSpec,
        mapping:    TableauFieldMapping,
        ds_id:      str,
    ) -> TdsDocument:

        # Lookup helpers from ProjectSpec
        ds_config = next(
            (d for d in spec.data_sources if d.datasource_id == ds_id), None
        )
        caption   = ds_config.datasource_name if ds_config else ds_id
        conn_id   = ds_config.connection_id   if ds_config else ""
        conn_cfg  = next(
            (c for c in spec.connections if c.connection_id == conn_id), None
        )

        doc = TdsDocument(
            datasource_id=ds_id,
            caption=caption,
            ds_name=_make_ds_name(ds_id),
        )

        # Step 1: Connection
        if conn_cfg:
            doc.add_connection(_build_connection_spec(conn_cfg))

        # Step 2: Relations (tables + joins)
        tables = spec.get_tables_for_datasource(ds_id)
        if tables:
            primary_key = ds_config.primary_table if ds_config else None
            primary_table = next(
                (t for t in tables if t.table_id == primary_key),
                tables[0],
            )
            schema = (getattr(conn_cfg, "schema_", None) or getattr(conn_cfg, "schema", "PUBLIC")) if conn_cfg else "PUBLIC"
            _build_relations(doc, spec, tables, primary_table, schema, ds_id)

        # Step 3: Col maps
        for tbl in tables:
            for col in spec.get_columns_for_table(tbl.table_id):
                display = col.display_name or col.column_name
                doc.add_col_map(
                    key=display,
                    value=f"[{tbl.table_name}].[{col.column_name}]",
                )

        # Step 4: Raw columns from FieldMapping
        fields = mapping.fields_for_datasource(ds_id)
        if not fields:
            # Fall back: build from spec columns directly
            fields = _fields_from_spec(spec, ds_id)

        seen_names: set = set()
        for fm in fields:
            if fm.status == MappingStatus.MISSING:
                continue            # Missing columns are skipped
            col_name = fm.tableau_name.strip().strip("[]")
            if col_name.lower() in seen_names:
                continue
            seen_names.add(col_name.lower())
            doc.add_column(_field_to_column_spec(fm))

        # Step 5: Calculated columns from MetricMapping
        metrics = mapping.metrics_for_datasource(ds_id)
        for mm in metrics:
            if not mm.formula_valid:
                continue
            col_name = mm.caption.strip()
            if col_name.lower() in seen_names:
                continue
            seen_names.add(col_name.lower())
            doc.add_column(_metric_to_column_spec(mm))

        return doc

    # ── File I/O ────────────────────────────────────────────────────────────

    def _write_tds(self, doc: TdsDocument, result: AgentResult) -> None:
        """Write TDS XML to disk. Non-blocking — failures add warnings."""
        try:
            self._output_dir.mkdir(parents=True, exist_ok=True)
            path = self._output_dir / f"{doc.datasource_id}.tds"
            path.write_text(doc.to_xml(), encoding="utf-8")
        except Exception as e:
            result.add_warning(f"Could not write TDS file for '{doc.datasource_id}': {e}")


# ── ProjectSpec → RelationSpec helpers ────────────────────────────────────────

def _build_relations(
    doc:           TdsDocument,
    spec:          ProjectSpec,
    tables:        list,
    primary_table: Any,
    schema:        str,
    ds_id:         str,
) -> None:
    """
    Build <relation> elements:
      - If there is only one table → simple table relation.
      - If there are joins → join relation tree rooted at primary_table.
    """
    rels = [r for r in spec.relationships if r.datasource_id == ds_id]

    if not rels or len(tables) == 1:
        # Single-table: plain <relation type='table'>
        doc.add_relation(RelationSpec(
            relation_id=primary_table.table_id,
            table_name=primary_table.table_name,
            schema=schema,
            is_join=False,
        ))
        return

    # Multi-table: build join RelationSpecs from relationships.csv
    # Map table_id → table_name
    id_to_name = {t.table_id: t.table_name for t in tables}

    for rel in rels:
        left_name  = id_to_name.get(rel.left_table_id, rel.left_table_id)
        right_name = id_to_name.get(rel.right_table_id, rel.right_table_id)
        doc.add_relation(RelationSpec(
            relation_id=rel.relationship_id,
            table_name=f"{left_name}__{right_name}",
            schema=schema,
            is_join=True,
            join_type=rel.join_type,
            left_table=left_name,
            right_table=right_name,
            left_key=rel.left_key,
            right_key=rel.right_key,
        ))


def _build_connection_spec(conn_cfg: Any) -> ConnectionSpec:
    return ConnectionSpec(
        connection_id=conn_cfg.connection_id,
        conn_class=getattr(conn_cfg, "class_", None) or getattr(conn_cfg, "conn_class", "snowflake"),
        server=conn_cfg.server,
        dbname=conn_cfg.dbname,
        schema=getattr(conn_cfg, "schema_", None) or getattr(conn_cfg, "schema", "PUBLIC"),
        warehouse=conn_cfg.warehouse or None,
        port=conn_cfg.port or None,
        role=conn_cfg.role or None,
        username="",                # Never store usernames in XML
        auth_method=conn_cfg.auth_method,
    )


# ── FieldMapping → ColumnSpec helpers ─────────────────────────────────────────

_ROLE_TO_AGG = {
    "measure":   "Sum",
    "dimension": "None",
}

def _field_to_column_spec(fm: FieldMapping) -> ColumnSpec:
    """Convert a raw FieldMapping to a Tableau <column> spec."""
    agg = fm.aggregation or "NONE"
    # Normalise aggregation to Tableau's title-case format
    agg_clean = agg.strip().title() if agg.upper() != "NONE" else "None"
    return ColumnSpec(
        name=fm.tableau_name or f"[{fm.caption}]",
        caption=fm.caption,
        datatype=fm.datatype,
        role=fm.role,
        type_=fm.type_,
        hidden=fm.hidden,
        aggregation=agg_clean,
        description=fm.description,
        format_string=fm.format_string,
        folder_group=fm.folder_group,
        is_calculated=False,
    )


def _metric_to_column_spec(mm: MetricMapping) -> ColumnSpec:
    """Convert a MetricMapping calculated field to a Tableau <column> spec."""
    return ColumnSpec(
        name=mm.xml_name,
        caption=mm.caption,
        datatype=mm.datatype,
        role=mm.role,
        type_=mm.type_,
        hidden=False,
        aggregation="Sum",
        description=mm.description,
        format_string=mm.format_string,
        is_calculated=True,
        formula=mm.tableau_formula,
        is_lod=mm.is_lod,
    )


# ── Fallback: build FieldMapping list from ProjectSpec columns ─────────────────

def _fields_from_spec(spec: ProjectSpec, ds_id: str) -> List[FieldMapping]:
    """
    Dry-run fallback: construct minimal FieldMapping objects directly
    from ProjectSpec columns when field_mapping has no entries for this DS.
    """
    _DTYPE_ROLE = {
        "string": ("dimension", "nominal"),
        "boolean": ("dimension", "nominal"),
        "date": ("dimension", "ordinal"),
        "datetime": ("dimension", "ordinal"),
        "integer": ("measure", "quantitative"),
        "real": ("measure", "quantitative"),
    }
    results: List[FieldMapping] = []
    for tbl in spec.get_tables_for_datasource(ds_id):
        for col in spec.get_columns_for_table(tbl.table_id):
            dtype = col.datatype or "string"
            default_role, default_type = _DTYPE_ROLE.get(dtype, ("dimension", "nominal"))
            role = col.role or default_role
            type_ = default_type if role == "measure" else \
                    ("ordinal" if dtype in ("date", "datetime") else "nominal")
            display = col.display_name or col.column_name
            results.append(FieldMapping(
                field_id=col.column_id,
                column_id=col.column_id,
                table_name=tbl.table_name,
                datasource_id=ds_id,
                tableau_name=f"[{display}]",
                xml_name=f"[{display}]",
                caption=display.replace("_", " ").title(),
                datatype=dtype,
                role=role,
                type_=type_,
                aggregation="SUM" if role == "measure" else "NONE",
                hidden=col.hidden,
                folder_group=col.group,
                description=col.description,
                format_string=None,
            ))
    return results


# ── Reconstruct TableauFieldMapping from dict ──────────────────────────────────

def _reconstruct_mapping(fm_dict: dict) -> TableauFieldMapping:
    """
    Re-hydrate a TableauFieldMapping from the to_dict() output stored in state.
    Only the attributes needed for XML generation are reconstructed.
    """
    from models.field_mapping import MappingSource, MappingStatus
    mapping = TableauFieldMapping(
        project_id=fm_dict.get("project_id", ""),
        run_id=fm_dict.get("run_id", ""),
    )
    for f in fm_dict.get("fields", []):
        mapping.fields.append(FieldMapping(
            field_id=f.get("field_id", ""),
            column_id=f.get("column_id"),
            table_name=f.get("table_name", ""),
            datasource_id=f.get("datasource_id", ""),
            tableau_name=f.get("tableau_name", ""),
            xml_name=f.get("xml_name", ""),
            caption=f.get("caption", ""),
            datatype=f.get("datatype", "string"),
            role=f.get("role", "dimension"),
            type_=f.get("type", "nominal"),
            aggregation=f.get("aggregation", "NONE"),
            hidden=f.get("hidden", False),
            folder_group=f.get("folder_group"),
            description=f.get("description"),
            format_string=f.get("format_string"),
            status=MappingStatus(f.get("status", "mapped")),
            source=MappingSource(f.get("source", "db")),
            conflict_note=f.get("conflict_note"),
        ))
    for m in fm_dict.get("metrics", []):
        mapping.metrics.append(MetricMapping(
            metric_id=m.get("metric_id", ""),
            metric_name=m.get("metric_name", ""),
            datasource_id=m.get("datasource_id", ""),
            tableau_formula=m.get("tableau_formula", ""),
            datatype=m.get("datatype", "real"),
            role=m.get("role", "measure"),
            type_=m.get("type", "quantitative"),
            is_lod=m.get("is_lod", False),
            format_string=m.get("format_string"),
            description=m.get("description"),
            calc_id=m.get("calc_id", ""),
            caption=m.get("caption", ""),
            formula_valid=m.get("formula_valid", True),
            formula_errors=m.get("formula_errors", []),
        ))
    return mapping
