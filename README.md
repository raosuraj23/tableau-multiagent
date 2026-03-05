# Tableau Multi-Agent System

Automated Tableau Cloud workbook generation from MicroStrategy metric CSVs and Figma designs, always connecting to **published** Tableau datasources discovered via the Tableau Metadata API.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    MASTER ORCHESTRATOR                          │
│              (LangGraph DAG  •  SQLite state)                   │
└────────────────────────────┬────────────────────────────────────┘
                             │
                  ┌──────────▼──────────┐
                  │  Input & Validation  │  ← mstr_metrics.csv
                  │       Agent         │    mstr_attributes.csv
                  └──────────┬──────────┘    dashboard_requirements.csv
                             │               project_config.csv
              ┌──────────────┴─────────────┐
              │ parallel fan-out            │
              ▼                            ▼
  ┌────────────────────┐      ┌────────────────────────┐
  │  MSTR Metric        │      │   Figma Design Agent   │
  │  Mapper Agent       │      │                        │
  │                     │      │  PRIMARY: Figma API     │
  │  • Tableau Metadata │      │  FALLBACK: Claude Vision│
  │    API discovery    │      │  DEFAULT: 2×2 grid      │
  │  • Claude LLM       │      └────────────────────────┘
  │    formula mapping  │
  └────────────────────┘
              │                            │
              └──────────────┬─────────────┘
                             │ fan-in
                  ┌──────────▼──────────┐
                  │   TWB Generator     │  ← Deterministic XML
                  │      Agent          │    Published DS (sqlproxy)
                  └──────────┬──────────┘    Worksheets + Dashboard
                             │               Color palette
                  ┌──────────▼──────────┐
                  │  Deployment Agent   │  → Tableau Cloud
                  └─────────────────────┘    (TSC publish)
```

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Published datasources only** | No direct DB credentials in workbooks; all auth handled by Tableau Cloud |
| **Tableau Metadata API for DS discovery** | Discovers real field names/types before generating field references |
| **LLM for MSTR mapping + Figma Vision** | AI handles semantic ambiguity; XML generation stays deterministic |
| **CSV-first inputs** | Fully offline-compatible; no MSTR API dependency |
| **Figma API → Vision fallback** | Works with or without a Figma token |

---

## Agent Summary (4 specialist + orchestrator)

| # | Agent | Phase | LLM? |
|---|-------|-------|------|
| 0 | Master Orchestrator | All | No |
| 1 | Input & Validation | Intake | No |
| 2 | MSTR Metric Mapper | Parallel A | **Claude** (formula mapping) |
| 3 | Figma Design | Parallel B | **Claude Vision** (fallback) |
| 4 | TWB Generator | Assembly | No (deterministic XML) |
| 5 | Deployment | Publish | No |

---

## Quick Start (Windows)

### Prerequisites
- Python 3.12
- Anthropic API key — [console.anthropic.com](https://console.anthropic.com)
- Tableau Cloud account with Personal Access Token
- Figma Personal Access Token *(optional — image fallback available)*

### Setup

```bat
cd C:\Projects\tableau-multiagent

python -m venv .venv
.venv\Scripts\activate

pip install -r requirements.txt

copy .env.template .env
notepad .env
```

### .env template

```
ANTHROPIC_API_KEY=sk-ant-...
FIGMA_TOKEN=figd_...                  # optional
TAB_PAT_NAME=my-pat
TAB_PAT_SECRET=...
```

### Fill in your CSVs

```bat
REM Copy examples and edit
xcopy csv_inputs\examples\* csv_inputs\ /Y
```

### Run

```bat
# Validate inputs only
python cli.py validate

# Generate TWB without publishing
python cli.py run --env dev --dry-run

# Full pipeline
python cli.py run --env dev

# Full pipeline with image fallback instead of Figma API
python cli.py run --env dev --figma-image path\to\dashboard.png

# Overwrite existing workbook
python cli.py run --env prod --allow-overwrite
```

---

## CSV Input Files

All project configuration lives in `csv_inputs/`. No code changes needed for new projects.

| File | Required | Purpose |
|------|----------|---------|
| `project_config.csv` | ✅ | Project settings, Tableau site, published DS name, Figma file ID |
| `mstr_metrics.csv` | ✅ | MSTR metric definitions + formulas |
| `mstr_attributes.csv` | ✅ | MSTR attribute definitions |
| `dashboard_requirements.csv` | ✅ | Sheet specs: chart types, rows/cols shelves, layout |
| `figma_layout.csv` | ⬜ | Manual layout override (used when no Figma API/image) |

### Key fields: project_config.csv

| Column | Description | Example |
|--------|-------------|---------|
| `published_datasource_name` | Name of the published DS on Tableau Cloud | `Sales_Analytics` |
| `figma_file_id` | Figma file key (from URL) | `abc123xyz` |
| `tableau_server_url` | Tableau Cloud base URL | `https://10ax.online.tableau.com` |
| `tableau_site` | Site ID | `mycompany` |
| `target_project` | Tableau project folder | `Sales` |

### Key fields: mstr_metrics.csv

| Column | Description | Example |
|--------|-------------|---------|
| `metric_name` | Display name | `Profit Ratio` |
| `mstr_formula` | MSTR formula syntax | `Sum(Profit) / Sum(Revenue)` |
| `datatype` | real / integer / string / date | `real` |
| `format_string` | Tableau format string | `#,##0.00%` |

### Key fields: dashboard_requirements.csv

| Column | Description | Example |
|--------|-------------|---------|
| `view_id` | Unique ID | `view_001` |
| `view_type` | `worksheet` or `dashboard` | `worksheet` |
| `chart_type` | Bar / Line / Pie / Text / Map | `Bar` |
| `rows` | Rows shelf field | `Sub-Category` |
| `columns` | Columns shelf field | `SUM(Sales)` |
| `views_in_dashboard` | Pipe-separated view_ids | `view_001\|view_002` |

---

## Pipeline Flow

```
IDLE → INTAKE → MAPPING+DESIGN (parallel) → GENERATING → DEPLOYING → COMPLETE
                                                       ↓
                                              (abort on critical errors)
```

Each phase is independently checkpointed. A failed phase can be retried without
rerunning earlier phases.

---

## Removed from Previous Architecture

The following agents and concerns have been **removed** in this focused redesign:

| Removed | Reason |
|---------|--------|
| Connectivity Testing Agent | No direct DB connections — published DS only |
| Source Schema Profiler Agent | Tableau Metadata API handles schema discovery |
| Semantic Model / TDS Agent | sqlproxy pattern replaces TDS XML generation |
| Metric Definition Agent | Merged into MSTR Metric Mapper |
| Tableau Model Agent | Merged into TWB Generator |
| Dashboard Agent | Merged into TWB Generator |
| Monitoring Agent | Out of scope for workbook generation |
| Documentation Agent | Out of scope; can be added back as standalone |
| Direct DB drivers (Snowflake, Postgres) | Not needed with published DS |

---

## Security

- All credentials stored as **OS environment variables** only
- `.env` is `.gitignore`d — never committed
- Tableau PAT tokens: rotate every 90 days
- No DB passwords ever touch the workbook files (published DS handles auth)
