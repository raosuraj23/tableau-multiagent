# Tableau Multi-Agent System

Automated Tableau Cloud workbook generation from MicroStrategy reports and Snowflake datasources using a LangGraph multi-agent pipeline.

---

## Architecture Overview

```
MicroStrategy CSVs          Snowflake Schema
  (attributes + metrics)      (tables + views)
         │                          │
         ▼                          ▼
  ┌─────────────────────────────────────────────┐
  │           MASTER ORCHESTRATOR               │
  │         (LangGraph DAG + SQLite state)      │
  └──────┬──────┬──────┬──────┬──────┬──────────┘
         │      │      │      │      │
     Intake  Validate Connect Profile Convert
         │
    ┌────┴─────────────────────────┐
    │  Claude (XML generation)     │  ← Semantic, Metric, Worksheet, Dashboard
    │  Gemini (semantic mapping)   │  ← MSTR conversion, profiling, docs
    └──────────────────────────────┘
         │
    ┌────▼───────────┐
    │ Tableau Cloud  │
    │  (published)   │
    └────────────────┘
```

### LLM Routing

| Agent | Model | Reason |
|-------|-------|--------|
| MSTR conversion | Gemini | Semantic interpretation of business logic |
| TDS / TWB XML generation | Claude | Precise, structured XML output |
| Worksheet builder | Claude | Schema-critical field references |
| Dashboard layout | Claude | Zone coordinate calculation |
| Documentation | Gemini | Natural language generation |
| Profiling / recommendations | Gemini | Data analysis reasoning |

---

## Quick Start (Windows)

### Prerequisites
- Python 3.12 ([download](https://python.org))
- Git (optional)
- Tableau Cloud account with Personal Access Token
- Snowflake account credentials
- Anthropic API key ([console.anthropic.com](https://console.anthropic.com))
- Google AI Studio key ([aistudio.google.com](https://aistudio.google.com))

### Setup

```bat
REM 1. Clone or extract the project
cd C:\Projects
git clone <repo-url> tableau-multiagent
cd tableau-multiagent

REM 2. Run the one-command setup
setup.bat

REM 3. Edit .env with your credentials
notepad .env

REM 4. Validate everything is working
python validate_setup.py

REM 5. Fill in your project CSVs
REM    (copy from csv_inputs\examples\ and edit)

REM 6. Dry run (validates + generates, no publish)
python cli.py run --env dev --dry-run

REM 7. Full run
python cli.py run --env dev
```

---

## Project Structure

```
tableau-multiagent/
├── agents/                     # Individual agent implementations
│   ├── base_agent.py           # Abstract base class
│   ├── intake_agent.py         # CSV ingestion
│   ├── validation_agent.py     # Metadata validation
│   ├── connectivity_agent.py   # Connection testing
│   ├── profiler_agent.py       # Snowflake schema profiling
│   ├── conversion_agent.py     # MSTR → Tableau mapping (Gemini)
│   ├── semantic_agent.py       # TDS XML generation (Claude)
│   ├── metric_agent.py         # Calculated field XML (Claude)
│   ├── tableau_model_agent.py  # Worksheet XML (Claude)
│   ├── dashboard_agent.py      # Dashboard zone XML (Claude)
│   ├── qa_agent.py             # Testing & validation
│   ├── deployment_agent.py     # Tableau Cloud publish
│   ├── monitoring_agent.py     # Post-publish health
│   └── documentation_agent.py # Auto-doc generation (Gemini)
│
├── orchestrator/
│   ├── orchestrator.py         # LangGraph DAG
│   ├── state_machine.py        # Phase transitions
│   └── retry_engine.py         # Tenacity retry logic
│
├── config/
│   ├── settings.yaml           # Global config
│   └── llm_config.yaml         # Per-agent LLM routing
│
├── csv_inputs/                 # Your project data (fill these in)
│   ├── project_config.csv
│   ├── data_sources.csv
│   ├── connections.csv
│   ├── auth.csv
│   ├── tables.csv
│   ├── columns.csv
│   ├── relationships.csv
│   ├── metrics.csv
│   ├── dimensions.csv
│   ├── dashboard_requirements.csv
│   ├── mstr_attributes.csv     # Export from MicroStrategy
│   └── mstr_metrics.csv        # Export from MicroStrategy
│
├── state/
│   ├── state.db                # SQLite state store
│   ├── checkpoints/            # LangGraph checkpoints
│   └── snapshots/              # Phase output snapshots
│
├── models/
│   ├── tds/                    # Generated TDS datasource files
│   └── twb/                    # Generated TWB workbook files
│
├── tableau/
│   ├── output/                 # Final .twb / .twbx files
│   └── templates/              # Base TWB templates
│
├── tests/
│   ├── unit/
│   ├── integration/
│   └── regression/
│
├── logs/
│   ├── orchestrator.log
│   ├── agents.log
│   └── audit.log
│
├── cli.py                      # CLI entrypoint
├── validate_setup.py           # Environment validator
├── setup.bat                   # Windows one-command setup
├── requirements.txt
├── .env.template               # Credentials template
└── README.md
```

---

## CSV Input Files

All project configuration lives in `csv_inputs/`. The system is **fully driven by these CSVs** — no code changes required for new projects.

| File | Purpose |
|------|---------|
| `project_config.csv` | Project settings, Tableau site, target workbook name |
| `data_sources.csv` | Datasource definitions (published vs live vs extract) |
| `connections.csv` | Snowflake connection parameters |
| `auth.csv` | Credential env var references (no actual secrets) |
| `tables.csv` | Database tables / views used |
| `columns.csv` | Column definitions, types, roles |
| `relationships.csv` | Table join definitions |
| `metrics.csv` | Calculated field definitions |
| `dimensions.csv` | Custom dimension / hierarchy definitions |
| `dashboard_requirements.csv` | Sheet types, chart types, layout spec |
| `mstr_attributes.csv` | MicroStrategy attribute export |
| `mstr_metrics.csv` | MicroStrategy metric / formula export |

See `csv_inputs/examples/` for pre-filled sample files.

---

## CLI Commands

```bat
# Full pipeline
python cli.py run --env dev

# Validate CSVs only (no generation)
python cli.py validate --csv-dir csv_inputs\

# Dry run (generate TWB but don't publish)
python cli.py run --env dev --dry-run

# Resume from a specific phase
python cli.py run --phase semantic

# Check current workflow state
python cli.py status

# Rollback last Tableau Cloud deployment
python cli.py rollback

# Overwrite existing workbook on prod
python cli.py run --env prod --allow-overwrite
```

---

## Workflow Phases

```
IDLE → INTAKE → VALIDATING → CONNECTING → PROFILING
→ CONVERTING → MODELING → GENERATING → TESTING
→ DEPLOYING → MONITORING → COMPLETE
```

Each phase is independently retriable. Failed phases are checkpointed — resume from the failure point without rerunning earlier phases.

---

## Security

- All credentials stored as **OS environment variables** only
- `auth.csv` contains env var **names**, never actual values
- `.env` is in `.gitignore` — never committed to git
- Snowflake password masked in all log output
- Tableau PAT rotation recommended every 90 days

---

## Development Artifact Sequence

| # | Artifact | Status |
|---|----------|--------|
| 1 | Project Scaffold (this) | ✅ Complete |
| 2 | CSV Input Templates (all 12 files) | ⏳ Next |
| 3 | `agents/base_agent.py` | Pending |
| 4 | `agents/intake_agent.py` | Pending |
| 5 | `agents/validation_agent.py` | Pending |
| 6 | `agents/semantic_agent.py` | Pending |
| 7 | `agents/metric_agent.py` | Pending |
| 8 | `agents/tableau_model_agent.py` | Pending |
| 9 | `agents/dashboard_agent.py` | Pending |
| 10 | `agents/deployment_agent.py` | Pending |
| 11 | `orchestrator/orchestrator.py` | Pending |
| 12 | `cli.py` | Pending |
| 13 | `tests/` | Pending |
