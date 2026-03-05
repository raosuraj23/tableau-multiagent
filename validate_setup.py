"""
validate_setup.py
=================
Validates the Tableau Multi-Agent System environment.
Run after setup.bat to confirm everything is ready before first use.

Usage:
    python validate_setup.py
    python validate_setup.py --skip-api-calls    # skips live credential tests
"""

import sys
import os
import importlib
import subprocess
from pathlib import Path
from typing import List, Tuple

# ── Colour helpers (no dependencies) ──────────────────────────────────────────
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

def ok(msg):    print(f"  {GREEN}[OK]{RESET}   {msg}")
def warn(msg):  print(f"  {YELLOW}[WARN]{RESET} {msg}")
def fail(msg):  print(f"  {RED}[FAIL]{RESET} {msg}")
def info(msg):  print(f"  {CYAN}[INFO]{RESET} {msg}")
def header(msg): print(f"\n{BOLD}{msg}{RESET}")

# ── Check: Python version ──────────────────────────────────────────────────────
def check_python() -> bool:
    header("1. Python Version")
    major, minor = sys.version_info[:2]
    version_str = f"{major}.{minor}.{sys.version_info[2]}"
    if major == 3 and minor == 12:
        ok(f"Python {version_str}")
        return True
    elif major == 3 and minor >= 11:
        warn(f"Python {version_str} (expected 3.12.x — should work)")
        return True
    else:
        fail(f"Python {version_str} — requires 3.12+")
        return False

# ── Check: required packages ───────────────────────────────────────────────────
REQUIRED_PACKAGES = [
    # LangChain ecosystem — versions solved together by pip
    ("langgraph",               "langgraph"),
    ("langchain",               "langchain"),
    ("langchain_core",          "langchain-core"),
    ("langchain_anthropic",     "langchain-anthropic"),
    ("langchain_google_genai",  "langchain-google-genai"),
    # LLM SDKs
    ("anthropic",               "anthropic"),
    ("google.genai",            "google-genai"),
    # Tableau
    ("tableauserverclient",     "tableauserverclient"),
    # XML / Data
    ("lxml",                    "lxml"),
    ("pandas",                  "pandas"),
    # Snowflake
    ("snowflake.connector",     "snowflake-connector-python"),
    # Web / API
    ("requests",                "requests"),
    # CLI / Logging
    ("click",                   "click"),
    ("rich",                    "rich"),
    ("structlog",               "structlog"),
    ("dotenv",                  "python-dotenv"),
    ("tenacity",                "tenacity"),
    ("yaml",                    "pyyaml"),
    # Validation
    ("jsonschema",              "jsonschema"),
    ("pydantic",                "pydantic"),
    # Utilities
    ("jinja2",                  "jinja2"),
    ("docx",                    "python-docx"),
]

def check_packages() -> Tuple[int, int]:
    header("2. Required Packages")
    passed = 0
    failed = 0
    for import_name, pkg_name in REQUIRED_PACKAGES:
        try:
            importlib.import_module(import_name)
            # Show installed version
            try:
                import importlib.metadata as meta
                version = meta.version(pkg_name)
                ok(f"{pkg_name}=={version}")
            except Exception:
                ok(f"{pkg_name}")
            passed += 1
        except ImportError:
            fail(f"{pkg_name}  →  pip install {pkg_name}")
            failed += 1
    return passed, failed

# ── Check: .env file and required keys ────────────────────────────────────────
REQUIRED_ENV_KEYS = [
    ("ANTHROPIC_API_KEY",    "Anthropic Claude API key"),
    ("GOOGLE_API_KEY",       "Google Gemini API key"),
    ("TABLEAU_SERVER_URL",   "Tableau Cloud server URL"),
    ("TABLEAU_SITE_ID",      "Tableau Cloud site ID"),
    ("TABLEAU_PAT_NAME",     "Tableau PAT name"),
    ("TABLEAU_PAT_SECRET",   "Tableau PAT secret"),
    ("SNOWFLAKE_ACCOUNT",    "Snowflake account URL"),
    ("SNOWFLAKE_DATABASE",   "Snowflake database"),
    ("SNOWFLAKE_WAREHOUSE",  "Snowflake warehouse"),
    ("SNOWFLAKE_USER",       "Snowflake username"),
    ("SNOWFLAKE_PASSWORD",   "Snowflake password"),
]

def check_env_file() -> Tuple[int, int]:
    header("3. Environment Variables (.env)")
    env_path = Path(".env")
    if not env_path.exists():
        fail(".env file not found — run setup.bat or: copy .env.template .env")
        return 0, len(REQUIRED_ENV_KEYS)

    ok(".env file found")

    # Load .env manually (avoid importing dotenv before it's confirmed installed)
    env_values = {}
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                env_values[key.strip()] = value.strip()

    passed = 0
    failed = 0
    for key, description in REQUIRED_ENV_KEYS:
        value = env_values.get(key, "")
        if not value or value.startswith("your_"):
            warn(f"{key}  — not set yet  ({description})")
            failed += 1
        else:
            # Mask secret values in output
            if any(x in key.lower() for x in ["secret", "password", "key"]):
                display = value[:4] + "..." + value[-2:] if len(value) > 8 else "***"
            else:
                display = value
            ok(f"{key} = {display}")
            passed += 1
    return passed, failed

# ── Check: required directories ───────────────────────────────────────────────
REQUIRED_DIRS = [
    "agents", "orchestrator", "config", "csv_inputs",
    "state", "state/checkpoints", "state/snapshots",
    "logs", "models/tds", "models/twb",
    "tableau/output", "tableau/templates",
    "tests/unit", "tests/integration", "tests/regression",
    "docs", "deployment",
]

def check_directories() -> bool:
    header("4. Project Directories")
    all_ok = True
    for d in REQUIRED_DIRS:
        p = Path(d)
        if p.exists():
            ok(str(d))
        else:
            warn(f"{d}  — missing, creating now...")
            p.mkdir(parents=True, exist_ok=True)
            ok(f"{d}  — created")
    return all_ok

# ── Check: required config files ──────────────────────────────────────────────
REQUIRED_CONFIG_FILES = [
    "config/settings.yaml",
    "config/llm_config.yaml",
    "requirements.txt",
    ".env.template",
]

def check_config_files() -> bool:
    header("5. Configuration Files")
    all_ok = True
    for f in REQUIRED_CONFIG_FILES:
        p = Path(f)
        if p.exists():
            ok(f"{f}  ({p.stat().st_size} bytes)")
        else:
            fail(f"{f}  — MISSING")
            all_ok = False
    return all_ok

# ── Check: CSV examples ────────────────────────────────────────────────────────
def check_csv_inputs() -> None:
    header("6. CSV Input Files")
    csv_dir = Path("csv_inputs")
    csv_files = list(csv_dir.glob("*.csv")) if csv_dir.exists() else []
    if csv_files:
        ok(f"Found {len(csv_files)} CSV file(s) in csv_inputs/")
        for f in csv_files:
            info(f"  {f.name}")
    else:
        warn("No CSV files found in csv_inputs/ yet")
        info("Copy files from csv_inputs/examples/ and fill in your data")
        info("(CSV templates provided in Artifact #2)")

# ── Check: live API connections (optional) ─────────────────────────────────────
def check_tableau_connection(env_values: dict) -> None:
    header("7. Tableau Cloud Connection (live test)")
    url     = env_values.get("TABLEAU_SERVER_URL", "")
    pat_n   = env_values.get("TABLEAU_PAT_NAME", "")
    pat_s   = env_values.get("TABLEAU_PAT_SECRET", "")
    site_id = env_values.get("TABLEAU_SITE_ID", "")

    if not all([url, pat_n, pat_s, site_id]) or any(
        v.startswith("your_") for v in [url, pat_n, pat_s, site_id]
    ):
        warn("Tableau credentials not configured — skipping live test")
        return

    try:
        import tableauserverclient as TSC
        auth = TSC.PersonalAccessTokenAuth(pat_n, pat_s, site_id=site_id)
        server = TSC.Server(url, use_server_version=True)
        with server.auth.sign_in(auth):
            ok(f"Tableau Cloud authenticated  →  {url}  (site: {site_id})")
            projects, _ = server.projects.get()
            info(f"  Found {len(projects)} project(s)")
    except Exception as e:
        fail(f"Tableau Cloud connection failed: {e}")

def check_snowflake_connection(env_values: dict) -> None:
    header("8. Snowflake Connection (live test)")

    acct = env_values.get("SNOWFLAKE_ACCOUNT", "")
    user = env_values.get("SNOWFLAKE_USER", "")
    pwd  = env_values.get("SNOWFLAKE_PASSWORD", "")
    db   = env_values.get("SNOWFLAKE_DATABASE", "")
    schema = env_values.get("SNOWFLAKE_SCHEMA", "")
    wh   = env_values.get("SNOWFLAKE_WAREHOUSE", "")
    role = env_values.get("SNOWFLAKE_ROLE", "")

    private_key_path = env_values.get("SNOWFLAKE_PRIVATE_KEY_PATH", "")
    private_key_passphrase = env_values.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "")

    if not acct:
        warn("Snowflake account not configured — skipping live test")
        return

    try:
        import snowflake.connector

        conn_params = {
            "account": acct,
            "database": db,
            "warehouse": wh,
            "schema": schema,
            "role": role,
        }

        # -------------------------------
        # KEY PAIR AUTHENTICATION
        # -------------------------------
        if private_key_path:
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import serialization

            if not user:
                fail("SNOWFLAKE_USER required for key-pair authentication")
                return

            with open(private_key_path, "rb") as key_file:
                p_key = serialization.load_pem_private_key(
                    key_file.read(),
                    password=private_key_passphrase.encode()
                    if private_key_passphrase else None,
                    backend=default_backend()
                )

            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )

            conn_params.update({
                "user": user,
                "private_key": pkb
            })

            auth_type = "KEY_PAIR"

        # -------------------------------
        # USERNAME / PASSWORD AUTH
        # -------------------------------
        elif user and pwd:
            conn_params.update({
                "user": user,
                "password": pwd
            })
            auth_type = "USERNAME_PASSWORD"

        else:
            warn("Snowflake credentials not configured properly — skipping live test")
            return

        # -------------------------------
        # CONNECT
        # -------------------------------
        conn = snowflake.connector.connect(**conn_params)
        cur = conn.cursor()

        cur.execute("""
            SELECT CURRENT_USER(), 
                   CURRENT_DATABASE(), 
                   CURRENT_SCHEMA(),
                   CURRENT_WAREHOUSE(), 
                   CURRENT_ROLE()
        """)

        row = cur.fetchone()

        ok(
            f"Snowflake connected via {auth_type} → "
            f"user={row[0]}, db={row[1]}, schema={row[2]}, "
            f"wh={row[3]}, role={row[4]}"
        )

        cur.close()
        conn.close()

    except Exception as e:
        fail(f"Snowflake connection failed: {e}")

def check_anthropic_key(env_values: dict) -> None:
    header("9. Anthropic API Key (live test)")
    key = env_values.get("ANTHROPIC_API_KEY", "")
    if not key or key.startswith("your_"):
        warn("ANTHROPIC_API_KEY not configured — skipping")
        return
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=key)
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=10,
            messages=[{"role": "user", "content": "Hi"}]
        )
        ok(f"Anthropic API connected  →  model: {msg.model}")
    except Exception as e:
        fail(f"Anthropic API failed: {e}")

def check_gemini_key(env_values: dict) -> None:
    header("10. Google Gemini API Key (live test)")
    key = env_values.get("GOOGLE_API_KEY", "")
    if not key or key.startswith("your_"):
        warn("GOOGLE_API_KEY not configured — skipping")
        return

    try:
        from google import genai as google_genai
        client = google_genai.Client(api_key=key)

        # Step 1: Discover available models for this API key
        try:
            available = []
            for m in client.models.list():
                name = m.name or ""
                actions = m.supported_actions or []
                # New SDK uses 'generateContent' in supported_actions
                if "generateContent" in actions and "gemini" in name.lower():
                    # Strip 'models/' prefix if present — generate_content wants bare name
                    bare = name.replace("models/", "")
                    available.append(bare)
            if available:
                info(f"  Models available: {available[:6]}")
            else:
                warn("  No generateContent-capable Gemini models found for this API key")
                return
        except Exception as e:
            warn(f"  Could not list models: {e}")
            # Fall back to well-known names
            available = ["gemini-2.0-flash", "gemini-2.0-flash-lite",
                         "gemini-1.5-flash", "gemini-1.5-pro"]

        # Step 2: Try generation with first available model
        last_error = None
        for model_name in available[:4]:
            try:
                resp = client.models.generate_content(
                    model=model_name,
                    contents="Reply with one word: ok",
                )
                ok(f"Gemini API connected  →  model: {model_name}, "
                   f"response: {resp.text.strip()}")
                return
            except Exception as e:
                last_error = e
                continue

        fail(f"Gemini generation failed on all tried models. Last error: {last_error}")
        info("  Run: pip install --upgrade google-genai")

    except ImportError:
        fail("google-genai package not installed")
        info("  Run: pip install google-genai")
    except Exception as e:
        fail(f"Gemini API failed: {e}")


# ── Check: CSV cross-reference integrity ──────────────────────────────────────
REQUIRED_CSV_FILES = [
    "project_config.csv",
    "data_sources.csv",
    "connections.csv",
    "auth.csv",
    "tables.csv",
    "columns.csv",
    "relationships.csv",
    "metrics.csv",
    "dimensions.csv",
    "dashboard_requirements.csv",
    "mstr_attributes.csv",
    "mstr_metrics.csv",
]

def check_csv_cross_references() -> None:
    """Basic FK integrity check across the CSV input files."""
    header("11. CSV Cross-Reference Integrity")
    csv_dir = Path("csv_inputs")
    if not csv_dir.exists():
        warn("csv_inputs/ directory not found")
        return

    try:
        import pandas as pd

        missing = []
        dfs = {}
        for fname in REQUIRED_CSV_FILES:
            fpath = csv_dir / fname
            if fpath.exists():
                dfs[fname] = pd.read_csv(fpath, dtype=str).fillna("")
                ok(f"{fname}  ({len(dfs[fname])} row(s))")
            else:
                warn(f"{fname}  — not found yet")
                missing.append(fname)

        if missing:
            info(f"  {len(missing)} file(s) missing — skipping FK checks")
            return

        # FK: data_sources.connection_id → connections.connection_id
        ds_conn_ids   = set(dfs["data_sources.csv"]["connection_id"].unique())
        valid_conn_ids = set(dfs["connections.csv"]["connection_id"].unique())
        bad = ds_conn_ids - valid_conn_ids
        if bad:
            fail(f"data_sources.connection_id has unknown values: {bad}")
        else:
            ok("data_sources.connection_id → connections FK valid")

        # FK: tables.datasource_id → data_sources.datasource_id
        tbl_ds_ids    = set(dfs["tables.csv"]["datasource_id"].unique())
        valid_ds_ids  = set(dfs["data_sources.csv"]["datasource_id"].unique())
        bad = tbl_ds_ids - valid_ds_ids
        if bad:
            fail(f"tables.datasource_id has unknown values: {bad}")
        else:
            ok("tables.datasource_id → data_sources FK valid")

        # FK: columns.table_id → tables.table_id
        col_tbl_ids    = set(dfs["columns.csv"]["table_id"].unique())
        valid_tbl_ids  = set(dfs["tables.csv"]["table_id"].unique())
        bad = col_tbl_ids - valid_tbl_ids
        if bad:
            fail(f"columns.table_id has unknown values: {bad}")
        else:
            ok("columns.table_id → tables FK valid")

        # FK: metrics.datasource_id → data_sources.datasource_id
        met_ds_ids = set(dfs["metrics.csv"]["datasource_id"].unique())
        bad = met_ds_ids - valid_ds_ids
        if bad:
            fail(f"metrics.datasource_id has unknown values: {bad}")
        else:
            ok("metrics.datasource_id → data_sources FK valid")

        # Check: mstr_metrics has tableau_formula for all mapped rows
        mapped = dfs["mstr_metrics.csv"][dfs["mstr_metrics.csv"]["conversion_status"] == "mapped"]
        empty_formula = mapped[mapped["tableau_formula"] == ""]
        if len(empty_formula) > 0:
            warn(f"{len(empty_formula)} mapped mstr_metrics row(s) missing tableau_formula")
        else:
            ok(f"All {len(mapped)} mstr_metrics mapped rows have tableau_formula")

    except Exception as e:
        fail(f"CSV cross-reference check failed: {e}")

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    skip_api = "--skip-api-calls" in sys.argv

    print(f"\n{BOLD}{'='*60}{RESET}")
    print(f"{BOLD}  Tableau Multi-Agent System — Setup Validator{RESET}")
    print(f"{BOLD}{'='*60}{RESET}")

    errors   = 0
    warnings = 0

    # 1. Python
    if not check_python():
        errors += 1

    # 2. Packages
    pkg_ok, pkg_fail = check_packages()
    errors += pkg_fail
    if pkg_fail:
        info(f"  Install missing packages: pip install -r requirements.txt")

    # 3. .env
    env_ok, env_fail = check_env_file()
    warnings += env_fail

    # Load env for live tests
    env_values = {}
    if Path(".env").exists():
        with open(".env", "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    env_values[k.strip()] = v.strip()

    # 4. Directories
    check_directories()

    # 5. Config files
    if not check_config_files():
        errors += 1

    # 6. CSV inputs
    check_csv_inputs()

    # 6b. CSV cross-reference integrity (if files present)
    check_csv_cross_references()

    # 7-10. Live API tests (skippable)
    if not skip_api:
        check_tableau_connection(env_values)
        check_snowflake_connection(env_values)
        check_anthropic_key(env_values)
        check_gemini_key(env_values)
    else:
        info("\n[Skipping live API connection tests (--skip-api-calls)]")

    # ── Summary ──────────────────────────────────────────────────────────────
    print(f"\n{BOLD}{'='*60}{RESET}")
    if errors == 0 and warnings == 0:
        print(f"{GREEN}{BOLD}  ✓ All checks passed — system is ready!{RESET}")
        sys.exit(0)
    elif errors == 0:
        print(f"{YELLOW}{BOLD}  ⚠ Setup complete with {warnings} warning(s){RESET}")
        print(f"    Fill in .env credentials, then re-run this script.")
        sys.exit(0)
    else:
        print(f"{RED}{BOLD}  ✗ {errors} error(s), {warnings} warning(s) — fix above issues{RESET}")
        sys.exit(1)

if __name__ == "__main__":
    main()
