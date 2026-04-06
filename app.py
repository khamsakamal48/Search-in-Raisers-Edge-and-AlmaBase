"""
Alumni Search App — Dual-View Search across Raiser's Edge and AlmaBase
======================================================================

This Streamlit app searches alumni records across two PostgreSQL database
views simultaneously and displays results side by side with confidence scores.

CONFIGURATION
-------------
1. Set the following environment variables (or create a .env file):
   - DB_HOST     : PostgreSQL host (default: localhost)
   - DB_PORT     : PostgreSQL port (default: 5432)
   - DB_NAME     : Database name
   - DB_USER     : Database user
   - DB_PASSWORD  : Database password

2. Update VIEW_1 and VIEW_2 below with your actual view names and column names.

3. Run setup.sql on your database to enable required extensions and create indexes.

4. Install dependencies: pip install -r requirements.txt

5. Launch: streamlit run app.py
"""

import os
import re
from urllib.parse import quote_plus
from concurrent.futures import ThreadPoolExecutor, as_completed

import streamlit as st
import psycopg2
import psycopg2.extras
import pandas as pd
from sqlalchemy import create_engine, text
from rapidfuzz import fuzz
import jellyfish
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# View configuration — UPDATE THESE with your actual view/column names
# ---------------------------------------------------------------------------
VIEW_1 = {
    "name": "raisers_edge_view",       # <-- your Raiser's Edge view name
    "label": "Raiser's Edge",
    "columns": {
        "name": "full_name",            # column containing the person's name
        "email": "emails",              # comma-separated emails (via STRING_AGG in view)
        "phone": "phones",             # comma-separated phones (via STRING_AGG in view)
        "identifier": "constituent_id", # primary identifier column
        "roll_number": "roll_numbers",  # comma-separated roll numbers (via STRING_AGG)
        "department": "departments",    # comma-separated departments (via STRING_AGG)
        "degree": "degrees",            # comma-separated degrees (via STRING_AGG)
        "batch": "batches",             # comma-separated batches (via STRING_AGG)
    },
    "display_names": {
        "constituent_id": "RE ID",
        "full_name": "Name",
        "roll_numbers": "Roll Number(s)",
        "departments": "Department(s)",
        "degrees": "Degree(s)",
        "batches": "Batch",
        "emails": "Email(s)",
        "phones": "Phone(s)",
        "mapped_almabase_id": "AlmaBase ID (from RE)",
    },
}

VIEW_2 = {
    "name": "almabase_view",           # <-- your AlmaBase view name
    "label": "AlmaBase",
    "columns": {
        "name": "full_name",
        "email": "emails",             # comma-separated emails (via STRING_AGG in view)
        "phone": "phones",             # comma-separated phones (via STRING_AGG in view)
        "identifier": "almabase_id",
        "roll_number": "roll_numbers",
        "department": "departments",    # comma-separated departments (via STRING_AGG)
        "degree": "degrees",            # comma-separated degrees (via STRING_AGG)
        "batch": "batches",             # comma-separated batches (via STRING_AGG)
        "listed_on_directory": "listed_on_directory",
    },
    "display_names": {
        "almabase_id": "AB ID",
        "re_constituent_id": "RE ID (from AlmaBase)",
        "full_name": "Name",
        "roll_numbers": "Roll Number(s)",
        "departments": "Department(s)",
        "degrees": "Degree(s)",
        "batches": "Batch",
        "emails": "Email(s)",
        "phones": "Phone(s)",
        "listed_on_directory": "Listed on User Directory",
    },
}

# Filterable columns — these will appear as multi-select filters in the sidebar
FILTER_COLUMNS = ["department", "degree", "batch"]

# AlmaBase raw table name (Excel data is uploaded here)
ALMABASE_TABLE = "almabase_raw"

# RE alias mapping table (CSV with Constituent ID → AlmaBase ID)
RE_ALIAS_TABLE = "re_alias_mapping"

# Similarity threshold for pg_trgm (lower = more results, less precise)
TRGM_THRESHOLD = 0.3

# Score weights for combined confidence
WEIGHT_SQL = 0.40
WEIGHT_FUZZY = 0.40
WEIGHT_PHONETIC = 0.20


# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------
@st.cache_resource
def get_db_connection():
    """Create and return a PostgreSQL connection using environment variables."""
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )
        conn.set_session(autocommit=True)
        return conn
    except psycopg2.Error as e:
        st.error(f"Database connection failed: {e}")
        return None


def _get_fresh_connection():
    """Return an existing connection if healthy, otherwise reconnect."""
    conn = get_db_connection()
    if conn is None:
        return None
    try:
        conn.cursor().execute("SELECT 1")
        return conn
    except Exception:
        # Connection went stale — clear cache and retry
        get_db_connection.clear()
        return get_db_connection()


# ---------------------------------------------------------------------------
# SQLAlchemy engine (for pandas to_sql)
# ---------------------------------------------------------------------------
def _get_sqlalchemy_engine():
    """Create a SQLAlchemy engine from the same env vars as psycopg2."""
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    dbname = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    return create_engine(f"postgresql+psycopg2://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{dbname}")


# ---------------------------------------------------------------------------
# AlmaBase Excel upload and view creation
# ---------------------------------------------------------------------------
# Institution name to filter education records for (case-insensitive ILIKE)
ALMABASE_INSTITUTION = "IIT Bombay"


def _get_ordered_columns(engine) -> list[tuple[str, int]]:
    """Return (column_name, ordinal_position) pairs ordered by position."""
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT column_name, ordinal_position FROM information_schema.columns "
            f"WHERE table_name = '{ALMABASE_TABLE}' ORDER BY ordinal_position"
        ))
        return [(row[0], row[1]) for row in result]


def _find_education_slots(cols_ordered: list[tuple[str, int]]) -> list[dict]:
    """
    Walk through columns in ordinal order and group them into education slots.
    Each slot starts with an 'Institution (N)' column and ends before the next one.
    Returns a list of dicts: {institution_col, slot_cols: {name: pos}}.
    """
    inst_indices = []
    for i, (name, pos) in enumerate(cols_ordered):
        if re.match(r'^Institution \(\d+\)', name, re.IGNORECASE):
            inst_indices.append(i)

    slots = []
    for idx, start_i in enumerate(inst_indices):
        inst_name, inst_pos = cols_ordered[start_i]
        # Slot columns extend until the next Institution column (or end of list)
        end_i = inst_indices[idx + 1] if idx + 1 < len(inst_indices) else len(cols_ordered)
        slot_cols = {
            name: pos for name, pos in cols_ordered[start_i + 1:end_i]
        }
        slots.append({
            "institution_col": inst_name,
            "columns": slot_cols,
        })
    return slots


def _case_when_expr(institution_col: str, data_col: str) -> str:
    """Build a CASE WHEN expression that only returns the value if institution matches."""
    return (
        f'CASE WHEN "{institution_col}" ILIKE \'%{ALMABASE_INSTITUTION}%\' '
        f'THEN NULLIF(TRIM(CAST("{data_col}" AS TEXT)), \'\') END'
    )


def _build_filtered_concat(label: str, slots: list[dict], col_patterns: list[str]) -> str:
    """
    Build a CONCAT_WS expression that only includes education data where the
    institution matches ALMABASE_INSTITUTION.
    """
    parts = []
    for slot in slots:
        inst_col = slot["institution_col"]
        for col_name in slot["columns"]:
            for pat in col_patterns:
                if re.match(pat, col_name, re.IGNORECASE):
                    parts.append(_case_when_expr(inst_col, col_name))
                    break
    if not parts:
        return f"NULL AS {label}"
    return f"CONCAT_WS(', ', {', '.join(parts)}) AS {label}"


def _simple_concat(label: str, col_patterns: list[str], table_cols: list[str]) -> str:
    """
    Build a CONCAT_WS expression for non-education columns (emails, phones)
    that don't need institution filtering.
    """
    matched = []
    for pat in col_patterns:
        for c in table_cols:
            if re.match(pat, c, re.IGNORECASE):
                matched.append(c)
    if not matched:
        return f"NULL AS {label}"
    parts = ", ".join(f'NULLIF(TRIM(CAST("{c}" AS TEXT)), \'\')' for c in matched)
    return f'CONCAT_WS(\', \', {parts}) AS {label}'


def _create_almabase_view(engine):
    """
    Create or replace the almabase_view as a regular VIEW over almabase_raw.
    Dynamically discovers column names and builds CASE WHEN expressions so that
    department, degree, batch, and roll number values are only included when
    the corresponding Institution column matches ALMABASE_INSTITUTION.
    Emails and phones are included without institution filtering.
    """
    cols_ordered = _get_ordered_columns(engine)
    if not cols_ordered:
        return

    table_cols = [name for name, _ in cols_ordered]

    # Find education slots (each starting with an Institution column)
    edu_slots = _find_education_slots(cols_ordered)

    # Education-filtered fields (only include when institution = IIT Bombay)
    department_patterns = [r'^Department \(\d+\)']
    degree_patterns = [r'^Course \(\d+\)', r'^Degree \(\d+\)']
    batch_patterns = [r'^Year of Graduation \(\d+\)', r'^Class year \(\d+\)', r'^Class Year \(\d+\)']
    roll_patterns = [r'^Roll Number \(\d+\)', r'^Other Education Roll Number \(\d+\)']

    depts_expr = _build_filtered_concat("departments", edu_slots, department_patterns)
    degrees_expr = _build_filtered_concat("degrees", edu_slots, degree_patterns)
    batches_expr = _build_filtered_concat("batches", edu_slots, batch_patterns)
    rolls_expr = _build_filtered_concat("roll_numbers", edu_slots, roll_patterns)

    # Also include Roll No Value custom fields (not inside education slots)
    roll_custom_patterns = [r'^Roll No Value \(\d+\)']
    roll_custom = _simple_concat("_roll_custom", roll_custom_patterns, table_cols)
    # If there are custom roll fields, merge them with the education ones
    has_custom_rolls = roll_custom != "NULL AS _roll_custom"
    if has_custom_rolls:
        # Wrap both into a combined expression
        rolls_inner = rolls_expr.replace(" AS roll_numbers", "")
        roll_custom_inner = roll_custom.replace(" AS _roll_custom", "")
        rolls_expr = f"CONCAT_WS(', ', ({rolls_inner}), ({roll_custom_inner})) AS roll_numbers"

    # Non-education fields (no institution filtering needed)
    email_patterns = [r'^Address \(\d+\)', r'^Email Id \d+']
    phone_patterns = [
        r'^Mobile Phone Number$', r'^Home Phone Number$', r'^Office Phone Number$',
        r'^Home Mobile$', r'^Home Phone$', r'^Work Mobile$', r'^Work Phone$',
    ]

    emails_expr = _simple_concat("emails", email_patterns, table_cols)
    phones_expr = _simple_concat("phones", phone_patterns, table_cols)

    # Use a MATERIALIZED VIEW so we can create GIN indexes for fast search
    # Listed on User Directory column (optional — may not exist in all uploads)
    listed_col_name = 'is listed on user directory'
    has_listed_col = any(c.strip().lower().startswith(listed_col_name) for c in table_cols)
    if has_listed_col:
        listed_col = next(c for c in table_cols if c.strip().lower().startswith(listed_col_name))
        listed_expr = f',\n        LOWER(TRIM(CAST("{listed_col}" AS TEXT))) AS listed_on_directory'
    else:
        listed_expr = ""

    # Constituent Id column (RE ID stored in AlmaBase, optional)
    constituent_col_name = 'constituent id'
    has_constituent_col = any(c.strip().lower() == constituent_col_name for c in table_cols)
    if has_constituent_col:
        constituent_col = _find_col(table_cols, 'Constituent Id')
        constituent_expr = (
            f',\n        REGEXP_REPLACE(TRIM(CAST("{constituent_col}" AS TEXT)), '
            f"'\\.0$', '') AS re_constituent_id"
        )
    else:
        constituent_expr = ""

    select_sql = f"""
    SELECT
        "{_find_col(table_cols, 'Almabase Profile Id')}" AS almabase_id,
        "{_find_col(table_cols, 'Full Name')}" AS full_name,
        {rolls_expr},
        {depts_expr},
        {degrees_expr},
        {batches_expr},
        {emails_expr},
        {phones_expr}{listed_expr}{constituent_expr}
    FROM {ALMABASE_TABLE}
    """

    with engine.connect() as conn:
        conn.execute(text("DROP MATERIALIZED VIEW IF EXISTS almabase_view CASCADE"))
        conn.execute(text(f"CREATE MATERIALIZED VIEW almabase_view AS {select_sql}"))

        # Create GIN trigram indexes for fast fuzzy search
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_ab_name_trgm "
            "ON almabase_view USING gin (full_name gin_trgm_ops)"
        ))
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_ab_emails_trgm "
            "ON almabase_view USING gin (emails gin_trgm_ops)"
        ))
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_ab_phones_trgm "
            "ON almabase_view USING gin (phones gin_trgm_ops)"
        ))
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_ab_roll_numbers_trgm "
            "ON almabase_view USING gin (roll_numbers gin_trgm_ops)"
        ))
        conn.execute(text(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_ab_almabase_id "
            "ON almabase_view (almabase_id)"
        ))
        conn.commit()


def _find_col(table_cols: list[str], target: str) -> str:
    """Find a column name case-insensitively, return first match or target as-is."""
    for c in table_cols:
        if c.lower() == target.lower():
            return c
    return target


# Column name patterns we actually need from the Excel files (out of 592 columns).
# Reading only these makes the upload ~20x faster.
_NEEDED_COL_PATTERNS = [
    r'^Almabase Profile Id$',
    r'^Constituent Id$',
    r'^(First Name|Last Name|Middle Name|Full Name|Prefix)$',
    r'^Institution \(\d+\)',
    r'^Department \(\d+\)',
    r'^Course \(\d+\)',
    r'^Degree \(\d+\)',
    r'^Year of Graduation \(\d+\)',
    r'^Class year \(\d+\)',
    r'^Class Year \(\d+\)',
    r'^Roll Number \(\d+\)',
    r'^Other Education Roll Number \(\d+\)',
    r'^Roll No Value \(\d+\)',
    r'^Address \(\d+\)',
    r'^Email Id \d+',
    r'^Mobile Phone Number$',
    r'^Home Phone Number$',
    r'^Office Phone Number$',
    r'^Home Mobile$',
    r'^Home Phone$',
    r'^Work Mobile$',
    r'^Work Phone$',
    r'^Is Listed on User Directory',
]


def _get_needed_columns(all_columns: list[str]) -> list[str]:
    """Filter Excel column names to only those needed for the view."""
    needed = []
    for col in all_columns:
        for pat in _NEEDED_COL_PATTERNS:
            if re.match(pat, col.strip(), re.IGNORECASE):
                needed.append(col)
                break
    return needed


def upload_almabase_files(uploaded_files) -> tuple[bool, str]:
    """
    Read uploaded Excel files (only needed columns), concatenate them,
    upload to almabase_raw table (replacing existing data), and recreate
    the almabase_view. Returns (success, message).
    """
    import time
    import io

    try:
        progress = st.progress(0, text="Reading Excel files...")
        dfs = []
        total_files = len(uploaded_files)

        t0 = time.time()
        for i, f in enumerate(uploaded_files):
            progress.progress((i) / (total_files + 2), text=f"Reading file {i + 1}/{total_files}...")
            df = pd.read_excel(f, engine="calamine")
            dfs.append(df)
        t1 = time.time()

        progress.progress(total_files / (total_files + 2), text="Uploading to database...")
        combined = pd.concat(dfs, ignore_index=True)
        combined.dropna(how="all", inplace=True)
        st.toast(f"Read {len(combined)} rows x {len(combined.columns)} cols in {t1-t0:.1f}s")

        t2 = time.time()
        # Use a single psycopg2 connection for all DB operations to avoid lock contention
        conn_raw = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )
        try:
            with conn_raw.cursor() as cur:
                # Drop materialized view and table
                cur.execute("DROP MATERIALIZED VIEW IF EXISTS almabase_view CASCADE")
                cur.execute(f"DROP TABLE IF EXISTS {ALMABASE_TABLE}")

                # Create table with correct column types (all TEXT for simplicity)
                col_defs = ", ".join(f'"{c}" TEXT' for c in combined.columns)
                cur.execute(f"CREATE TABLE {ALMABASE_TABLE} ({col_defs})")

                # Bulk load with COPY
                buf = io.StringIO()
                combined.to_csv(buf, index=False, header=False)
                buf.seek(0)
                cols = ", ".join(f'"{c}"' for c in combined.columns)
                cur.copy_expert(
                    f"COPY {ALMABASE_TABLE} ({cols}) FROM STDIN WITH (FORMAT CSV, NULL '')",
                    buf,
                )
            conn_raw.commit()
        finally:
            conn_raw.close()
        t3 = time.time()
        st.toast(f"DB insert: {t3-t2:.1f}s")

        progress.progress((total_files + 1) / (total_files + 2), text="Creating view...")
        engine = _get_sqlalchemy_engine()
        _create_almabase_view(engine)

        # Clear cached filter options so they reload with new data
        load_filter_options.clear()
        _almabase_has_column.clear()

        row_count = len(combined)
        progress.progress(1.0, text="Done!")
        return True, f"Uploaded {row_count} records from {total_files} file(s)."

    except Exception as e:
        return False, f"Upload failed: {e}"


# ---------------------------------------------------------------------------
# RE Alias (AlmaBase ID) CSV upload
# ---------------------------------------------------------------------------
def upload_re_alias_file(uploaded_file) -> tuple[bool, str]:
    """
    Read a CSV with columns: Constituent ID, Alias Type, Alias.
    Store Constituent ID and Alias (AlmaBase ID) in re_alias_mapping table.
    Returns (success, message).
    """
    import io

    try:
        df = pd.read_csv(uploaded_file)
        # Find the relevant columns case-insensitively
        col_map = {}
        for c in df.columns:
            cl = c.strip().lower()
            if cl == "constituent id":
                col_map["constituent_id"] = c
            elif cl == "alias":
                col_map["alias"] = c

        if "constituent_id" not in col_map or "alias" not in col_map:
            return False, "CSV must have 'Constituent ID' and 'Alias' columns."

        mapped = df[[col_map["constituent_id"], col_map["alias"]]].copy()
        mapped.columns = ["constituent_id", "almabase_id"]
        mapped.dropna(subset=["constituent_id", "almabase_id"], inplace=True)
        mapped["constituent_id"] = mapped["constituent_id"].astype(str).str.strip()
        mapped["almabase_id"] = mapped["almabase_id"].astype(str).str.strip()

        conn_raw = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )
        try:
            with conn_raw.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {RE_ALIAS_TABLE}")
                cur.execute(
                    f"CREATE TABLE {RE_ALIAS_TABLE} ("
                    f"  constituent_id TEXT NOT NULL,"
                    f"  almabase_id TEXT NOT NULL"
                    f")"
                )
                buf = io.StringIO()
                mapped.to_csv(buf, index=False, header=False)
                buf.seek(0)
                cur.copy_expert(
                    f"COPY {RE_ALIAS_TABLE} (constituent_id, almabase_id) "
                    f"FROM STDIN WITH (FORMAT CSV, NULL '')",
                    buf,
                )
                cur.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_re_alias_cid "
                    f"ON {RE_ALIAS_TABLE} (constituent_id)"
                )
            conn_raw.commit()
        finally:
            conn_raw.close()

        _re_alias_table_exists.clear()
        return True, f"Uploaded {len(mapped)} alias mappings."

    except Exception as e:
        return False, f"Alias upload failed: {e}"


# ---------------------------------------------------------------------------
# Table existence checks
# ---------------------------------------------------------------------------
@st.cache_data(ttl=600)
def _re_alias_table_exists() -> bool:
    """Check if the re_alias_mapping table exists."""
    conn = _get_fresh_connection()
    if conn is None:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                f"WHERE table_name = '{RE_ALIAS_TABLE}'"
            )
            return cur.fetchone() is not None
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Filter value loading
# ---------------------------------------------------------------------------
@st.cache_data(ttl=600)
def _almabase_has_column(column_name: str) -> bool:
    """Check if the almabase_view has a specific column."""
    conn = _get_fresh_connection()
    if conn is None:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_attribute "
                "WHERE attrelid = 'almabase_view'::regclass "
                f"AND attname = '{column_name}' AND attnum > 0 AND NOT attisdropped"
            )
            return cur.fetchone() is not None
    except Exception:
        return False


def _almabase_has_listed_column() -> bool:
    return _almabase_has_column('listed_on_directory')


def _almabase_has_re_constituent_id() -> bool:
    return _almabase_has_column('re_constituent_id')


@st.cache_data(ttl=600)
def load_filter_options():
    """
    Fetch distinct values for department, degree, and batch from both views.
    Since these columns are comma-separated (STRING_AGG), we split them using
    unnest(string_to_array(...)) to get individual values for the filter dropdowns.
    Returns a dict like {"department": [...], "degree": [...], "batch": [...]}.
    """
    conn = _get_fresh_connection()
    if conn is None:
        return {col: [] for col in FILTER_COLUMNS}

    combined = {col: set() for col in FILTER_COLUMNS}
    for view_config in (VIEW_1, VIEW_2):
        view_name = view_config["name"]
        columns = view_config["columns"]
        for filter_key in FILTER_COLUMNS:
            db_col = columns.get(filter_key)
            if not db_col:
                continue
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT DISTINCT TRIM(val) AS val "
                        f"FROM {view_name}, "
                        f"unnest(string_to_array({db_col}, ',')) AS val "
                        f"WHERE TRIM(val) != '' "
                        f"ORDER BY val"
                    )
                    for row in cur.fetchall():
                        val = row[0].strip() if row[0] else ""
                        if not val:
                            continue
                        # Normalize float-like year values (e.g. "1989.0" → "1989")
                        if val.endswith(".0") and val[:-2].isdigit():
                            val = val[:-2]
                        combined[filter_key].add(val)
            except psycopg2.Error:
                pass

    return {col: sorted(vals) for col, vals in combined.items()}


# ---------------------------------------------------------------------------
# Search-type detection
# ---------------------------------------------------------------------------
def detect_search_type(query: str) -> str:
    """
    Classify the search query as 'email', 'numeric' (could be phone or roll number),
    'roll_number', or 'name'.
    """
    query = query.strip()
    if re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", query):
        return "email"
    digits_only = re.sub(r"[\s\-\+\(\).]", "", query)
    if digits_only.isdigit() and len(digits_only) >= 5:
        # Could be a phone number OR a roll number — search both fields
        return "numeric"
    if re.match(r"^[A-Za-z]?\d{2,}[A-Za-z]*\d*$", query):
        return "roll_number"
    return "name"


# ---------------------------------------------------------------------------
# SQL query building
# ---------------------------------------------------------------------------
def _build_filter_clause(columns: dict, filters: dict, params: dict) -> str:
    """
    Build SQL AND clauses for active filters.
    Since filter columns are comma-separated (STRING_AGG), we use ILIKE
    to match any selected value within the comma-separated string.
    Multiple selected values for one filter are OR'd together.
    Mutates params dict to add filter values.
    """
    clauses = []
    for filter_key, selected_values in filters.items():
        if not selected_values:
            continue
        db_col = columns.get(filter_key)
        if not db_col:
            continue
        or_parts = []
        for i, val in enumerate(selected_values):
            param_name = f"filter_{filter_key}_{i}"
            params[param_name] = f"%{val}%"
            or_parts.append(f"{db_col} ILIKE %({param_name})s")
        clauses.append(f"({' OR '.join(or_parts)})")
    return (" AND " + " AND ".join(clauses)) if clauses else ""


def build_search_query(view_name: str, columns: dict, search_term: str, search_type: str, filters: dict | None = None, limit: int = 15, listed_on_directory: bool = False):
    """
    Build a parameterized SQL query and params tuple.

    For name searches: uses pg_trgm similarity + word_similarity + soundex/metaphone.
    For other types: uses exact or ILIKE matching.
    Applies optional filters (department, degree, batch) as AND clauses.
    """
    col = columns
    filters = filters or {}
    params: dict = {}
    filter_clause = _build_filter_clause(col, filters, params)

    # Apply "Listed on User Directory" filter (only for views that have the column)
    listed_col = col.get("listed_on_directory")
    if listed_on_directory and listed_col:
        filter_clause += f" AND {listed_col} = 'yes'"

    # For Raiser's Edge view, LEFT JOIN alias mapping to show AlmaBase ID(s)
    alias_select = ""
    alias_join = ""
    has_alias = view_name == VIEW_1["name"] and _re_alias_table_exists()
    if has_alias:
        alias_select = ", _alias.mapped_almabase_id"
        alias_join = (
            f" LEFT JOIN ("
            f"   SELECT constituent_id, STRING_AGG(DISTINCT almabase_id, ', ') AS mapped_almabase_id"
            f"   FROM {RE_ALIAS_TABLE} GROUP BY constituent_id"
            f" ) _alias"
            f" ON _alias.constituent_id = CAST({view_name}.{col['identifier']} AS TEXT)"
        )

    # Cross-view lookup: additional OR conditions for identifier searches
    # RE view: also search by AlmaBase ID via alias mapping
    # AlmaBase view: also search by RE ID via re_constituent_id column
    cross_ref_clause = ""
    if search_type in ("numeric", "roll_number"):
        if has_alias:
            # Searching RE by an AlmaBase ID → match against mapped almabase_ids
            cross_ref_clause = " OR _alias.mapped_almabase_id ILIKE %(term_xref)s"
        elif view_name == VIEW_2["name"] and _almabase_has_re_constituent_id():
            # Searching AlmaBase by an RE ID → match against re_constituent_id
            cross_ref_clause = " OR CAST(re_constituent_id AS TEXT) ILIKE %(term_xref)s"

    # Qualify column references with view name to avoid ambiguity with alias join
    v = view_name

    if search_type == "name":
        name_col = col["name"]
        params.update({"term": search_term, "threshold": TRGM_THRESHOLD})
        # Use the % operator (trigram similarity) in WHERE so PostgreSQL
        # can use the GIN trigram index. soundex/dmetaphone in WHERE
        # forces a full sequential scan and are applied in Python instead.
        query = f"""
            SELECT {v}.*,
                   similarity({v}.{name_col}, %(term)s)        AS trgm_sim,
                   word_similarity(%(term)s, {v}.{name_col})    AS trgm_word_sim
                   {alias_select}
            FROM {v}
            {alias_join}
            WHERE {v}.{name_col} %% %(term)s
            {filter_clause}
            ORDER BY {v}.{name_col} <-> %(term)s
            LIMIT {limit}
        """
    elif search_type == "email":
        params["term"] = f"%{search_term}%"
        query = f"""
            SELECT {v}.*, 1.0 AS trgm_sim, 1.0 AS trgm_word_sim
                   {alias_select}
            FROM {v}
            {alias_join}
            WHERE {v}.{col['email']} ILIKE %(term)s
            {filter_clause}
            LIMIT {limit}
        """
    elif search_type == "numeric":
        # Could be a phone number or a roll number — search both fields
        digits = re.sub(r"[\s\-\+\(\).]", "", search_term)
        params["term"] = f"%{digits}%"
        params["term_raw"] = f"%{search_term}%"
        params["term_xref"] = f"%{search_term}%"
        query = f"""
            SELECT {v}.*, 1.0 AS trgm_sim, 1.0 AS trgm_word_sim
                   {alias_select}
            FROM {v}
            {alias_join}
            WHERE (regexp_replace({v}.{col['phone']}, '[^0-9]', '', 'g') LIKE %(term)s
               OR CAST({v}.{col['roll_number']} AS TEXT) ILIKE %(term_raw)s
               OR CAST({v}.{col['identifier']} AS TEXT) ILIKE %(term_raw)s
               {cross_ref_clause})
            {filter_clause}
            LIMIT {limit}
        """
    else:  # roll_number / identifier (alphanumeric like B21CS042)
        params["term"] = f"%{search_term}%"
        params["term_xref"] = f"%{search_term}%"
        query = f"""
            SELECT {v}.*, 1.0 AS trgm_sim, 1.0 AS trgm_word_sim
                   {alias_select}
            FROM {v}
            {alias_join}
            WHERE (CAST({v}.{col['roll_number']} AS TEXT) ILIKE %(term)s
               OR CAST({v}.{col['identifier']} AS TEXT) ILIKE %(term)s
               {cross_ref_clause})
            {filter_clause}
            LIMIT {limit}
        """

    return query, params


# ---------------------------------------------------------------------------
# Query execution
# ---------------------------------------------------------------------------
def execute_search(view_name: str, columns: dict, search_term: str, search_type: str, filters: dict | None = None, limit: int = 15, listed_on_directory: bool = False):
    """Execute the search query and return rows as list of dicts."""
    query, params = build_search_query(view_name, columns, search_term, search_type, filters, limit, listed_on_directory)
    try:
        # Use a dedicated connection per query so parallel searches don't block each other
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )
        conn.set_session(autocommit=True)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
            return [_dedup_csv_values(dict(r)) for r in rows], None
        finally:
            conn.close()
    except psycopg2.Error as e:
        return None, f"Query error: {e}"


def _dedup_csv_values(row: dict) -> dict:
    """Deduplicate comma-separated values in each field, preserving order."""
    for key, value in row.items():
        if isinstance(value, str) and ", " in value:
            seen = set()
            unique = []
            for item in value.split(", "):
                item = item.strip()
                # Normalize float-like year values (e.g. "1989.0" → "1989")
                if item.endswith(".0") and item[:-2].isdigit():
                    item = item[:-2]
                if item and item not in seen:
                    seen.add(item)
                    unique.append(item)
            row[key] = ", ".join(unique)
    return row


# ---------------------------------------------------------------------------
# Python-side scoring
# ---------------------------------------------------------------------------
def compute_fuzzy_score(query: str, candidate_name: str) -> float:
    """Compute a 0-100 fuzzy match score using rapidfuzz."""
    if not candidate_name:
        return 0.0
    token_sort = fuzz.token_sort_ratio(query.lower(), candidate_name.lower())
    partial = fuzz.partial_ratio(query.lower(), candidate_name.lower())
    return max(token_sort, partial)


def compute_phonetic_score(query: str, candidate_name: str) -> float:
    """
    Compute a 0-100 phonetic similarity score.
    Compares each token in the query against each token in the candidate.
    """
    if not candidate_name:
        return 0.0
    query_tokens = query.lower().split()
    cand_tokens = candidate_name.lower().split()
    if not query_tokens or not cand_tokens:
        return 0.0

    matches = 0
    total = len(query_tokens)
    for qt in query_tokens:
        qt_sdx = jellyfish.soundex(qt)
        qt_meta = jellyfish.metaphone(qt)
        for ct in cand_tokens:
            ct_sdx = jellyfish.soundex(ct)
            ct_meta = jellyfish.metaphone(ct)
            if qt_sdx == ct_sdx or qt_meta == ct_meta:
                matches += 1
                break
    return (matches / total) * 100.0 if total > 0 else 0.0


def compute_combined_score(sql_similarity: float, fuzzy_score: float, phonetic_score: float) -> float:
    """Combine scores into a single 0-100 confidence value."""
    # sql_similarity is 0-1 from pg_trgm; scale to 0-100
    sql_pct = min(sql_similarity * 100, 100)
    combined = (WEIGHT_SQL * sql_pct) + (WEIGHT_FUZZY * fuzzy_score) + (WEIGHT_PHONETIC * phonetic_score)
    return round(min(combined, 100), 1)


# ---------------------------------------------------------------------------
# Orchestration: search a single view
# ---------------------------------------------------------------------------
def search_view(view_config: dict, search_term: str, search_type: str, filters: dict | None = None, limit: int = 15, listed_on_directory: bool = False) -> tuple:
    """
    Search a single view and return (label, scored_results, error).
    scored_results is a list of dicts with an added 'confidence' key.
    """
    rows, error = execute_search(
        view_config["name"], view_config["columns"], search_term, search_type, filters, limit, listed_on_directory
    )
    if error:
        return view_config["label"], [], error
    if not rows:
        return view_config["label"], [], None

    name_col = view_config["columns"]["name"]

    scored = []
    for row in rows:
        candidate_name = row.get(name_col, "") or ""
        if search_type == "name":
            sql_sim = max(float(row.get("trgm_sim", 0)), float(row.get("trgm_word_sim", 0)))
            fuzzy = compute_fuzzy_score(search_term, candidate_name)
            phonetic = compute_phonetic_score(search_term, candidate_name)
            confidence = compute_combined_score(sql_sim, fuzzy, phonetic)
        else:
            # For exact-field searches, give high confidence
            confidence = 100.0

        # Remove internal scoring columns before display
        for key in ("trgm_sim", "trgm_word_sim"):
            row.pop(key, None)

        row["confidence"] = confidence
        scored.append(row)

    # Sort by confidence descending
    scored.sort(key=lambda r: r["confidence"], reverse=True)
    return view_config["label"], scored, None


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------
def render_results(container, results: list, view_label: str, error: str | None, display_names: dict | None = None):
    """Render search results inside a Streamlit column/container."""
    display_names = display_names or {}
    container.subheader(view_label)

    if error:
        container.error(f"Error searching {view_label}: {error}")
        return
    if not results:
        container.info("No results found.")
        return

    container.caption(f"{len(results)} result(s)")

    for row in results:
        confidence = row.pop("confidence", 0)
        # Color-code confidence
        if confidence >= 80:
            badge = f"🟢 **{confidence}%**"
        elif confidence >= 50:
            badge = f"🟡 **{confidence}%**"
        else:
            badge = f"🔴 **{confidence}%**"

        with container.expander(f"{badge}  —  {_display_name(row)}", expanded=(confidence >= 80)):
            for key, value in row.items():
                if value is not None and str(value).strip():
                    label = display_names.get(key, key)
                    st.markdown(f"**{label}:** {value}")


def _display_name(row: dict) -> str:
    """Extract a display-friendly name from a result row."""
    for key in ("full_name", "name", "first_name"):
        if key in row and row[key]:
            return str(row[key])
    # Fallback: first non-None string value
    for v in row.values():
        if v is not None and isinstance(v, str) and v.strip():
            return v
    return "(no name)"


# ---------------------------------------------------------------------------
# Reviewing section — complete record view
# ---------------------------------------------------------------------------

_RE_DETAIL_SQL = """
WITH
    target AS (
        SELECT cl.id
        FROM constituent_list cl
        INNER JOIN constituent_code_list ccl ON ccl.constituent_id = cl.id
        WHERE cl.type = 'Individual' AND ccl.description = 'Alumni' AND cl.lookup_id = %s
        LIMIT 1
    ),
    iitb_schools AS (
        SELECT constituent_id, class_of, degree, social_organization, known_name, campus AS value
        FROM school_list WHERE school = 'Indian Institute of Technology Bombay'
            AND constituent_id = (SELECT id FROM target)
        UNION
        SELECT constituent_id, class_of, degree, social_organization, known_name, majors_0 AS value
        FROM school_list WHERE school = 'Indian Institute of Technology Bombay'
            AND constituent_id = (SELECT id FROM target)
    ),
    iitb_schools_final AS (
        SELECT constituent_id,
            CONCAT_WS(E', ', MAX(class_of), MAX(degree), STRING_AGG(DISTINCT value, E', \\n'),
                MAX(social_organization), MAX(known_name)) AS iitb_education
        FROM iitb_schools GROUP BY constituent_id
    ),
    dedup_emails AS (
        SELECT constituent_id,
            CONCAT(address, CASE WHEN MAX("primary"::int) = 1 THEN ' (Primary)' END) AS address,
            MAX("primary"::int) AS primary_flag
        FROM email_list WHERE inactive = FALSE
            AND constituent_id = (SELECT id FROM target)
        GROUP BY constituent_id, address
    ),
    emails AS (
        SELECT constituent_id, STRING_AGG(address, E', \\n' ORDER BY "primary_flag" DESC) AS emails
        FROM dedup_emails GROUP BY constituent_id
    ),
    dedup_phones AS (
        SELECT constituent_id,
            CONCAT(number, CASE WHEN MAX("primary"::int) = 1 THEN ' (Primary)' END) AS number,
            MAX("primary"::int) AS primary_flag
        FROM phone_list WHERE inactive = FALSE
            AND constituent_id = (SELECT id FROM target)
        GROUP BY constituent_id, number
    ),
    phones AS (
        SELECT constituent_id, STRING_AGG(number, E', \\n' ORDER BY "primary_flag" DESC) AS phones
        FROM dedup_phones GROUP BY constituent_id
    ),
    dedup_address AS (
        SELECT constituent_id,
            CONCAT(formatted_address, CASE WHEN MAX("preferred"::int) = 1 THEN ' (Primary)' END) AS address,
            MAX("preferred"::int) AS primary_flag
        FROM address_list WHERE inactive = FALSE
            AND constituent_id = (SELECT id FROM target)
        GROUP BY constituent_id, formatted_address
    ),
    address AS (
        SELECT constituent_id, STRING_AGG(address, E'\\n\\n' ORDER BY "primary_flag" DESC) AS address
        FROM dedup_address GROUP BY constituent_id
    ),
    employment AS (
        SELECT constituent_id,
            CONCAT(
                COALESCE(position, ''),
                CASE WHEN position IS NOT NULL AND name IS NOT NULL THEN ' at ' ELSE '' END,
                COALESCE(name, ''),
                CASE
                    WHEN (start_y IS NOT NULL AND start_m IS NOT NULL AND start_d IS NOT NULL)
                        OR (end_y IS NOT NULL AND end_m IS NOT NULL AND end_d IS NOT NULL)
                    THEN CONCAT(' (',
                        COALESCE(CASE WHEN start_y IS NOT NULL AND start_m IS NOT NULL AND start_d IS NOT NULL
                            THEN TO_CHAR(MAKE_DATE(start_y::INT, start_m::INT, start_d::INT), 'DD-Mon-YYYY') END, ''),
                        CASE WHEN (start_y IS NOT NULL AND start_m IS NOT NULL AND start_d IS NOT NULL)
                            OR (end_y IS NOT NULL AND end_m IS NOT NULL AND end_d IS NOT NULL) THEN ' - ' ELSE '' END,
                        COALESCE(CASE WHEN end_y IS NOT NULL AND end_m IS NOT NULL AND end_d IS NOT NULL
                            THEN TO_CHAR(MAKE_DATE(end_y::INT, end_m::INT, end_d::INT), 'DD-Mon-YYYY') END, ''),
                        ')')
                    ELSE ''
                END
            ) AS employment,
            MAX(is_primary_business::int) AS primary_flag
        FROM relationship_list WHERE last_name IS NULL
            AND constituent_id = (SELECT id FROM target)
        GROUP BY constituent_id, name, position, start_y, start_m, start_d, end_y, end_m, end_d
    ),
    employment_final AS (
        SELECT constituent_id,
            STRING_AGG(CONCAT(employment, CASE WHEN primary_flag = 1 THEN ' (Primary)' END),
                E'; \\n' ORDER BY "primary_flag" DESC) AS employment
        FROM employment GROUP BY constituent_id
    ),
    relationships AS (
        SELECT rl.constituent_id,
            CONCAT(rl.name, ' (', rl.type, CASE WHEN rl.reciprocal_type IS NOT NULL THEN CONCAT(' / ', rl.reciprocal_type)  ELSE NULL END, ')',
                CASE WHEN cl.lookup_id IS NOT NULL THEN CONCAT(' - ', cl.lookup_id) ELSE '' END
            ) AS relationships
        FROM relationship_list rl
        LEFT JOIN constituent_list cl ON rl.relation_id = cl.id
        WHERE rl.last_name IS NOT NULL
            AND rl.constituent_id = (SELECT id FROM target)
    ),
    relationships_final AS (
        SELECT constituent_id, STRING_AGG(relationships, E'; \\n') AS relationships
        FROM relationships GROUP BY constituent_id
    ),
    non_iitb_schools AS (
        SELECT constituent_id,
            STRING_AGG(CONCAT_WS(E', ', school, class_of, degree, campus, majors_0), E'; \\n') AS education
        FROM school_list WHERE school != 'Indian Institute of Technology Bombay'
            AND constituent_id = (SELECT id FROM target)
        GROUP BY constituent_id
    ),
    verified_emails AS (
        SELECT parent_id, LOWER(value) AS value,
            STRING_AGG(DISTINCT(CONCAT(comment, ' (', TO_CHAR(date, 'DD-Mon-YYYY'), ')')), ', ') AS source
        FROM constituent_custom_fields WHERE category = 'Verified Email'
            AND parent_id = (SELECT id FROM target)
        GROUP BY parent_id, value
    ),
    verified_emails_final AS (
        SELECT parent_id, STRING_AGG(CONCAT(value, ' (', source, ')'), E'; \\n ') AS emails
        FROM verified_emails GROUP BY parent_id
    ),
    verified_phones AS (
        SELECT parent_id, value,
            STRING_AGG(DISTINCT(CONCAT(comment, ' (', TO_CHAR(date, 'DD-Mon-YYYY'), ')')), ', ') AS source
        FROM constituent_custom_fields WHERE category = 'Verified Phone'
            AND parent_id = (SELECT id FROM target)
        GROUP BY parent_id, value
    ),
    verified_phones_final AS (
        SELECT parent_id, STRING_AGG(CONCAT(value, ' (', source, ')'), E'; \\n ') AS phones
        FROM verified_phones GROUP BY parent_id
    ),
    verified_location AS (
        SELECT parent_id, value,
            STRING_AGG(DISTINCT(CONCAT(comment, ' (', TO_CHAR(date, 'DD-Mon-YYYY'), ')')), ', ') AS source
        FROM constituent_custom_fields WHERE category = 'Verified Location'
            AND parent_id = (SELECT id FROM target)
        GROUP BY parent_id, value
    ),
    verified_location_final AS (
        SELECT parent_id, STRING_AGG(CONCAT(value, ' (', source, ')'), E'; \\n ') AS location
        FROM verified_location GROUP BY parent_id
    ),
    awards AS (
        SELECT parent_id,
            STRING_AGG(DISTINCT(CONCAT(value, ' (', TO_CHAR(date, 'DD-Mon-YYYY'), ')')), E'; \\n') AS award
        FROM constituent_custom_fields WHERE category = 'Awards'
            AND parent_id = (SELECT id FROM target)
        GROUP BY parent_id
    ),
    events AS (
        SELECT parent_id,
            STRING_AGG(DISTINCT(CONCAT(value, ' (', TO_CHAR(date, 'DD-Mon-YYYY'), ')')), E'; \\n') AS events
        FROM constituent_custom_fields WHERE category = 'Events Attended'
            AND parent_id = (SELECT id FROM target)
        GROUP BY parent_id
    ),
    notes_grouped AS (
        SELECT constituent_id,
            STRING_AGG(DISTINCT(CONCAT_WS(E' \\n', type, summary, text)), E'\\n\\n') AS notes
        FROM notes WHERE constituent_id = (SELECT id FROM target)
        GROUP BY constituent_id
    ),
    constituencies AS (
        SELECT constituent_id, STRING_AGG(DISTINCT description, ', ') AS codes
        FROM constituent_code_list WHERE constituent_id = (SELECT id FROM target)
        GROUP BY constituent_id
    )
SELECT
    cl.lookup_id AS "RE ID",
    TRIM(CONCAT_WS(' ', cl.title, cl.first, cl.middle, cl.last)) AS "Name",
    cl.former_name AS "Former Name",
    cl.gender AS "Gender",
    cl.preferred_name AS "Nickname",
    cl.marital_status AS "Marital Status",
    CASE WHEN cl.birthdate_y IS NOT NULL AND cl.birthdate_m IS NOT NULL AND cl.birthdate_d IS NOT NULL
        THEN MAKE_DATE(cl.birthdate_y::int, cl.birthdate_m::int, cl.birthdate_d::int) END AS "Date of Birth",
    cl.age AS "Age",
    cl.deceased AS "Is Deceased?",
    CASE WHEN cl.deceased_date_y IS NOT NULL AND cl.deceased_date_m IS NOT NULL AND cl.deceased_date_d IS NOT NULL
        THEN MAKE_DATE(cl.deceased_date_y::int, cl.deceased_date_m::int, cl.deceased_date_d::int) END AS "Deceased Date",
    iitb.iitb_education AS "Education (IITB)",
    n_iitb.education AS "Education (non-IITB)",
    e.emails AS "Email(s)",
    p.phones AS "Phone(s)",
    a.address AS "Address(es)",
    emp.employment AS "Employment(s)",
    rel.relationships AS "Relationship(s)",
    v_emails.emails AS "Verified Email(s)",
    v_phones.phones AS "Verified Phone(s)",
    v_location.location AS "Verified Location(s)",
    aw.award AS "Award(s)",
    ev.events AS "Event(s)",
    n.notes AS "Notes",
    co.codes AS "Constituencies"
FROM constituent_list cl
    LEFT JOIN emails e ON e.constituent_id = cl.id
    LEFT JOIN iitb_schools_final iitb ON iitb.constituent_id = cl.id
    LEFT JOIN phones p ON p.constituent_id = cl.id
    LEFT JOIN address a ON a.constituent_id = cl.id
    LEFT JOIN employment_final emp ON emp.constituent_id = cl.id
    LEFT JOIN relationships_final rel ON rel.constituent_id = cl.id
    LEFT JOIN non_iitb_schools n_iitb ON n_iitb.constituent_id = cl.id
    LEFT JOIN verified_emails_final v_emails ON v_emails.parent_id = cl.id
    LEFT JOIN verified_phones_final v_phones ON v_phones.parent_id = cl.id
    LEFT JOIN verified_location_final v_location ON v_location.parent_id = cl.id
    LEFT JOIN awards aw ON aw.parent_id = cl.id
    LEFT JOIN events ev ON ev.parent_id = cl.id
    LEFT JOIN notes_grouped n ON n.constituent_id = cl.id
    LEFT JOIN constituencies co ON co.constituent_id = cl.id
WHERE cl.id = (SELECT id FROM target)
"""


@st.cache_data(ttl=300)
def fetch_re_detail(re_id: str) -> dict | None:
    """Fetch complete RE data for a single constituent by lookup_id."""
    conn = _get_fresh_connection()
    if conn is None:
        return None
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(_RE_DETAIL_SQL, (re_id,))
            row = cur.fetchone()
        return dict(row) if row else None
    except psycopg2.Error as e:
        st.error(f"RE detail query error: {e}")
        return None


@st.cache_data(ttl=300)
def fetch_almabase_detail(almabase_id: str) -> dict | None:
    """Fetch complete AlmaBase data from almabase_raw by Almabase Profile Id."""
    conn = _get_fresh_connection()
    if conn is None:
        return None
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f'SELECT * FROM {ALMABASE_TABLE} WHERE '
                f'"{_find_col_in_almabase("Almabase Profile Id")}" = %s',
                (almabase_id,),
            )
            row = cur.fetchone()
        return dict(row) if row else None
    except psycopg2.Error as e:
        st.error(f"AlmaBase detail query error: {e}")
        return None


@st.cache_data(ttl=600)
def _get_almabase_raw_columns() -> list[str]:
    """Get all column names from almabase_raw table."""
    conn = _get_fresh_connection()
    if conn is None:
        return []
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                f"WHERE table_name = '{ALMABASE_TABLE}' ORDER BY ordinal_position"
            )
            return [row[0] for row in cur.fetchall()]
    except psycopg2.Error:
        return []


def _find_col_in_almabase(target: str) -> str:
    """Find a column name in almabase_raw case-insensitively."""
    cols = _get_almabase_raw_columns()
    for c in cols:
        if c.lower() == target.lower():
            return c
    return target


# AlmaBase column grouping patterns for the Reviewing section.
# Each group: (section_name, list_of_regex_patterns)
_AB_SECTION_PATTERNS = [
    ("👤 Basic Information", [
        r'^Almabase Profile Id$',
        r'^Constituent Id$',
        r'^(First Name|Last Name|Middle Name|Full Name|Prefix|Suffix)$',
        r'^(Gender|Date of Birth|Birthdate|Birthday)$',
        r'^(Title|Salutation)$',
        r'(?i)^Birth date$',
        r'(?i)^Is Deceased$',
        r'(?i)^Marital Status$',
    ]),
    ("🎓 Education", [
        r'^Institution \(\d+\)',
        r'^Department \(\d+\)',
        r'^Course \(\d+\)',
        r'^Degree \(\d+\)',
        r'^Year of Graduation \(\d+\)',
        r'^Class year \(\d+\)',
        r'^Class Year \(\d+\)',
        r'^Roll Number \(\d+\)',
        r'^Other Education Roll Number \(\d+\)',
        r'^Roll No Value \(\d+\)',
        r'(?i)Hostel',
    ]),
    ("📧 Email(s)", [
        r'^Email Id \d+',
        r'^Address \(\d+\)',
    ]),
    ("📞 Phone(s)", [
        r'^Mobile Phone Number',
        r'^Home Phone Number',
        r'^Office Phone Number',
        r'^Home Mobile',
        r'^Home Phone',
        r'^Work Mobile',
        r'^Work Phone',
    ]),
    ("💼 Employment", [
        r'(?i)(employer|company|organization|organisation|job|occupation|position|designation|work.*title)',
        r'(?i)^(Current|Previous|Past).*(employer|company|organization|organisation)',
    ]),
    ("🏠 Address(es)", [
        r'(?i)^(city|state|country|zip|postal|pin)',
        r'(?i).*Address',
        r'(?i).*location',
        r'(?i).*residence',
        r'(?i).*Permanent',
    ]),
    ("🌐 Social & Online", [
        r'(?i)(linkedin|twitter|facebook|instagram|github|website|url|social)',
    ]),
    ("📖 Chapter", [
        r'(?i)Chapter',
    ]),
    ("⭐ Life Member", [
        r'(?i)Life Member',
    ]),
    ("📋 Directory", [
        r'^Is Listed on User Directory',
    ]),
]


def _organize_almabase_data(row: dict) -> list[tuple[str, list[tuple[str, str]]]]:
    """
    Group almabase_raw columns into display sections.
    Returns list of (section_name, [(column_name, value), ...]) where values are non-empty.
    """
    assigned = set()
    sections = []

    for section_name, patterns in _AB_SECTION_PATTERNS:
        items = []
        for col, val in row.items():
            if col in assigned:
                continue
            for pat in patterns:
                if re.match(pat, col, re.IGNORECASE):
                    if val is not None and str(val).strip():
                        items.append((col, str(val).strip()))
                    assigned.add(col)
                    break
        if items:
            sections.append((section_name, items))

    # Collect remaining non-empty columns into "Other Details"
    other_items = []
    for col, val in row.items():
        if col not in assigned and val is not None and str(val).strip():
            other_items.append((col, str(val).strip()))
    if other_items:
        sections.append(("Other Details", other_items))

    return sections


# RE detail sections for display grouping
_RE_SECTIONS = [
    ("👤 Basic Information", [
        "RE ID", "Name", "Former Name", "Gender", "Nickname",
        "Marital Status", "Date of Birth", "Age", "Is Deceased?", "Deceased Date",
    ]),
    ("🎓 Education (IITB)", ["Education (IITB)"]),
    ("🎓 Education (non-IITB)", ["Education (non-IITB)"]),
    ("📧 Email(s)", ["Email(s)"]),
    ("📞 Phone(s)", ["Phone(s)"]),
    ("🏠 Address(es)", ["Address(es)"]),
    ("💼 Employment(s)", ["Employment(s)"]),
    ("🤝 Relationship(s)", ["Relationship(s)"]),
    ("✅ Verified Email(s)", ["Verified Email(s)"]),
    ("✅ Verified Phone(s)", ["Verified Phone(s)"]),
    ("✅ Verified Location(s)", ["Verified Location(s)"]),
    ("🏆 Award(s)", ["Award(s)"]),
    ("📅 Event(s)", ["Event(s)"]),
    ("📝 Notes", ["Notes"]),
    ("🏛️ Constituencies", ["Constituencies"]),
]


def _render_re_detail(data: dict):
    """Render RE detail data as a series of expanders."""
    for section_name, fields in _RE_SECTIONS:
        items = []
        for f in fields:
            val = data.get(f)
            if val is not None and str(val).strip():
                items.append((f, str(val)))
        if not items:
            continue
        is_basic = section_name == "👤 Basic Information"
        with st.expander(section_name, expanded=is_basic):
            for label, value in items:
                # Multi-line fields: replace \n with line breaks
                if "\n" in value:
                    st.markdown(f"**{label}:**")
                    st.text(value)
                else:
                    st.markdown(f"**{label}:** {value}")


def _render_almabase_detail(data: dict):
    """Render AlmaBase detail data organized into sections."""
    sections = _organize_almabase_data(data)
    for i, (section_name, items) in enumerate(sections):
        is_first = i == 0
        with st.expander(section_name, expanded=is_first):
            # For Education section, try to render as a table grouped by institution slots
            if section_name == "🎓 Education":
                _render_education_table(items)
            else:
                for col, val in items:
                    if "\n" in val:
                        st.markdown(f"**{col}:**")
                        st.text(val)
                    else:
                        st.markdown(f"**{col}:** {val}")


def _render_education_table(items: list[tuple[str, str]]):
    """Render education items grouped by slot number into a readable table."""
    # Group items by their slot number: "Institution (1)" → slot 1
    slots: dict[str, list[tuple[str, str]]] = {}
    no_slot = []
    for col, val in items:
        m = re.search(r'\((\d+)\)', col)
        if m:
            slot_num = m.group(1)
            slots.setdefault(slot_num, []).append((col, val))
        else:
            no_slot.append((col, val))

    for slot_num in sorted(slots.keys(), key=int):
        slot_items = slots[slot_num]
        # Clean labels: remove " (N)" suffix
        cleaned = [(re.sub(r'\s*\(\d+\)$', '', col), val) for col, val in slot_items]
        inst_val = next((v for l, v in cleaned if l.lower() == "institution"), None)
        header = f"**{inst_val}**" if inst_val else f"**Education Slot {slot_num}**"
        st.markdown(header)
        for label, val in cleaned:
            if label.lower() != "institution" or not inst_val:
                st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;{label}: {val}")
        st.markdown("---")

    for col, val in no_slot:
        st.markdown(f"**{col}:** {val}")


def render_reviewing_section():
    """Render the Reviewing section — full record view by ID."""
    st.title("Alumni Review — Complete Record View")
    st.markdown("Enter an **RE ID** or **AlmaBase ID** to view the complete record.")

    col_re_input, col_ab_input = st.columns(2)
    with col_re_input:
        re_id = st.text_input("RE Constituent ID", key="review_re_id",
                              placeholder="e.g. 12345")
    with col_ab_input:
        ab_id = st.text_input("AlmaBase Profile ID", key="review_ab_id",
                              placeholder="e.g. 67890")

    if not re_id and not ab_id:
        st.info("Enter an RE ID or AlmaBase ID above to view complete record details.")
        st.stop()

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Raiser's Edge")
        if re_id and re_id.strip():
            with st.spinner("Loading RE data..."):
                re_data = fetch_re_detail(re_id.strip())
            if re_data:
                _render_re_detail(re_data)
            else:
                st.warning(f"No RE record found for ID: {re_id}")
        else:
            st.info("Enter an RE ID to view Raiser's Edge data.")

    with col_right:
        st.subheader("AlmaBase")
        if ab_id and ab_id.strip():
            with st.spinner("Loading AlmaBase data..."):
                ab_data = fetch_almabase_detail(ab_id.strip())
            if ab_data:
                _render_almabase_detail(ab_data)
            else:
                st.warning(f"No AlmaBase record found for ID: {ab_id}")
        else:
            st.info("Enter an AlmaBase ID to view AlmaBase data.")


# ---------------------------------------------------------------------------
# Main app
# ---------------------------------------------------------------------------
def main():
    st.set_page_config(page_title="Alumni Search", layout="wide")

    # --- Sidebar ---
    filter_options = load_filter_options()
    filters = {}
    listed_on_directory = False
    result_limit = 15

    with st.sidebar:
        # Navigation
        page = st.radio("Section", ["Mapping", "Reviewing"], horizontal=True,
                        label_visibility="collapsed")

        # Mapping-specific sidebar options
        if page == "Mapping":
            st.header("Filters")
            for filter_key in FILTER_COLUMNS:
                options = filter_options.get(filter_key, [])
                if options:
                    selected = st.multiselect(
                        filter_key.replace("_", " ").title(),
                        options=options,
                        default=[],
                        key=f"filter_{filter_key}",
                    )
                    if selected:
                        filters[filter_key] = selected

            st.space(size='xxsmall')

            if _almabase_has_listed_column():
                listed_on_directory = st.checkbox(
                    "View only Alums listed on AlmaBase's User Directory",
                    value=True,
                    key="listed_on_directory_filter",
                )

            result_limit = st.slider("Max results per view", min_value=5, max_value=50, value=15, step=5)

        st.divider()

        # Data upload sections (visible on both pages)
        st.header("AlmaBase Data Upload")
        uploaded_files = st.file_uploader(
            "Upload AlmaBase Excel files",
            type=["xlsx"],
            accept_multiple_files=True,
            key="almabase_upload",
        )
        if uploaded_files:
            if st.button("Upload to Database", type="primary"):
                with st.spinner("Uploading and processing..."):
                    success, message = upload_almabase_files(uploaded_files)
                if success:
                    st.success(message)
                else:
                    st.error(message)

        st.divider()

        st.header("RE Alias Upload")
        alias_file = st.file_uploader(
            "Upload RE Alias CSV (Constituent ID, Alias Type, Alias)",
            type=["csv"],
            accept_multiple_files=False,
            key="re_alias_upload",
        )
        if alias_file:
            if st.button("Upload Aliases", type="primary"):
                with st.spinner("Uploading aliases..."):
                    success, message = upload_re_alias_file(alias_file)
                if success:
                    st.success(message)
                else:
                    st.error(message)

    # --- Main content ---
    if page == "Reviewing":
        render_reviewing_section()
        return

    # --- Mapping page ---
    st.title("Alumni Mapping — Raiser's Edge & AlmaBase")
    st.markdown("Search by **name**, **email**, **phone number**, **roll number**, or any identifier.")

    search_term = st.text_input("Search", placeholder="e.g. Manish Kumar, manish@example.com, 9876543210, B21CS042")

    if not search_term or not search_term.strip():
        st.stop()

    search_term = search_term.strip()
    search_type = detect_search_type(search_term)
    st.caption(f"Detected search type: **{search_type}**")

    # Search both views in parallel
    col1, col2 = st.columns(2)

    with st.spinner("Searching…"):
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_1 = executor.submit(search_view, VIEW_1, search_term, search_type, filters, result_limit, listed_on_directory)
            future_2 = executor.submit(search_view, VIEW_2, search_term, search_type, filters, result_limit, listed_on_directory)

            for future in as_completed([future_1, future_2]):
                label, results, error = future.result()
                view_cfg = VIEW_1 if label == VIEW_1["label"] else VIEW_2
                target_col = col1 if label == VIEW_1["label"] else col2
                render_results(target_col, results, label, error, view_cfg.get("display_names"))


if __name__ == "__main__":
    main()
