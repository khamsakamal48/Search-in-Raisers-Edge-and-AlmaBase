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
    },
    "display_names": {
        "almabase_id": "AB ID",
        "full_name": "Name",
        "roll_numbers": "Roll Number(s)",
        "departments": "Department(s)",
        "degrees": "Degree(s)",
        "batches": "Batch",
        "emails": "Email(s)",
        "phones": "Phone(s)",
    },
}

# Filterable columns — these will appear as multi-select filters in the sidebar
FILTER_COLUMNS = ["department", "degree", "batch"]

# AlmaBase raw table name (Excel data is uploaded here)
ALMABASE_TABLE = "almabase_raw"

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
    select_sql = f"""
    SELECT
        "{_find_col(table_cols, 'Almabase Profile Id')}" AS almabase_id,
        "{_find_col(table_cols, 'Full Name')}" AS full_name,
        {rolls_expr},
        {depts_expr},
        {degrees_expr},
        {batches_expr},
        {emails_expr},
        {phones_expr}
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
]


def _get_needed_columns(all_columns: list[str]) -> list[str]:
    """Filter Excel column names to only those needed for the view."""
    needed = []
    for col in all_columns:
        for pat in _NEEDED_COL_PATTERNS:
            if re.match(pat, col, re.IGNORECASE):
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
            header_df = pd.read_excel(f, engine="calamine", nrows=0)
            needed_cols = _get_needed_columns(list(header_df.columns))
            f.seek(0)
            df = pd.read_excel(f, engine="calamine", usecols=needed_cols)
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

        row_count = len(combined)
        progress.progress(1.0, text="Done!")
        return True, f"Uploaded {row_count} records from {total_files} file(s)."

    except Exception as e:
        return False, f"Upload failed: {e}"


# ---------------------------------------------------------------------------
# Filter value loading
# ---------------------------------------------------------------------------
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


def build_search_query(view_name: str, columns: dict, search_term: str, search_type: str, filters: dict | None = None, limit: int = 15):
    """
    Build a parameterized SQL query and params tuple.

    For name searches: uses pg_trgm similarity + word_similarity + soundex/metaphone.
    For other types: uses exact or ILIKE matching.
    Applies optional filters (department, degree, batch) as AND clauses.
    """
    col = columns
    filters = filters or {}
    params: dict = {}

    if search_type == "name":
        name_col = col["name"]
        filter_clause = _build_filter_clause(col, filters, params)
        params.update({"term": search_term, "threshold": TRGM_THRESHOLD})
        # Use the % operator (trigram similarity) in WHERE so PostgreSQL
        # can use the GIN trigram index. soundex/dmetaphone in WHERE
        # forces a full sequential scan and are applied in Python instead.
        query = f"""
            SELECT *,
                   similarity({name_col}, %(term)s)        AS trgm_sim,
                   word_similarity(%(term)s, {name_col})    AS trgm_word_sim
            FROM {view_name}
            WHERE {name_col} %% %(term)s
            {filter_clause}
            ORDER BY {name_col} <-> %(term)s
            LIMIT {limit}
        """
    elif search_type == "email":
        filter_clause = _build_filter_clause(col, filters, params)
        params["term"] = f"%{search_term}%"
        query = f"""
            SELECT *, 1.0 AS trgm_sim, 1.0 AS trgm_word_sim
            FROM {view_name}
            WHERE {col['email']} ILIKE %(term)s
            {filter_clause}
            LIMIT {limit}
        """
    elif search_type == "numeric":
        # Could be a phone number or a roll number — search both fields
        digits = re.sub(r"[\s\-\+\(\).]", "", search_term)
        filter_clause = _build_filter_clause(col, filters, params)
        params["term"] = f"%{digits}%"
        params["term_raw"] = f"%{search_term}%"
        query = f"""
            SELECT *, 1.0 AS trgm_sim, 1.0 AS trgm_word_sim
            FROM {view_name}
            WHERE regexp_replace({col['phone']}, '[^0-9]', '', 'g') LIKE %(term)s
               OR CAST({col['roll_number']} AS TEXT) ILIKE %(term_raw)s
               OR CAST({col['identifier']} AS TEXT) ILIKE %(term_raw)s
            {filter_clause}
            LIMIT {limit}
        """
    else:  # roll_number / identifier (alphanumeric like B21CS042)
        filter_clause = _build_filter_clause(col, filters, params)
        params["term"] = f"%{search_term}%"
        query = f"""
            SELECT *, 1.0 AS trgm_sim, 1.0 AS trgm_word_sim
            FROM {view_name}
            WHERE (CAST({col['roll_number']} AS TEXT) ILIKE %(term)s
               OR CAST({col['identifier']} AS TEXT) ILIKE %(term)s)
            {filter_clause}
            LIMIT {limit}
        """

    return query, params


# ---------------------------------------------------------------------------
# Query execution
# ---------------------------------------------------------------------------
def execute_search(view_name: str, columns: dict, search_term: str, search_type: str, filters: dict | None = None, limit: int = 15):
    """Execute the search query and return rows as list of dicts."""
    query, params = build_search_query(view_name, columns, search_term, search_type, filters, limit)
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
def search_view(view_config: dict, search_term: str, search_type: str, filters: dict | None = None, limit: int = 15) -> tuple:
    """
    Search a single view and return (label, scored_results, error).
    scored_results is a list of dicts with an added 'confidence' key.
    """
    rows, error = execute_search(
        view_config["name"], view_config["columns"], search_term, search_type, filters, limit
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
# Main app
# ---------------------------------------------------------------------------
def main():
    st.set_page_config(page_title="Alumni Search", layout="wide")
    st.title("Alumni Search — Raiser's Edge & AlmaBase")
    st.markdown("Search by **name**, **email**, **phone number**, **roll number**, or any identifier.")

    # --- Sidebar ---
    filter_options = load_filter_options()
    filters = {}
    with st.sidebar:
        # Filters section (used frequently)
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

        result_limit = st.slider("Max results per view", min_value=5, max_value=50, value=15, step=5)

        st.divider()

        # AlmaBase upload section (used occasionally)
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
            future_1 = executor.submit(search_view, VIEW_1, search_term, search_type, filters, result_limit)
            future_2 = executor.submit(search_view, VIEW_2, search_term, search_type, filters, result_limit)

            for future in as_completed([future_1, future_2]):
                label, results, error = future.result()
                view_cfg = VIEW_1 if label == VIEW_1["label"] else VIEW_2
                target_col = col1 if label == VIEW_1["label"] else col2
                render_results(target_col, results, label, error, view_cfg.get("display_names"))


if __name__ == "__main__":
    main()
