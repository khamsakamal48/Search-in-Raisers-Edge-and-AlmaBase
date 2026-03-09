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
from concurrent.futures import ThreadPoolExecutor, as_completed

import streamlit as st
import psycopg2
import psycopg2.extras
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
}

VIEW_2 = {
    "name": "almabase_view",           # <-- your AlmaBase view name
    "label": "AlmaBase",
    "columns": {
        "name": "full_name",
        "email": "emails",             # comma-separated emails (via STRING_AGG in view)
        "phone": "phones",             # comma-separated phones (via STRING_AGG in view)
        "identifier": "almabase_id",
        "roll_number": "roll_number",
        "department": "departments",    # comma-separated departments (via STRING_AGG)
        "degree": "degrees",            # comma-separated degrees (via STRING_AGG)
        "batch": "batches",             # comma-separated batches (via STRING_AGG)
    },
}

# Filterable columns — these will appear as multi-select filters in the sidebar
FILTER_COLUMNS = ["department", "degree", "batch"]

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
                    combined[filter_key].update(
                        row[0].strip() for row in cur.fetchall() if row[0] and row[0].strip()
                    )
            except psycopg2.Error:
                pass

    return {col: sorted(vals) for col, vals in combined.items()}


# ---------------------------------------------------------------------------
# Search-type detection
# ---------------------------------------------------------------------------
def detect_search_type(query: str) -> str:
    """Classify the search query as 'email', 'phone', 'roll_number', or 'name'."""
    query = query.strip()
    if re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", query):
        return "email"
    digits_only = re.sub(r"[\s\-\+\(\).]", "", query)
    if digits_only.isdigit() and len(digits_only) >= 7:
        return "phone"
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


def build_search_query(view_name: str, columns: dict, search_term: str, search_type: str, filters: dict | None = None):
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
        query = f"""
            SELECT *,
                   similarity({name_col}, %(term)s)        AS trgm_sim,
                   word_similarity(%(term)s, {name_col})    AS trgm_word_sim,
                   soundex({name_col})                      AS sdx,
                   dmetaphone({name_col})                   AS dmeta
            FROM {view_name}
            WHERE (similarity({name_col}, %(term)s) > %(threshold)s
               OR word_similarity(%(term)s, {name_col}) > %(threshold)s
               OR soundex({name_col}) = soundex(%(term)s)
               OR dmetaphone({name_col}) = dmetaphone(%(term)s))
            {filter_clause}
            ORDER BY similarity({name_col}, %(term)s) DESC
            LIMIT 50
        """
    elif search_type == "email":
        filter_clause = _build_filter_clause(col, filters, params)
        params["term"] = f"%{search_term}%"
        query = f"""
            SELECT *, 1.0 AS trgm_sim, 1.0 AS trgm_word_sim,
                   '' AS sdx, '' AS dmeta
            FROM {view_name}
            WHERE {col['email']} ILIKE %(term)s
            {filter_clause}
            LIMIT 50
        """
    elif search_type == "phone":
        digits = re.sub(r"[\s\-\+\(\).]", "", search_term)
        filter_clause = _build_filter_clause(col, filters, params)
        params["term"] = f"%{digits}%"
        query = f"""
            SELECT *, 1.0 AS trgm_sim, 1.0 AS trgm_word_sim,
                   '' AS sdx, '' AS dmeta
            FROM {view_name}
            WHERE regexp_replace({col['phone']}, '[^0-9]', '', 'g') LIKE %(term)s
            {filter_clause}
            LIMIT 50
        """
    else:  # roll_number / identifier
        filter_clause = _build_filter_clause(col, filters, params)
        params["term"] = f"%{search_term}%"
        query = f"""
            SELECT *, 1.0 AS trgm_sim, 1.0 AS trgm_word_sim,
                   '' AS sdx, '' AS dmeta
            FROM {view_name}
            WHERE (CAST({col['roll_number']} AS TEXT) ILIKE %(term)s
               OR CAST({col['identifier']} AS TEXT) ILIKE %(term)s)
            {filter_clause}
            LIMIT 50
        """

    return query, params


# ---------------------------------------------------------------------------
# Query execution
# ---------------------------------------------------------------------------
def execute_search(view_name: str, columns: dict, search_term: str, search_type: str, filters: dict | None = None):
    """Execute the search query and return rows as list of dicts."""
    conn = _get_fresh_connection()
    if conn is None:
        return None, "Database connection unavailable"

    query, params = build_search_query(view_name, columns, search_term, search_type, filters)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
        return [dict(r) for r in rows], None
    except psycopg2.Error as e:
        return None, f"Query error: {e}"


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
def search_view(view_config: dict, search_term: str, search_type: str, filters: dict | None = None) -> tuple:
    """
    Search a single view and return (label, scored_results, error).
    scored_results is a list of dicts with an added 'confidence' key.
    """
    rows, error = execute_search(
        view_config["name"], view_config["columns"], search_term, search_type, filters
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
        for key in ("trgm_sim", "trgm_word_sim", "sdx", "dmeta"):
            row.pop(key, None)

        row["confidence"] = confidence
        scored.append(row)

    # Sort by confidence descending
    scored.sort(key=lambda r: r["confidence"], reverse=True)
    return view_config["label"], scored, None


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------
def render_results(container, results: list, view_label: str, error: str | None):
    """Render search results inside a Streamlit column/container."""
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
                    container.markdown(f"**{key}:** {value}")


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

    # --- Sidebar filters ---
    filter_options = load_filter_options()
    filters = {}
    with st.sidebar:
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
            future_1 = executor.submit(search_view, VIEW_1, search_term, search_type, filters)
            future_2 = executor.submit(search_view, VIEW_2, search_term, search_type, filters)

            for future in as_completed([future_1, future_2]):
                label, results, error = future.result()
                target_col = col1 if label == VIEW_1["label"] else col2
                render_results(target_col, results, label, error)


if __name__ == "__main__":
    main()
