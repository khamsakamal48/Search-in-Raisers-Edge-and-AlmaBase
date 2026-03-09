-- ==========================================================================
-- Alumni Search App — Database Setup Script
-- ==========================================================================
-- Run this script on your PostgreSQL database before using the app.
--
-- Prerequisites:
--   - PostgreSQL 9.6+ (for pg_trgm) / 11+ (for dmetaphone)
--   - Superuser or extension-creation privileges
-- ==========================================================================

-- 1. Enable required extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;         -- trigram similarity
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;   -- soundex, metaphone, dmetaphone

-- 2. Set the default similarity threshold (optional, can also be set per-session)
-- SELECT set_limit(0.3);


-- ==========================================================================
-- 3. Raiser's Edge — MATERIALIZED VIEW
-- ==========================================================================
-- Using a materialized view (not a regular view) because:
--   - full_name is computed via CONCAT_WS and cannot be indexed in a regular view
--   - GIN trigram indexes only work on materialized views / tables
--
-- IMPORTANT: Refresh periodically when source data changes:
--   REFRESH MATERIALIZED VIEW CONCURRENTLY raisers_edge_view;
-- (CONCURRENTLY requires the unique index below)
-- ==========================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS raisers_edge_view AS
WITH
    iitb_education AS (
        SELECT *
        FROM school_list
        WHERE school = 'Indian Institute of Technology Bombay'
    ),
    emails AS (
        SELECT *
        FROM email_list
        WHERE inactive = FALSE
    ),
    phones AS (
        SELECT *
        FROM phone_list
        WHERE inactive = FALSE
    )
SELECT
    cl.lookup_id::INT                                AS constituent_id,
    CONCAT_WS(' ', cl.first, cl.middle, cl.last)     AS full_name,
    STRING_AGG(DISTINCT edu.known_name, ', ')         AS roll_numbers,
    STRING_AGG(DISTINCT edu.majors_0, ', ')           AS departments,
    STRING_AGG(DISTINCT edu.degree, ', ')             AS degrees,
    STRING_AGG(DISTINCT edu.class_of::VARCHAR, ', ')  AS batches,
    STRING_AGG(DISTINCT e.address, ', ')              AS emails,
    STRING_AGG(DISTINCT p.number, ', ')               AS phones
FROM constituent_list cl
LEFT JOIN constituent_code_list AS ccl ON ccl.constituent_id = cl.id
LEFT JOIN iitb_education        AS edu ON edu.constituent_id = cl.id
LEFT JOIN emails                AS e   ON e.constituent_id   = cl.id
LEFT JOIN phones                AS p   ON p.constituent_id   = cl.id
WHERE cl.type = 'Individual'
  AND ccl.description IN ('Alumni', 'Student', 'Live Alumni')
GROUP BY cl.lookup_id::INT, CONCAT_WS(' ', cl.first, cl.middle, cl.last);

-- Unique index (required for REFRESH MATERIALIZED VIEW CONCURRENTLY)
CREATE UNIQUE INDEX IF NOT EXISTS idx_re_constituent_id
    ON raisers_edge_view (constituent_id);

-- Trigram indexes on the materialized view for fuzzy name search
CREATE INDEX IF NOT EXISTS idx_re_name_trgm
    ON raisers_edge_view USING gin (full_name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_re_emails_trgm
    ON raisers_edge_view USING gin (emails gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_re_phones_trgm
    ON raisers_edge_view USING gin (phones gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_re_roll_numbers_trgm
    ON raisers_edge_view USING gin (roll_numbers gin_trgm_ops);


-- ==========================================================================
-- 4. AlmaBase
-- ==========================================================================
-- AlmaBase data is uploaded via the app's sidebar (Excel files → almabase_raw
-- table). The app automatically creates a regular VIEW (almabase_view) that
-- concatenates the spread-out columns (emails, phones, departments, etc.)
-- into comma-separated fields.
--
-- No materialized view is needed — the regular view refreshes automatically
-- whenever the underlying almabase_raw table is updated.
--
-- Note: GIN indexes cannot be created on regular views. If performance is
-- an issue, create indexes on the almabase_raw base table columns directly.
-- ==========================================================================


-- Raiser's Edge indexes (on the materialized view)

-- Unique index (required for REFRESH MATERIALIZED VIEW CONCURRENTLY)
CREATE UNIQUE INDEX IF NOT EXISTS idx_re_constituent_id
    ON raisers_edge_view (constituent_id);

-- Trigram indexes on the materialized view for fuzzy name search
CREATE INDEX IF NOT EXISTS idx_re_name_trgm
    ON raisers_edge_view USING gin (full_name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_re_emails_trgm
    ON raisers_edge_view USING gin (emails gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_re_phones_trgm
    ON raisers_edge_view USING gin (phones gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_re_roll_numbers_trgm
    ON raisers_edge_view USING gin (roll_numbers gin_trgm_ops);
