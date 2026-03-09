#!/bin/bash
set -e

# --- Set up .pgpass for passwordless psql ---
echo "${DB_HOST}:${DB_PORT}:${DB_NAME}:${DB_USER}:${DB_PASSWORD}" > /root/.pgpass
chmod 600 /root/.pgpass

# --- Set up cron job for Raiser's Edge materialized view refresh ---
# Runs every night at 2 AM; logs to /var/log/mv_refresh.log
echo "0 8 * * * PGPASSFILE=/root/.pgpass psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} -d ${DB_NAME} -c 'REFRESH MATERIALIZED VIEW CONCURRENTLY raisers_edge_view;' >> /var/log/mv_refresh.log 2>&1" \
    | crontab -

# Start cron in the background
cron

# Start Streamlit
exec streamlit run app.py --server.port=8501 --server.address=0.0.0.0
