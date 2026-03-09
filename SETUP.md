# Setup Guide

## 1. Install Python dependencies

```bash
pip install -r requirements.txt
```

## 2. Configure environment variables

Create a `.env` file in the project root (or export these variables):

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password
```

## 3. Configure views

Open `app.py` and update `VIEW_1` and `VIEW_2` at the top of the file with your actual PostgreSQL view names and column names.

## 4. Run the database setup script

Edit `setup.sql` to update the AlmaBase section with your actual table/column names, then run:

```bash
psql -h localhost -U your_user -d your_database -f setup.sql
```

This creates the materialized views and GIN trigram indexes needed for fuzzy search.

## 5. Set up automatic materialized view refresh

The materialized views need to be refreshed when source data changes. Add a cron job to refresh them automatically.

Open your crontab:

```bash
crontab -e
```

Add the following lines (adjust the schedule, host, user, and database name):

```cron
# Refresh Raiser's Edge materialized view — every night at 2 AM
0 2 * * * psql -h localhost -U your_user -d your_database -c "REFRESH MATERIALIZED VIEW CONCURRENTLY raisers_edge_view;" >> /var/log/mv_refresh.log 2>&1

# Refresh AlmaBase materialized view — every night at 2 AM
# (uncomment once the AlmaBase materialized view is created)
# 0 2 * * * psql -h localhost -U your_user -d your_database -c "REFRESH MATERIALIZED VIEW CONCURRENTLY almabase_view;" >> /var/log/mv_refresh.log 2>&1
```

To use a `.pgpass` file instead of being prompted for a password, create `~/.pgpass`:

```
localhost:5432:your_database:your_user:your_password
```

Then `chmod 600 ~/.pgpass`.

To verify the cron job is saved:

```bash
crontab -l
```

## 6. Launch the app

```bash
streamlit run app.py
```

The app will open in your browser at `http://localhost:8501`.
