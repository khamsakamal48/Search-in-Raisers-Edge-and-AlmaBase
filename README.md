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

## 3. Run the database setup script

Run the setup script to create the Raiser's Edge materialized view and indexes:

```bash
psql -h localhost -U your_user -d your_database -f setup.sql
```

## 4. Upload AlmaBase data

AlmaBase data is uploaded directly through the app's sidebar. No manual SQL setup is needed.

1. Launch the app (see step 6)
2. Use the "AlmaBase Data Upload" section in the sidebar
3. Select one or more `.xlsx` files and click "Upload to Database"

The app will create the `almabase_raw` table and a regular VIEW (`almabase_view`) automatically. Since it's a regular view (not materialized), it reflects any table changes immediately.

## 5. Set up automatic Raiser's Edge refresh

The Raiser's Edge materialized view needs periodic refresh. Add a cron job:

```bash
crontab -e
```

```cron
# Refresh Raiser's Edge materialized view — every night at 2 AM
0 2 * * * psql -h localhost -U your_user -d your_database -c "REFRESH MATERIALIZED VIEW CONCURRENTLY raisers_edge_view;" >> /var/log/mv_refresh.log 2>&1
```

To avoid password prompts, create `~/.pgpass`:

```
localhost:5432:your_database:your_user:your_password
```

Then `chmod 600 ~/.pgpass`.

## 6. Launch the app

```bash
streamlit run app.py
```

The app will open in your browser at `http://localhost:8501`.
