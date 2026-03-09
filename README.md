# Alumni Search — Raiser's Edge & AlmaBase

Search alumni records across two PostgreSQL views side by side with fuzzy name matching.

## Quick Start (Docker)

1. Create a `.env` file pointing to your existing PostgreSQL database:

```
DB_HOST=your_postgres_host
DB_PORT=5432
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password
```

2. Run `setup.sql` on your database (one-time, creates extensions + materialized view):

```bash
psql -h your_postgres_host -U your_user -d your_database -f setup.sql
```

3. Build and start the app:

```bash
docker compose up -d --build
```

4. Open `http://localhost:8501`

### What the container does

- Runs the Streamlit app on port 8501
- Automatically creates a `.pgpass` file from your `.env` variables (for passwordless `psql`)
- Sets up a cron job inside the container that refreshes the Raiser's Edge materialized view every night at 8 AM

### Cron job and .pgpass

The `entrypoint.sh` script handles this automatically on container start:

1. Creates `/root/.pgpass` with your DB credentials from `.env` (permissions set to `600`)
2. Registers a cron job: `REFRESH MATERIALIZED VIEW CONCURRENTLY raisers_edge_view;` at 8 AM daily
3. Cron logs go to `/var/log/mv_refresh.log` inside the container

To check the cron schedule:

```bash
docker compose exec app crontab -l
```

To check refresh logs:

```bash
docker compose exec app cat /var/log/mv_refresh.log
```

To trigger a manual refresh:

```bash
docker compose exec app psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "REFRESH MATERIALIZED VIEW CONCURRENTLY raisers_edge_view;"
```

To change the refresh schedule, edit `entrypoint.sh` and rebuild:

```bash
docker compose up -d --build
```

## Manual Setup (without Docker)

### 1. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment variables

Create a `.env` file in the project root:

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password
```

### 3. Run the database setup script

```bash
psql -h localhost -U your_user -d your_database -f setup.sql
```

### 4. Launch the app

```bash
streamlit run app.py
```

The app will open at `http://localhost:8501`.

### 5. Set up the cron job (manual)

Create `~/.pgpass` for passwordless `psql`:

```
your_host:5432:your_database:your_user:your_password
```

```bash
chmod 600 ~/.pgpass
```

Add the cron job:

```bash
crontab -e
```

```cron
# Refresh Raiser's Edge materialized view every night at 2 AM
0 2 * * * psql -h your_host -U your_user -d your_database -c "REFRESH MATERIALIZED VIEW CONCURRENTLY raisers_edge_view;" >> /var/log/mv_refresh.log 2>&1
```

## Upload AlmaBase Data

AlmaBase data is uploaded directly through the app's sidebar — no manual SQL needed.

1. Use the "AlmaBase Data Upload" section in the sidebar
2. Select one or more `.xlsx` files and click "Upload to Database"

The app creates the `almabase_raw` table and a regular VIEW (`almabase_view`) automatically. The view refreshes instantly when data changes.
