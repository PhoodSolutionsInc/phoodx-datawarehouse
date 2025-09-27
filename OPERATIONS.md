# Data Warehouse Operations Guide

This guide covers day-to-day operational procedures for the PostgreSQL-based data warehouse.

## Table of Contents

- [Creating a Brand New DataWarehouse from Scratch](#creating-a-brand-new-datawarehouse-from-scratch)
- [Adding a New Tenant](#adding-a-new-tenant)
- [Dropping a Tenant](#dropping-a-tenant)
- [Re-creating a Single Materialized View](#re-creating-a-single-materialized-view)
- [Re-creating an Entire Tenant Dataset](#re-creating-an-entire-tenant-dataset)
- [Connecting to the DataWarehouse for Reports](#connecting-to-the-datawarehouse-for-reports)
- [Adding a New Template](#adding-a-new-template)
- [Adding a New Type of DataTable/MV (Legacy)](#adding-a-new-type-of-datatablemv-legacy)
- [Managing Cron Jobs](#managing-cron-jobs)
- [Troubleshooting](#troubleshooting)
- [Monitoring and Health Checks](#monitoring-and-health-checks)
- [Maintenance Procedures](#maintenance-procedures)

---

## Creating a Brand New DataWarehouse from Scratch

### Prerequisites
- PostgreSQL RDS instance running
- Master `postgres` user credentials
- Network connectivity to tenant databases

### Step 1: Initial Database and User Setup
```bash
# Connect as master postgres user to template1 database
psql -h your-warehouse.rds.amazonaws.com -U postgres -d template1
```

**IMPORTANT**: Before running the script, edit `create.sql` and replace the password stubs:
- Change `STUB_WHADMIN_PASSWORD` to a secure password
- Change `STUB_PHOOD_RO_PASSWORD` to a secure password

```sql
-- Run the initial setup script
\i sql/create.sql

-- Connect to the new warehouse database as whadmin
\c phood_warehouse whadmin

-- Create all warehouse functions
\i sql/functions.sql

-- Load the foodlogstats template
\i sql/template-foodlogstats.sql
```

This will:
- Rename the default database to `phood_warehouse`
- Create required extensions (`dblink`, `pg_cron`)
- Create `whadmin` user (warehouse operations)
- Create `phood_ro` user (read-only BI access)
- Set up initial permissions and schemas
- Create the `_wh` schema and all warehouse functions

### Step 2: Verify Installation
```sql
-- Check that all functions are created
SELECT routine_name, routine_type
FROM information_schema.routines
WHERE routine_schema = '_wh'
ORDER BY routine_name;

-- Verify tenant_connections table exists
\d _wh.tenant_connections

-- Verify users and permissions
SELECT
    rolname,
    rolsuper,
    rolcreatedb,
    rolcreaterole,
    rolcanlogin
FROM pg_roles
WHERE rolname IN ('postgres', 'whadmin', 'phood_ro')
ORDER BY rolname;
```

### Step 3: Create Initial Tenant Schemas
```sql
-- Create schemas for each tenant (still as postgres user)
CREATE SCHEMA tenant_a;
CREATE SCHEMA tenant_b;

-- Grant operational access to whadmin user for tenant_a
GRANT ALL PRIVILEGES ON SCHEMA tenant_a TO whadmin;
GRANT ALL ON ALL TABLES IN SCHEMA tenant_a TO whadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA tenant_a GRANT ALL ON TABLES TO whadmin;

-- Grant read access to phood_ro user for tenant_a
GRANT USAGE ON SCHEMA tenant_a TO phood_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA tenant_a TO phood_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA tenant_a GRANT SELECT ON TABLES TO phood_ro;

-- Repeat pattern above for additional tenant schemas as needed
```

---

## Adding a New Tenant

### Step 1: Create Tenant Schema
```sql
-- Connect as postgres user (for tenant creation)
-- Create dedicated schema for the tenant
CREATE SCHEMA new_tenant_name;

-- Grant operational access to whadmin user
GRANT ALL PRIVILEGES ON SCHEMA new_tenant_name TO whadmin;
GRANT ALL ON ALL TABLES IN SCHEMA new_tenant_name TO whadmin;
GRANT ALL ON ALL MATERIALIZED VIEWS IN SCHEMA new_tenant_name TO whadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA new_tenant_name GRANT ALL ON TABLES TO whadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA new_tenant_name GRANT ALL ON VIEWS TO whadmin;

-- Grant read access to phood_ro user
GRANT USAGE ON SCHEMA new_tenant_name TO phood_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA new_tenant_name TO phood_ro;
GRANT SELECT ON ALL MATERIALIZED VIEWS IN SCHEMA new_tenant_name TO phood_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA new_tenant_name GRANT SELECT ON TABLES TO phood_ro;
```

### Step 2: Add Tenant Connection
```sql
-- Add connection details to tenant_connections table (direct SQL)
INSERT INTO _wh.tenant_connections (tenant_name, host, port, dbname, username, password)
VALUES (
    'new_tenant_connection_name',     -- connection identifier
    'tenant-db.rds.amazonaws.com',    -- host
    5432,                             -- port
    'production_db',                  -- database name
    'readonly_user',                  -- username
    'secure_password_123'             -- password
);
```

### Step 3: Test Connection
```sql
-- Verify connection works
SELECT _wh.get_tenant_connection_string('new_tenant_connection_name');

-- Test a simple query via dblink
SELECT * FROM dblink(
    _wh.get_tenant_connection_string('new_tenant_connection_name'),
    'SELECT 1 as test'
) AS t(test integer);
```

### Step 4: Initial Data Load (Template-Based)
```sql
-- Connect as whadmin user for data operations
-- Create first materialized view (current date)
SELECT _wh.update_mv_by_template(
    'foodlogstats',                   -- template name
    'new_tenant_connection_name',     -- connection name
    'new_tenant_name',               -- schema name
    CURRENT_DATE
);

-- Backfill historical data (one month at a time)
SELECT _wh.update_mv_window_by_template(
    'foodlogstats',                   -- template name
    'new_tenant_connection_name',     -- connection name
    'new_tenant_name',               -- schema name
    '2024-01-01'::date,
    '2024-01-31'::date
);

-- Create unified tenant view
SELECT _wh.update_tenant_union_view_by_template(
    'foodlogstats',                   -- template name
    'new_tenant_connection_name',     -- connection name
    'new_tenant_name'                -- schema name
);

-- Update public master view to include new tenant
SELECT _wh.update_public_view_by_template('foodlogstats');
```

### Step 5: Update Master Cross-Tenant View
```sql
-- Connect as postgres user to update public schema
-- Update the master foodlogstats view to include the new tenant
-- Example for adding 'new_tenant_name' to existing tenants:

CREATE OR REPLACE VIEW public.foodlogstats AS
SELECT * FROM landb.foodlogstats
UNION ALL
SELECT * FROM mm.foodlogstats
UNION ALL
SELECT * FROM new_tenant_name.foodlogstats;

-- Grant read access to both users
GRANT SELECT ON public.foodlogstats TO phood_ro;
GRANT SELECT ON public.foodlogstats TO whadmin;
```

---

## Dropping a Tenant

### ⚠️ WARNING: This will permanently delete all tenant data!

### Step 1: Remove from Union Views
```sql
-- First, manually update any cross-tenant union views to exclude this tenant
-- (This step depends on your specific setup)
```

### Step 2: Drop Tenant Schema
```sql
-- This will cascade and drop all materialized views and tables
DROP SCHEMA tenant_name CASCADE;
```

### Step 3: Remove Connection
```sql
-- Remove tenant connection details
DELETE FROM _wh.tenant_connections
WHERE tenant_name = 'tenant_connection_name';
```

### Step 4: Clean Up Permissions
```sql
-- Remove any specific grants for this tenant
-- REVOKE USAGE ON SCHEMA tenant_name FROM reporting_users;
-- (Schema is already dropped, but document for completeness)
```

---

## Re-creating a Single Materialized View

### For a Corrupted or Incorrect MV

Since we removed `force_recreate`, manual steps are required:

### Step 1: Identify Dependencies
```sql
-- Find views that depend on this MV
SELECT
    schemaname,
    viewname,
    definition
FROM pg_views
WHERE definition LIKE '%target_mv_name%';
```

### Step 2: Safe Rebuild Process
```sql
-- 1. Note any dependent views (save their definitions)

-- 2. Drop dependent views first
DROP VIEW IF EXISTS schema_name.dependent_view_name;

-- 3. Drop the problematic MV
DROP MATERIALIZED VIEW schema_name.foodlogstats_2024_01_15;

-- 4. Recreate the MV
SELECT _wh.update_foodlogstats(
    'tenant_connection_name',
    'schema_name',
    '2024-01-15'::date
);

-- 5. Recreate dependent views
SELECT _wh.update_tenant_view(
    'tenant_connection_name',
    'schema_name',
    'foodlogstats'
);
```

---

## Re-creating an Entire Tenant Dataset

### For Major Data Corruption or Schema Changes

### Step 1: Create Backup List
```sql
-- Get list of all existing MVs for the tenant
SELECT matviewname
FROM pg_matviews
WHERE schemaname = 'tenant_schema'
ORDER BY matviewname;
```

### Step 2: Drop All MVs
```sql
-- Drop the union view first
DROP VIEW IF EXISTS tenant_schema.foodlogstats;

-- Drop all materialized views
DO $$
DECLARE
    mv_name TEXT;
BEGIN
    FOR mv_name IN
        SELECT matviewname
        FROM pg_matviews
        WHERE schemaname = 'tenant_schema'
    LOOP
        EXECUTE 'DROP MATERIALIZED VIEW tenant_schema.' || mv_name;
    END LOOP;
END $$;
```

### Step 3: Rebuild All Data
```sql
-- Rebuild all historical data (adjust date ranges as needed)
SELECT _wh.update_mv_window(
    'tenant_connection_name',
    'tenant_schema',
    '2024-01-01'::date,
    '2024-12-31'::date
);

-- Recreate union view
SELECT _wh.update_tenant_view(
    'tenant_connection_name',
    'tenant_schema',
    'foodlogstats'
);
```

---

## Connecting to the DataWarehouse for Reports

### Database Connection Details

#### For BI/Reporting (Read-Only Access)
```bash
# Connect as phood_ro user for read-only access
psql -h your-warehouse.rds.amazonaws.com \
     -U phood_ro \
     -d phood_warehouse \
     -p 5432
```

#### For Warehouse Operations
```bash
# Connect as whadmin user for warehouse management
psql -h your-warehouse.rds.amazonaws.com \
     -U whadmin \
     -d phood_warehouse \
     -p 5432
```

### Common Query Patterns
```sql
-- Query specific tenant data
SELECT * FROM tenant_a.foodlogstats
WHERE logged_time >= '2024-01-01';

-- Query specific date range
SELECT * FROM tenant_a.foodlogstats_2024_01_15;

-- Cross-tenant analysis
SELECT
    client,
    SUM(weight) as total_waste,
    COUNT(*) as incidents
FROM tenant_a.foodlogstats
WHERE logged_time >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY client;
```

### Business Intelligence Tools
Configure these tools with the `phood_ro` credentials:
- **Host**: your-warehouse.rds.amazonaws.com
- **Port**: 5432
- **Database**: phood_warehouse
- **Username**: phood_ro
- **Password**: [your secure password]

Supported tools:
- **Tableau**: Use PostgreSQL connector
- **Power BI**: Use PostgreSQL connector
- **Grafana**: Add PostgreSQL data source
- **Custom Apps**: Use any PostgreSQL client library

### Performance Tips
- Always use date filters when possible
- Use the specific daily MVs for single-day analysis
- Use the union views for multi-day analysis
- Add appropriate indexes for your specific query patterns

---

## Adding a New Template

Creating a new template allows you to define new types of materialized views without writing custom functions.

### Step 1: Create Template SQL File
Create a new file: `sql/template-{name}.sql`

```sql
-- Example: sql/template-inventory_stats.sql
INSERT INTO _wh.mv_templates (
    template_name,
    description,
    query_template,
    column_definitions,
    indexes
) VALUES (
    'inventory_stats',
    'Daily inventory statistics with movement tracking',
    $template$
SELECT
    i.id,
    l.name AS location,
    i.item_name,
    i.current_stock,
    i.movement_type,
    i.quantity_change,
    i.timestamp AS recorded_time
FROM inventory_movements i
LEFT JOIN locations l ON i.location_id = l.id
WHERE i.timestamp::DATE = '{TARGET_DATE}'::DATE
$template$,
    $columns$
id INTEGER,
location TEXT,
item_name TEXT,
current_stock INTEGER,
movement_type TEXT,
quantity_change INTEGER,
recorded_time TIMESTAMP
$columns$,
    'CREATE UNIQUE INDEX idx_{SCHEMA}_{VIEW_NAME}_id ON {SCHEMA}.{VIEW_NAME} (id);
CREATE INDEX idx_{SCHEMA}_{VIEW_NAME}_recorded_time ON {SCHEMA}.{VIEW_NAME} (recorded_time);
CREATE INDEX idx_{SCHEMA}_{VIEW_NAME}_location ON {SCHEMA}.{VIEW_NAME} (location);'
);
```

### Step 2: Load Template
```sql
-- Connect as whadmin
\c phood_warehouse whadmin

-- Load the new template
\i sql/template-inventory_stats.sql
```

### Step 3: Test Template
```sql
-- Create MV using new template
SELECT _wh.create_mv_from_template('inventory_stats', 'tenant_a', 'reports', CURRENT_DATE);

-- Create tenant union view
SELECT _wh.update_tenant_union_view_by_template('inventory_stats', 'tenant_a', 'reports');

-- Create public master view
SELECT _wh.update_public_view_by_template('inventory_stats');
```

### Step 4: Template Management
```sql
-- View all templates
SELECT template_name, description, created_at FROM _wh.mv_templates;

-- View template details
SELECT * FROM _wh.mv_templates WHERE template_name = 'inventory_stats';

-- Update template (edit the .sql file and re-run it)
-- Templates use ON CONFLICT DO UPDATE, so re-running updates safely
```

---

## Adding a New Type of DataTable/MV (Legacy)

### Step 1: Create the Core MV Function
```sql
-- Example: Adding a new "inventory_stats" MV type
CREATE OR REPLACE FUNCTION _wh.create_inventory_stats_mv(
    tenant_connection_name text,
    target_schema text,
    base_view_name text,
    target_date date,
    client_name text DEFAULT NULL::text
)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
  DECLARE
      connection_string TEXT;
      remote_query TEXT;
      view_name TEXT;
      full_mv_name TEXT;
      client_override TEXT;
      create_sql TEXT;
      context JSONB;
  BEGIN
      -- Follow same pattern as create_foodlogstats_mv
      view_name := _wh.create_mv_name(base_view_name, target_date);
      connection_string := _wh.get_tenant_connection_string(tenant_connection_name);
      full_mv_name := target_schema || '.' || view_name;
      client_override := COALESCE(client_name, target_schema);

      -- Define your specific remote query
      remote_query := format($remote$
          SELECT
              i.id,
              i.name,
              i.quantity_on_hand,
              i.cost_per_unit,
              l.name as location,
              '%s' as client,
              i.updated_at
          FROM inventory_items i
          LEFT JOIN locations l ON i.location_id = l.id
          WHERE i.updated_at::DATE = '%s'::DATE
      $remote$, client_override, target_date);

      -- Create MV with explicit column definitions
      create_sql := format($create$
          CREATE MATERIALIZED VIEW %I.%I AS
          SELECT * FROM dblink(%L, %L)
          AS t(
              id INTEGER,
              name TEXT,
              quantity_on_hand NUMERIC,
              cost_per_unit NUMERIC,
              location TEXT,
              client TEXT,
              updated_at TIMESTAMP
          )
      $create$, target_schema, view_name, connection_string, remote_query);

      EXECUTE create_sql;

      -- Create indexes
      EXECUTE format('CREATE UNIQUE INDEX ON %I.%I (id)', target_schema, view_name);
      EXECUTE format('CREATE INDEX ON %I.%I (updated_at)', target_schema, view_name);
      EXECUTE format('CREATE INDEX ON %I.%I (client)', target_schema, view_name);
      EXECUTE format('CREATE INDEX ON %I.%I (location)', target_schema, view_name);

      RETURN TRUE;
  EXCEPTION
      WHEN OTHERS THEN
          RETURN FALSE;
  END;
  $function$
;
```

### Step 2: Create the Wrapper Function
```sql
CREATE OR REPLACE FUNCTION _wh.update_inventory_stats(
    tenant_connection_name text,
    target_schema text,
    target_date date,
    allow_refresh_yesterday boolean DEFAULT true
)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
  BEGIN
      RETURN _wh.update_mv_core(
          tenant_connection_name,
          target_schema,
          target_date,
          'inventory_stats',
          '_wh.create_inventory_stats_mv',
          allow_refresh_yesterday
      );
  END;
  $function$
;
```

### Step 3: Test the New MV Type
```sql
-- Test creating a single MV
SELECT _wh.update_inventory_stats(
    'tenant_connection_name',
    'tenant_schema',
    CURRENT_DATE
);

-- Test date range
SELECT _wh.update_mv_window(
    'tenant_connection_name',
    'tenant_schema',
    '2024-01-01'::date,
    '2024-01-31'::date,
    '_wh.update_inventory_stats'
);
```

---

## Managing Cron Jobs

### View All Scheduled Jobs
```sql
-- See all cron jobs
SELECT
    jobid,
    schedule,
    command,
    jobname,
    active,
    database,
    username
FROM cron.job
ORDER BY jobid;
```

### View Job Execution History
```sql
-- Check recent job runs
SELECT
    j.jobname,
    j.command,
    r.status,
    r.start_time,
    r.end_time,
    r.return_message
FROM cron.job j
LEFT JOIN cron.job_run_details r ON j.jobid = r.jobid
WHERE r.start_time > NOW() - INTERVAL '24 hours'
ORDER BY r.start_time DESC;
```

### Add a New Daily Job
```sql
-- Example: Daily update for a tenant (run as whadmin user)
SELECT cron.schedule(
    'daily-tenant-a-update',           -- job name
    '0 2 * * *',                      -- schedule (2 AM daily)
    $$SELECT _wh.update_foodlogstats('tenant_a_conn', 'tenant_a', CURRENT_DATE);$$,
    'phood_warehouse',                 -- database name
    'whadmin'                          -- username
);
```

### Add a New Hourly Job
```sql
-- Example: Hourly refresh of current day
SELECT cron.schedule(
    'hourly-current-day-refresh',
    '0 * * * *',                      -- every hour
    $$SELECT _wh.update_foodlogstats('tenant_a_conn', 'tenant_a', CURRENT_DATE);$$,
    'phood_warehouse',                 -- database name
    'whadmin'                          -- username
);
```

### Disable/Enable a Job
```sql
-- Disable a job
UPDATE cron.job SET active = false WHERE jobname = 'daily-tenant-a-update';

-- Enable a job
UPDATE cron.job SET active = true WHERE jobname = 'daily-tenant-a-update';
```

### Remove a Job
```sql
-- Delete a job permanently
SELECT cron.unschedule('daily-tenant-a-update');
```

---

## Troubleshooting

### Common Issues

#### 1. "Connection to database failed"
```sql
-- Connect as whadmin user first
-- Check tenant connection details
SELECT * FROM _wh.tenant_connections WHERE tenant_name = 'problematic_tenant';

-- Test connection manually
SELECT * FROM dblink(
    _wh.get_tenant_connection_string('problematic_tenant'),
    'SELECT 1'
) AS t(test integer);
```

#### 2. "Materialized view already exists"
```sql
-- Check if MV exists
SELECT _wh.does_mv_exist('schema_name', 'view_name');

-- List all MVs for a schema
SELECT matviewname FROM pg_matviews WHERE schemaname = 'schema_name';
```

#### 3. "No data in materialized view"
```sql
-- Check row count
SELECT
    schemaname,
    matviewname,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||matviewname)) as size
FROM pg_matviews
WHERE schemaname = 'target_schema'
ORDER BY matviewname;

-- Check if source data exists for that date
SELECT * FROM dblink(
    _wh.get_tenant_connection_string('tenant_conn'),
    'SELECT COUNT(*) FROM phood_foodlogsum WHERE logged_time::DATE = ''2024-01-15'''
) AS t(count bigint);
```

#### 4. "Union view is missing data"
```sql
-- Check which MVs are included in union view
SELECT
    matviewname
FROM pg_matviews
WHERE schemaname = 'target_schema'
  AND matviewname LIKE 'foodlogstats_%'
ORDER BY matviewname;

-- Recreate union view
SELECT _wh.update_tenant_view('tenant_conn', 'target_schema', 'foodlogstats');
```

---

## Monitoring and Health Checks

### Database Size Monitoring
```sql
-- Check warehouse database size
SELECT
    pg_size_pretty(pg_database_size('warehouse')) as warehouse_size;

-- Check schema sizes
SELECT
    schemaname,
    pg_size_pretty(SUM(pg_total_relation_size(schemaname||'.'||tablename))) as schema_size
FROM pg_tables
WHERE schemaname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
GROUP BY schemaname
ORDER BY SUM(pg_total_relation_size(schemaname||'.'||tablename)) DESC;
```

### Connection Monitoring
```sql
-- Check active connections
SELECT
    count(*) as active_connections,
    max_conn.setting as max_connections
FROM pg_stat_activity,
     (SELECT setting FROM pg_settings WHERE name = 'max_connections') max_conn
WHERE state = 'active';
```

### Performance Monitoring
```sql
-- Check slow queries
SELECT
    query,
    calls,
    total_time,
    mean_time,
    rows
FROM pg_stat_statements
WHERE mean_time > 1000  -- queries taking > 1 second
ORDER BY mean_time DESC
LIMIT 10;
```

### Data Freshness Check
```sql
-- Check latest data for each tenant
SELECT
    schemaname,
    matviewname,
    regexp_replace(matviewname, '.*_(\d{4}_\d{2}_\d{2})$', '\1') as date_part
FROM pg_matviews
WHERE matviewname LIKE '%_____\_\_\_\_\_\_\_\_\_\_\_'  -- matches date pattern
ORDER BY schemaname, date_part DESC;
```

---

## Maintenance Procedures

### Clean Up Old Materialized Views
```sql
-- Find MVs older than 6 months
SELECT
    schemaname,
    matviewname,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||matviewname)) as size
FROM pg_matviews
WHERE matviewname ~ '_\d{4}_\d{2}_\d{2}$'  -- has date suffix
  AND TO_DATE(
    regexp_replace(matviewname, '.*_(\d{4}_\d{2}_\d{2})$', '\1'),
    'YYYY_MM_DD'
  ) < CURRENT_DATE - INTERVAL '6 months'
ORDER BY schemaname, matviewname;

-- Drop old MVs (BE CAREFUL!)
-- DO $$
-- DECLARE
--     mv_record RECORD;
-- BEGIN
--     FOR mv_record IN
--         SELECT schemaname, matviewname FROM pg_matviews
--         WHERE matviewname ~ '_\d{4}_\d{2}_\d{2}$'
--           AND TO_DATE(regexp_replace(matviewname, '.*_(\d{4}_\d{2}_\d{2})$', '\1'), 'YYYY_MM_DD') < CURRENT_DATE - INTERVAL '6 months'
--     LOOP
--         EXECUTE 'DROP MATERIALIZED VIEW ' || mv_record.schemaname || '.' || mv_record.matviewname;
--         RAISE NOTICE 'Dropped %', mv_record.schemaname || '.' || mv_record.matviewname;
--     END LOOP;
-- END $$;
```

### Update Statistics
```sql
-- Update PostgreSQL statistics for better query planning
ANALYZE;

-- Update statistics for specific large tables
ANALYZE schema_name.large_table_name;
```

### Vacuum Maintenance
```sql
-- Check tables that need vacuuming
SELECT
    schemaname,
    tablename,
    n_dead_tup,
    n_live_tup,
    ROUND(n_dead_tup::numeric / NULLIF(n_live_tup::numeric, 0) * 100, 2) as dead_tuple_percent
FROM pg_stat_user_tables
WHERE n_dead_tup > 1000
ORDER BY dead_tuple_percent DESC;

-- Manual vacuum if needed (usually automated)
-- VACUUM ANALYZE schema_name.table_name;
```

---

## Emergency Procedures

### Complete Warehouse Rebuild
```sql
-- 1. Export tenant connections
COPY _wh.tenant_connections TO '/tmp/tenant_connections_backup.csv' CSV HEADER;

-- 2. Drop and recreate all tenant schemas
-- (This is destructive - have backups!)

-- 3. Restore schema and functions
\i _wh-ddl.sql

-- 4. Restore tenant connections
COPY _wh.tenant_connections FROM '/tmp/tenant_connections_backup.csv' CSV HEADER;

-- 5. Rebuild all data using the normal procedures above
```

### Disaster Recovery
See README.md for high-level disaster recovery strategy. The key principle: **never backup massive MV data, just recreate from source systems**.