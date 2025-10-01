# Data Warehouse Operations Guide

This guide covers day-to-day operational procedures for the PostgreSQL-based data warehouse.

## Table of Contents

- [Creating a Brand New DataWarehouse from Scratch](#creating-a-brand-new-datawarehouse-from-scratch)
- [Adding a New Tenant](#adding-a-new-tenant)
- [Disabling a Tenant](#disabling-a-tenant)
- [Re-enabling a Tenant](#re-enabling-a-tenant)
- [Dropping a Tenant](#dropping-a-tenant)
- [Re-creating a Single Materialized View](#re-creating-a-single-materialized-view)
- [Re-creating an Entire Tenant Dataset](#re-creating-an-entire-tenant-dataset)
- [Converting Daily MVs to Yearly Tables](#converting-daily-mvs-to-yearly-tables)
- [Connecting to the DataWarehouse for Reports](#connecting-to-the-datawarehouse-for-reports)
- [Adding a New Template](#adding-a-new-template)
- [Updating Warehouse Functions](#updating-warehouse-functions)
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
- **pg_cron extension enabled** (see setup instructions below)

### pg_cron Extension Setup (Required)

**IMPORTANT**: The pg_cron extension requires specific AWS RDS configuration and must be set up BEFORE running the database setup scripts.

Follow the AWS documentation to enable pg_cron:
https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL_pg_cron.html#PostgreSQL_pg_cron.enable

Key steps:
1. **Parameter Group Configuration**: Create or modify your RDS parameter group to include:
   - `shared_preload_libraries = 'pg_cron'`
   - `cron.database_name = 'postgres'` (our default database)

2. **Apply Parameter Group**: Attach the parameter group to your RDS instance

3. **Restart Instance**: Reboot the RDS instance to load the extension

4. **Create Extension**: After restart, connect as postgres user and run:
   ```sql
   CREATE EXTENSION IF NOT EXISTS pg_cron;
   ```

5. **Verify Setup**: Check that the extension is available:
   ```sql
   SELECT extname, extversion FROM pg_extension WHERE extname = 'pg_cron';
   ```

**Note**: This setup process can take 10-15 minutes due to parameter group changes and instance restart requirements.

### Step 1: Initial Database and User Setup
```bash
# Connect as master postgres user to postgres database
psql -h your-warehouse.rds.amazonaws.com -U postgres -d postgres
```

**IMPORTANT**: Before running the script, edit `create.sql` and replace the password stubs:
- Change `STUB_WHADMIN_PASSWORD` to a secure password
- Change `STUB_PHOOD_RO_PASSWORD` to a secure password

```sql
-- Run the initial setup script
\i sql/create.sql

-- Stay connected to postgres database as whadmin
\c postgres whadmin

-- Create all warehouse functions
\i sql/functions.sql

-- Load the foodlogstats template
\i sql/template-foodlogstats.sql
```

This will:
- Use the default `postgres` database (required for pg_cron)
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

### Step 4: Determine Initial load Date Range
```sql
--- Connect as whadmin user for data operations
-- select MIN and MAX date value from source table. In this eg, the foodlogsum table
select * FROM dblink(
    _wh.get_tenant_connection_string('new_tenant_connecton_name'),
    'SELECT to_char(MIN(logged_time) AT TIME ZONE ''UTC'', ''YYYY-MM-DD HH24:MI:SS UTC''), to_char(MAX(logged_time) AT TIME ZONE ''UTC'', ''YYYY-MM-DD HH24:MI:SS UTC'') from phood_foodlogsum')
    as t(min_time TEXT, max_time TEXT)

```

### Step 5: Initial Data Load (Template-Based)
```sql

-- Backfill historical data (one year or less at a time). Be careful, this could put a load on the source DB.
-- Run the window function until we're caught up to today. The reason for a year or less also depends on the 
-- size of the source dataset.  As running this function creates one massive transaction.
-- It's worth noting that the dates are inclusive in this function. 
-- Also, it's OK to run this ontop of existing views, because it just re-freshes them. So, you can run overlaps
-- and not worry.
SELECT _wh.update_mv_window_by_template(
    'foodlogstats',                   -- template name
    'new_tenant_connection_name',     -- connection name
    'new_tenant_name',               -- schema name
    '2023-01-01'
    '2023-12-31'
);

-- Create unified tenant view. Shouldn't actually be necessary, as the update by window function already does this.
SELECT _wh.update_tenant_union_view_by_template(
    'foodlogstats',                   -- template name
    'new_tenant_connection_name',     -- connection name
    'new_tenant_name'                -- schema name
);

-- Check the year you converted for completness. This only works on the current year, and a full year of data. 
-- A partial year will fail this check.
select _wh.check_views_by_template_for_year(
    'foodlogstats',         -- template name
    'new_tenant_name'       -- schema name
    2023                    -- year
)

-- Update public master view to include new tenant
SELECT _wh.update_public_view_by_template('foodlogstats');

-- Verify tenant data is included in public view
SELECT schema_name, COUNT(*) as record_count
FROM public.foodlogstats
GROUP BY schema_name

UNION ALL

SELECT 'TOTAL' as schema_name, COUNT(*) as record_count
FROM public.foodlogstats;
```

### Step 6: Convert Completed Years to Yearly Tables (Optional)

If your backfill included complete calendar years (not just current year), convert them to yearly tables for better performance and storage efficiency.

```sql
-- Convert completed years to yearly tables (one year at a time)
-- Only convert years that are complete (365/366 daily MVs exist)
-- Do NOT create last years table, if we are still in January.  
-- This is because there is a window update function we run 
-- in the pgcron that updates the last 2 weeks of MVs for a customer
-- every morning at 3:00.   

-- Check how many daily MVs exist for each year
SELECT
    EXTRACT(YEAR FROM TO_DATE(
        regexp_replace(matviewname, '.*_(\d{4}_\d{2}_\d{2})$', '\1'),
        'YYYY_MM_DD'
    )) as year,
    COUNT(*) as daily_mvs_count
FROM pg_matviews
WHERE schemaname = 'new_tenant_name'
AND matviewname LIKE 'foodlogstats_%'
GROUP BY EXTRACT(YEAR FROM TO_DATE(
    regexp_replace(matviewname, '.*_(\d{4}_\d{2}_\d{2})$', '\1'),
    'YYYY_MM_DD'
))
ORDER BY year;

-- Convert complete years (365 or 366 MVs) to yearly tables
-- Example: Convert 2023 if it has 365 MVs
SELECT _wh.create_combined_table_from_template_by_year(
    'foodlogstats',        -- template name
    'new_tenant_name',     -- schema name
    2023                   -- year to convert
);

-- Convert 2024 if complete
SELECT _wh.create_combined_table_from_template_by_year(
    'foodlogstats',        -- template name
    'new_tenant_name',     -- schema name
    2024                   -- year to convert
);

-- Verify yearly tables were created and union view updated
SELECT COUNT(*) FROM new_tenant_name.foodlogstats_2023;
SELECT COUNT(*) FROM new_tenant_name.foodlogstats_2024;

-- Final verification: Check union view includes yearly data
SELECT
    EXTRACT(YEAR FROM logged_time AT TIME ZONE 'UTC') as year,
    COUNT(*) as records
FROM new_tenant_name.foodlogstats
WHERE EXTRACT(YEAR FROM logged_time AT TIME ZONE 'UTC') IN (2023, 2024)
GROUP BY EXTRACT(YEAR FROM logged_time AT TIME ZONE 'UTC')
ORDER BY year;
```

**Benefits of Yearly Conversion:**
- **Performance**: Single table scan vs. 365 MV unions
- **Storage**: 30-50% reduction in disk usage
- **Maintenance**: Simpler backup and maintenance operations
- **Queries**: Faster year-over-year analysis

**When to Convert:**
- ✅ **Complete years only** (365/366 days of data)
- ✅ **Historical years** (not current year)
- ✅ **During low-usage periods** (conversion takes 5-6 minutes)
- ❌ **Skip current year** (daily updates still needed)

### Step 7: Add Cron Jobs for Automated Processing

Add the new tenant to the automated cron job schedule by updating `sql/cron.sql` and running the new cron commands.

#### Add to Hourly Update Block
Edit `sql/cron.sql` and add to the "DAILY MV UPDATE JOBS" section. **Space out the minutes** to avoid overloading the database:

```sql
-- Current pattern: LandB at :10, MM at :11
-- Add new tenant at next available minute (e.g., :12)

-- NewTenant: Every 1 hour at :12
SELECT cron.schedule(
    'foodlogstats_update_newtenant',
    '12 * * * *',
    'SELECT _wh.update_mv_by_template(''foodlogstats'', ''new_tenant_connection_name'', ''new_tenant_name'', _wh.current_date_utc());'
);
```

#### Add to Nightly 2-Week Refresh Block
Add to the "NIGHTLY 2-WEEK MV REFRESH JOBS" section. **Stagger the timing** by 2+ minutes:

```sql
-- Current pattern: LandB at 8:30 UTC, MM at 8:32 UTC
-- Add new tenant at next available time (e.g., 8:34 UTC)

-- NewTenant: Daily at 3:34 AM (Central) That's 8:34 AM UTC
SELECT cron.schedule(
    'newtenant_2week_refresh',
    '34 8 * * *',
    'SELECT _wh.update_mv_window_by_template(''foodlogstats'', ''new_tenant_connection_name'', ''new_tenant_name'', _wh.current_date_utc() - INTERVAL ''14 days'', _wh.current_date_utc() - INTERVAL ''1 day'');'
);
```

#### Add to Yearly Combination Block
Add to the "YEARLY COMBINATION JOBS" section. **Space out by 10+ minutes** due to resource intensity:

```sql
-- Current pattern: LandB at 7:00 UTC, MM at 7:10 UTC
-- Add new tenant at next available time (e.g., 7:20 UTC)

-- NewTenant: February 15th at 2:20 AM (CENTRAL) 7:20 AM UTC
SELECT cron.schedule(
    'yearly_combination_newtenant',
    '20 7 15 2 *',
    'SELECT _wh.create_combined_table_from_template_by_year(''foodlogstats'', ''new_tenant_name'', EXTRACT(YEAR FROM _wh.current_date_utc() - INTERVAL ''1 year'')::integer);'
);
```

#### Execute the New Cron Commands
```sql
-- Connect as whadmin user and run the new cron commands. Since existing cron entries already exist (see cron.jobs table), only run the new job select commands.
-- (Copy the three SELECT statements above and execute them)

-- Verify jobs were created
SELECT jobid, jobname, schedule, active
FROM cron.job
WHERE jobname LIKE '%newtenant%'
ORDER BY jobname;
```

#### Update Master Cron File
Add the new tenant entries to `sql/cron.sql` so they're included in future cron deployments:

1. **Edit `sql/cron.sql`**
2. **Add the three new cron blocks** in their respective sections
3. **Update job names** to match your tenant naming convention
4. **Commit changes** to version control

#### Timing Guidelines
- **Hourly jobs**: Space 1-2 minutes apart (avoid conflicts)
- **Nightly jobs**: Space 2+ minutes apart (database load management)
- **Yearly jobs**: Space 10+ minutes apart (high resource usage)
- **Consider tenant size**: Larger tenants may need more spacing

#### Verification
```sql
-- Monitor first few job executions
SELECT
    j.jobname,
    r.status,
    r.start_time,
    r.end_time,
    r.return_message
FROM cron.job j
LEFT JOIN cron.job_run_details r ON j.jobid = r.jobid
WHERE j.jobname LIKE '%newtenant%'
AND r.start_time > NOW() - INTERVAL '24 hours'
ORDER BY r.start_time DESC;
```

---

## Disabling a Tenant

### Overview
Temporarily disable a tenant's data processing while keeping all existing data intact. This is useful for maintenance, cost reduction, or when a tenant is temporarily inactive.

### Step 1: Disable Cron Jobs (DBeaver GUI Method - Recommended)

This is the easiest and safest way to disable tenant cron jobs.

#### Connect to Database via DBeaver
```
Host: your-warehouse.rds.amazonaws.com
Port: 5432
Database: postgres
Username: whadmin
Password: [your whadmin password]
```

#### Navigate to Cron Jobs Table
1. **Expand Database Tree**:
   - `postgres` → `Schemas` → `cron` → `Tables` → `job`
2. **Open Data Viewer**:
   - Right-click on `job` table → `View/Edit Data` → `All Rows`

#### Disable Jobs Visually
1. **Find Tenant Jobs**:
   - Look for `jobname` column entries containing your tenant name
   - Example: `foodlogstats_update_tenant_name`, `tenant_name_2week_refresh`, etc.
2. **Scroll to Active Column**:
   - Scroll right to find the `active` column (boolean checkbox)
3. **Uncheck Jobs**:
   - **Uncheck the checkbox** for each tenant job you want to disable
   - The jobs should show `false` in the `active` column
4. **Save Changes**:
   - Click the **Save** button at the bottom of the DBeaver data viewer
   - DBeaver will confirm the changes were applied

### Step 2: Verify Jobs are Disabled

#### Quick Visual Verification in DBeaver
- **Check the `active` column** for your tenant jobs - should all show `false`
- **Filter by jobname** using DBeaver's filter feature to see only your tenant jobs

#### SQL Verification (Optional)
```sql
-- Confirm jobs are inactive
SELECT jobid, jobname, schedule, active
FROM cron.job
WHERE jobname LIKE '%tenant_name%'
ORDER BY jobname;
-- All tenant jobs should show active = false
```

### Step 3: Optional - Document Reason
```sql
-- Add a comment to the tenant connection for reference
UPDATE _wh.tenant_connections
SET updated_at = NOW()
WHERE tenant_name = 'tenant_name';

-- Add to maintenance log
INSERT INTO _wh.function_logs (function_name, log_level, message, context)
VALUES ('manual_operation', 'INFO', 'Tenant disabled',
        jsonb_build_object('tenant_name', 'tenant_name', 'reason', 'maintenance', 'disabled_date', NOW()));
```

### What Happens When Disabled
- ✅ **Existing data preserved** - All MVs, yearly tables, and union views remain
- ✅ **Union views stay functional** - Reporting continues with existing data
- ✅ **Public view unaffected** - Cross-tenant reports continue working
- ❌ **No new data processing** - Daily MVs stop updating
- ❌ **Data becomes stale** - Information becomes outdated over time

---

## Re-enabling a Tenant

### Overview
Reactivate a disabled tenant and catch up on missed data processing. This process safely brings the tenant back online with all historical data.

### Step 1: Determine Catch-Up Period
```sql
-- Find the last updated MV to see how far behind we are
SELECT MAX(matviewname) as last_mv
FROM pg_matviews
WHERE schemaname = 'tenant_schema'
AND matviewname LIKE 'foodlogstats_%';

-- Check tenant connection is still valid
SELECT * FROM _wh.tenant_connections
WHERE tenant_name = 'tenant_connection_name';

-- Test connection
SELECT * FROM dblink(
    _wh.get_tenant_connection_string('tenant_connection_name'),
    'SELECT 1 as test'
) AS t(test integer);
```

### Step 2: Re-enable Cron Jobs (DBeaver GUI Method - Recommended)

#### Connect to Database via DBeaver
```
Host: your-warehouse.rds.amazonaws.com
Port: 5432
Database: postgres
Username: whadmin
Password: [your whadmin password]
```

#### Navigate to Cron Jobs Table
1. **Expand Database Tree**:
   - `postgres` → `Schemas` → `cron` → `Tables` → `job`
2. **Open Data Viewer**:
   - Right-click on `job` table → `View/Edit Data` → `All Rows`

#### Re-enable Jobs Visually
1. **Find Disabled Tenant Jobs**:
   - Look for your tenant jobs with `active = false`
   - Filter by `jobname` containing your tenant name if needed
2. **Check the Active Column**:
   - Scroll right to find the `active` column (boolean checkbox)
3. **Check Jobs**:
   - **Check the checkbox** for each tenant job you want to re-enable
   - The jobs should show `true` in the `active` column
4. **Save Changes**:
   - Click the **Save** button at the bottom of the DBeaver data viewer
   - DBeaver will confirm the changes were applied

### Step 3: Backfill Missing Data
```sql
-- Backfill from last MV date to current date
-- IMPORTANT: Be conservative with date ranges to avoid overloading source DB
SELECT _wh.update_mv_window_by_template(
    'foodlogstats',
    'tenant_connection_name',
    'tenant_schema',
    '2024-12-01'::date,  -- Start from last known good date
    _wh.current_date_utc() - INTERVAL '1 day'  -- Up to yesterday
);

-- For large gaps, process in smaller chunks (1 month at a time)
SELECT _wh.update_mv_window_by_template(
    'foodlogstats',
    'tenant_connection_name',
    'tenant_schema',
    '2024-12-01'::date,
    '2024-12-31'::date
);
```

### Step 4: Verify Catch-Up Success
```sql
-- Check data integrity for recent period
SELECT _wh.check_views_by_template_for_year('foodlogstats', 'tenant_schema', 2024);

-- Verify union view is working
SELECT COUNT(*) FROM tenant_schema.foodlogstats
WHERE logged_time >= '2024-12-01'::date;

-- Update public master view
SELECT _wh.update_public_view_by_template('foodlogstats');
```

### Step 5: Monitor First Few Runs
```sql
-- Monitor cron job execution
SELECT j.jobname, r.status, r.start_time, r.end_time, r.return_message
FROM cron.job j
JOIN cron.job_run_details r ON j.jobid = r.jobid
WHERE j.jobname LIKE '%tenant_name%'
AND r.start_time > NOW() - INTERVAL '24 hours'
ORDER BY r.start_time DESC;
```

### Best Practices for Re-enabling
- **Start small** - Backfill in monthly chunks to avoid overwhelming source systems
- **Monitor performance** - Watch for any degradation during catch-up
- **Verify data quality** - Check for any anomalies in backfilled data
- **Coordinate timing** - Re-enable during off-peak hours when possible

---

## Dropping a Tenant

### ⚠️ WARNING: This will permanently delete all tenant data!

### Step 1: Disable Cron Jobs First
```sql
-- Connect as whadmin user
-- Stop all automated processing for the tenant
UPDATE cron.job
SET active = false
WHERE jobname LIKE '%tenant_name%';

-- Verify jobs are disabled
SELECT jobid, jobname, active
FROM cron.job
WHERE jobname LIKE '%tenant_name%';
```

### Step 2: Remove Cron Job Definitions
```sql
-- Permanently delete the cron jobs (optional - disabling is sufficient)
SELECT cron.unschedule('foodlogstats_update_tenant_name');
SELECT cron.unschedule('tenant_name_2week_refresh');
SELECT cron.unschedule('yearly_combination_tenant_name');
```

### Step 3: Drop Tenant Schema
```sql
-- This will cascade and drop all materialized views, yearly tables, and union views
DROP SCHEMA tenant_schema_name CASCADE;
```

### Step 4: Update Public Master View
```sql
-- CRITICAL: Update public view to exclude dropped tenant
-- This prevents the public view from breaking when the tenant schema is gone

-- Option A: Automatic update (recommended)
SELECT _wh.update_public_view_by_template('foodlogstats');

-- Option B: Manual verification of what's left
SELECT DISTINCT schemaname
FROM pg_views
WHERE viewname = 'foodlogstats'
AND schemaname NOT IN ('public', '_wh');

-- The public view will now only include remaining active tenants
```

### Step 5: Remove Connection Details
```sql
-- Remove tenant connection details
DELETE FROM _wh.tenant_connections
WHERE tenant_name = 'tenant_connection_name';
```

### Step 6: Verify Public View Integrity
```sql
-- Test that public view still works
SELECT COUNT(*) FROM public.foodlogstats;

-- Verify which tenants are still included
SELECT schema_name, COUNT(*) as record_count
FROM public.foodlogstats
GROUP BY schema_name
ORDER BY schema_name;
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

## Converting Daily MVs to Yearly Tables

### Overview
At the end of each year, convert daily materialized views to a single yearly table for improved performance and storage efficiency. This process combines all daily MVs for a specific year into one table and removes the individual daily MVs.

### Prerequisites
- All daily MVs for the target year must exist
- No other processes should be updating the same tenant during this operation
- Ensure sufficient disk space for the yearly table creation

### Step 1: Verify Year is Complete
```sql
-- Check that all expected daily MVs exist for the year
SELECT COUNT(*) as daily_mvs_count
FROM pg_matviews
WHERE schemaname = 'tenant_schema'
AND matviewname LIKE 'foodlogstats_2024_%';

-- Should be 365 or 366 for leap years
```

### Step 2: Run Yearly Combination
```sql
-- Connect as whadmin user
-- Combine all 2024 daily MVs into a yearly table
SELECT _wh.create_combined_table_from_template_by_year(
    'foodlogstats',     -- template name
    'tenant_schema',    -- target schema
    2024               -- target year
);
```

### Step 3: Verify Results
```sql
-- Check that yearly table was created
SELECT COUNT(*) FROM tenant_schema.foodlogstats_2024;

-- Verify tenant union view includes the yearly table
SELECT COUNT(*) FROM tenant_schema.foodlogstats
WHERE EXTRACT(YEAR FROM logged_time AT TIME ZONE 'UTC') = 2024;

-- Update public union view to include changes
SELECT _wh.update_public_view_by_template('foodlogstats');
```

### Step 4: Monitor and Validate
```sql
-- Check function results (returned as JSON)
-- Look for: {"success": true, "processed_mvs": 365, "total_records": 1529432}

-- Verify no daily MVs remain for the year
SELECT COUNT(*) as remaining_daily_mvs
FROM pg_matviews
WHERE schemaname = 'tenant_schema'
AND matviewname LIKE 'foodlogstats_2024_%';
-- Should be 0
```

### What the Function Does
1. **Validates** template exists and yearly table doesn't exist
2. **Temporarily modifies** tenant union view to exclude target year MVs
3. **Creates yearly table** using template column definitions and indexes
4. **Processes each daily MV**: INSERT data → DROP MV
5. **Recreates union view** to include new yearly table
6. **Returns detailed results** including counts and timing

### Rollback Plan
If the operation fails:
- Transaction automatically rolls back
- All daily MVs remain intact
- Tenant union view is restored to original state
- No data loss occurs

### Performance Notes
- Operation runs in a single transaction (may take 30-60 minutes for large datasets)
- During processing, the tenant union view temporarily excludes the target year
- Public union view remains accessible throughout the process
- Storage space is reduced by ~30-50% after yearly conversion

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

**🚨 CRITICAL TIMEZONE REQUIREMENT:**
- **ALL datetime filtering in templates MUST use UTC timezone**
- **NEVER use `timestamp::DATE` - ALWAYS use `(timestamp AT TIME ZONE 'UTC')::DATE`**
- **Failure to use UTC will cause data inconsistencies and cross-date contamination**

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
WHERE (i.timestamp AT TIME ZONE 'UTC')::DATE = '{TARGET_DATE}'::DATE
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

## Updating Warehouse Functions

When you need to modify or add warehouse functions (in `sql/functions.sql`), here are the recommended methods for updating them in the database.

### Method 1: DBeaver GUI Approach (Recommended)

This is the safest and most visual method for function updates.

#### Step 1: Connect to Database
```
Host: your-warehouse.rds.amazonaws.com
Port: 5432
Database: postgres
Username: postgres
Password: [your postgres password]
```

#### Step 2: Drop Existing Functions via DBeaver
1. **Navigate to Functions**:
   - Expand `postgres` database → `Schemas` → `_wh` → `Functions`
2. **Select Functions to Update**:
   - Hold `Ctrl/Cmd` and click multiple functions to select them
   - Right-click → `Delete` (or press `Delete` key)
3. **Confirm Deletion**:
   - DBeaver will show you the DROP statements
   - Click `Proceed` to execute

#### Step 3: Run Updated Functions Script
1. **Open SQL Editor**: File → New → SQL Editor
2. **Load Script**: Open `sql/functions.sql`
3. **Execute**: Click the "Execute SQL Script" button (or `Ctrl+Shift+Enter`)
4. **Verify Success**: Check for any error messages in the output

### Method 2: Command Line Approach

For users comfortable with psql command line.

#### Step 1: Connect as postgres user
```bash
psql -h your-warehouse.rds.amazonaws.com -U postgres -d postgres
```

#### Step 2: Drop Functions (Script Method)
```sql
-- Drop all _wh functions at once
DO $$
DECLARE
    func_record RECORD;
BEGIN
    FOR func_record IN
        SELECT routine_name, routine_type
        FROM information_schema.routines
        WHERE routine_schema = '_wh'
        AND routine_type = 'FUNCTION'
    LOOP
        EXECUTE 'DROP FUNCTION IF EXISTS _wh.' || func_record.routine_name || ' CASCADE';
        RAISE NOTICE 'Dropped function: %', func_record.routine_name;
    END LOOP;
END $$;
```

#### Step 3: Load New Functions
```sql
-- Execute the functions script
\i sql/functions.sql
```

### Method 3: CREATE OR REPLACE (Automatic)

**Note**: This only works if function signatures haven't changed. If you've added/removed parameters, you must use Method 1 or 2.

```bash
# Simply re-run the functions file
psql -h your-warehouse.rds.amazonaws.com -U postgres -d postgres -f sql/functions.sql
```

### Method 4: Individual Function Updates

For updating just one or two specific functions.

```sql
-- Connect as postgres user
-- Drop specific function(s)
DROP FUNCTION IF EXISTS _wh.update_mv_by_template(text, text, text, date, boolean, boolean) CASCADE;
DROP FUNCTION IF EXISTS _wh.update_mv_window_by_template(text, text, text, date, date) CASCADE;

-- Then run the functions.sql script to recreate them
\i sql/functions.sql
```

### Verification Steps

After any function update method:

```sql
-- Check that all expected functions exist
SELECT
    routine_name,
    routine_type,
    data_type
FROM information_schema.routines
WHERE routine_schema = '_wh'
ORDER BY routine_name;

-- Test a critical function
SELECT _wh.current_date_utc();

-- Check function permissions
SELECT
    p.proname as function_name,
    array_to_string(p.proacl, ', ') as permissions
FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE n.nspname = '_wh'
ORDER BY p.proname;
```

### Safety Considerations

#### Before Updating Functions:
- ✅ **Check for active jobs**: Ensure no cron jobs are currently running
- ✅ **Backup connection data**: Export `_wh.tenant_connections` if needed
- ✅ **Test in development**: Always test function changes on a dev instance first

#### Function Dependencies:
- **Templates remain intact**: Function updates don't affect `_wh.mv_templates`
- **Data preservation**: Existing MVs and yearly tables are unaffected
- **Permission grants**: May need to re-grant execute permissions after updates

### Troubleshooting Function Updates

#### Issue: "Function does not exist" errors
```sql
-- Check if function was actually dropped
SELECT routine_name
FROM information_schema.routines
WHERE routine_schema = '_wh'
AND routine_name = 'problematic_function_name';
```

#### Issue: "Permission denied" errors
```sql
-- Re-grant permissions to whadmin
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA _wh TO whadmin;
```

#### Issue: "Function signature mismatch"
```sql
-- Find all versions of a function
SELECT
    p.proname,
    pg_catalog.pg_get_function_arguments(p.oid) as arguments
FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE n.nspname = '_wh'
AND p.proname = 'function_name'
ORDER BY p.proname;

-- Drop all versions with CASCADE
DROP FUNCTION _wh.function_name CASCADE;
```

### Best Practices

1. **Use DBeaver GUI** for complex updates - visual confirmation reduces errors
2. **Test immediately** after function updates with simple function calls
3. **Update during maintenance windows** to avoid disrupting active operations
4. **Keep function backups** - save previous versions of `functions.sql` before major changes
5. **Document changes** - Note what functions were modified and why

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