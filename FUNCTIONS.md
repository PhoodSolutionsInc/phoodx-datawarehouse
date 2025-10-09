# Data Warehouse Functions Reference

This document provides detailed reference for all `_wh` schema functions in the template-based data warehouse using the **domain-first naming convention**.

## Table of Contents

- [Materialized View Functions](#materialized-view-functions)
  - [_wh.mv_create_from_template](#_whmv_create_from_template)
  - [_wh.mv_update_by_template](#_whmv_update_by_template)
  - [_wh.mv_update_window_by_template](#_whmv_update_window_by_template)
- [Union View Functions](#union-view-functions)
  - [_wh.union_view_update_tenant_by_template](#_whunion_view_update_tenant_by_template)
  - [_wh.union_view_update_public_by_template](#_whunion_view_update_public_by_template)
- [Year Table Functions](#year-table-functions)
  - [_wh.year_table_create_combined_from_template](#_whyear_table_create_combined_from_template)
  - [_wh.year_table_get_combined_view_by_template](#_whyear_table_get_combined_view_by_template)
  - [_wh.year_table_check_views_by_template_for_year](#_whyear_table_check_views_by_template_for_year)
- [Schema Modification Functions](#schema-modification-functions)
  - [_wh.schema_add_column_to_template](#_whschema_add_column_to_template)
  - [_wh.schema_add_column_to_all_mvs](#_whschema_add_column_to_all_mvs)
  - [_wh.schema_add_column_to_union_views](#_whschema_add_column_to_union_views)
- [Utility Functions](#utility-functions)
  - [_wh.mv_create_name](#_whmv_create_name)
  - [_wh.mv_does_exist](#_whmv_does_exist)
  - [_wh.mv_refresh](#_whmv_refresh)
  - [_wh.util_get_tenant_connection_string](#_whutil_get_tenant_connection_string)
  - [_wh.util_current_date_utc](#_whutil_current_date_utc)
- [Logging Functions](#logging-functions)
  - [_wh.log_info, _wh.log_error, _wh.log_debug](#_whlog_helpers)

---

## Materialized View Functions

### _wh.mv_create_from_template

Creates a materialized view from a template definition.

**Signature:**
```sql
_wh.mv_create_from_template(
    template_name text,
    tenant_connection_name text,
    target_schema text,
    target_date date,
    update_union_view boolean DEFAULT true
) RETURNS boolean
```

**Parameters:**
- `template_name`: Name of the template in `_wh.mv_templates`
- `tenant_connection_name`: Connection identifier from `_wh.tenant_connections`
- `target_schema`: Schema to create the MV in
- `target_date`: Date for the MV (substituted as `{TARGET_DATE}`)
- `update_union_view`: Whether to update the union view after creating the MV

**Returns:** `TRUE` on success, `FALSE` on failure

**What it does:**
1. Retrieves template definition from `_wh.mv_templates`
2. Substitutes date and schema placeholders in the template query
3. Creates materialized view with proper indexes
4. Optionally updates the union view for the schema

**Example:**
```sql
SELECT _wh.mv_create_from_template('foodlogstats', 'landb-prod', 'landb', '2025-09-24'::date);
```


### _wh.mv_update_by_template

Updates or creates a materialized view using a template. If the MV exists, it refreshes; if not, it creates.

**Signature:**
```sql
_wh.mv_update_by_template(
    template_name text,
    tenant_connection_name text,
    target_schema text,
    target_date date DEFAULT NULL,
    allow_refresh_yesterday boolean DEFAULT true,
    update_union_view boolean DEFAULT true
) RETURNS boolean
```

**Parameters:**
- `template_name`: Name of the template in `_wh.mv_templates`
- `tenant_connection_name`: Connection identifier
- `target_schema`: Schema to update the MV in
- `target_date`: Date for the MV (defaults to current UTC date)
- `allow_refresh_yesterday`: If true and target_date is today, also refresh yesterday's MV
- `update_union_view`: Whether to update the union view after operations

**Returns:** `TRUE` on success, `FALSE` on failure

**What it does:**
1. Optionally refreshes yesterday's MV if it exists
2. Creates new MV or refreshes existing one for target date
3. Optionally updates the union view for the schema
4. Provides comprehensive logging

**Example:**
```sql
SELECT _wh.mv_update_by_template('foodlogstats', 'landb-prod', 'landb');
```


### _wh.mv_update_window_by_template

Bulk updates materialized views for a date range using templates.

**Signature:**
```sql
_wh.mv_update_window_by_template(
    template_name text,
    tenant_connection_name text,
    target_schema text,
    start_date date,
    end_date date,
    update_union_view boolean DEFAULT true
) RETURNS jsonb
```

**Parameters:**
- `template_name`: Name of the template
- `tenant_connection_name`: Connection identifier
- `target_schema`: Schema to update MVs in
- `start_date`: Start date (inclusive)
- `end_date`: End date (inclusive)
- `update_union_view`: Whether to update union view after window completion

**Returns:** JSON object with detailed summary statistics

**What it does:**
1. Validates date range parameters
2. Loops through each date, creating/refreshing MVs
3. Includes 1-second pause between days to protect source database
4. Optionally updates union view once at the end
5. Returns comprehensive statistics and per-date results

**Example:**
```sql
SELECT _wh.mv_update_window_by_template('foodlogstats', 'landb-prod', 'landb', '2025-09-01'::date, '2025-09-30'::date);
```


---

## Union View Functions

### _wh.union_view_update_tenant_by_template

Creates or updates a tenant's union view that combines all daily MVs and yearly tables for a template.

**Signature:**
```sql
_wh.union_view_update_tenant_by_template(
    template_name text,
    target_schema text
) RETURNS boolean
```

**Parameters:**
- `template_name`: Name of the template (e.g., 'foodlogstats')
- `target_schema`: Schema to create the union view in

**Returns:** `TRUE` on success, `FALSE` on failure

**What it does:**
1. Finds all daily MVs matching `{template_name}_{YYYY_MM_DD}` pattern
2. Finds all yearly tables matching `{template_name}_{YYYY}` pattern
3. Creates optimized UNION ALL query combining daily MVs and yearly tables
4. Creates view `{target_schema}.{template_name}`

**Creates:** `target_schema.template_name` view

**Example:**
```sql
SELECT _wh.union_view_update_tenant_by_template('foodlogstats', 'landb');
-- Creates: landb.foodlogstats (combines daily MVs + yearly tables)
```


### _wh.union_view_update_public_by_template

Creates or updates the public master view that combines all tenant union views with schema context.

**Signature:**
```sql
_wh.union_view_update_public_by_template(
    template_name text
) RETURNS boolean
```

**Parameters:**
- `template_name`: Name of the template (e.g., 'foodlogstats')

**Returns:** `TRUE` on success, `FALSE` on failure

**What it does:**
1. Discovers all schemas containing union views for the template
2. Creates UNION ALL query combining all tenant union views
3. Adds `schema_name` column to identify data source
4. Creates view `public.{template_name}`

**Creates:** `public.template_name` view with `schema_name` column

**Example:**
```sql
SELECT _wh.union_view_update_public_by_template('foodlogstats');
-- Creates: public.foodlogstats (with schema_name column)
```


## Year Table Functions

### _wh.year_table_create_combined_from_template

Creates a yearly combination table from all daily MVs for a specific year using a template.

**Signature:**
```sql
_wh.year_table_create_combined_from_template(
    template_name text,
    target_schema text,
    target_year integer
) RETURNS boolean
```

**Parameters:**
- `template_name`: Name of the template (e.g., 'foodlogstats')
- `target_schema`: Schema containing the daily MVs
- `target_year`: Year to combine (e.g., 2024)

**Returns:** `TRUE` on success, `FALSE` on failure

**What it does:**
1. Finds all daily MVs for the year matching `{template_name}_{year}_*` pattern
2. Creates optimized UNION ALL query combining all daily MVs
3. Creates yearly table `{template_name}_{year}` as a regular table (not MV)
4. Adds proper indexes for the yearly table
5. Archives daily data for long-term storage

**Example:**
```sql
SELECT _wh.year_table_create_combined_from_template('foodlogstats', 'landb', 2024);
-- Creates: landb.foodlogstats_2024 (from all landb.foodlogstats_2024_* MVs)
```

### _wh.year_table_get_combined_view_by_template

Generates the SQL for a yearly combination view without executing it.

**Signature:**
```sql
_wh.year_table_get_combined_view_by_template(
    template_name text,
    target_schema text,
    target_year integer
) RETURNS text
```

**Parameters:**
- `template_name`: Name of the template
- `target_schema`: Schema containing the MVs
- `target_year`: Year to generate SQL for

**Returns:** Complete SQL statement for creating the yearly table

**Use Case:** Preview SQL before execution, debugging, or manual execution

**Example:**
```sql
SELECT _wh.year_table_get_combined_view_by_template('foodlogstats', 'landb', 2024);
-- Returns: "CREATE TABLE landb.foodlogstats_2024 AS SELECT * FROM landb.foodlogstats_2024_01_01 UNION ALL..."
```

### _wh.year_table_check_views_by_template_for_year

Checks data integrity and completeness for all MVs in a specific year.

**Signature:**
```sql
_wh.year_table_check_views_by_template_for_year(
    template_name text,
    target_schema text,
    target_year integer
) RETURNS jsonb
```

**Parameters:**
- `template_name`: Name of the template
- `target_schema`: Schema to check
- `target_year`: Year to analyze

**Returns:** JSON object with comprehensive analysis

**What it analyzes:**
1. Expected vs actual number of daily MVs
2. Missing dates identification
3. Record counts per MV and totals
4. Data distribution statistics
5. Potential data quality issues

**Example:**
```sql
SELECT _wh.year_table_check_views_by_template_for_year('foodlogstats', 'landb', 2024);
```

**Return Example:**
```json
{
  "year": 2024,
  "schema": "landb",
  "template": "foodlogstats",
  "expected_days": 366,
  "actual_mvs": 364,
  "missing_dates": ["2024-02-30", "2024-11-15"],
  "total_records": 1250000,
  "avg_daily_records": 3431,
  "data_quality_flags": []
}
```

## Schema Modification Functions

### _wh.schema_add_column_to_template

Adds a new column definition to an existing template.

**Signature:**
```sql
_wh.schema_add_column_to_template(
    template_name text,
    column_name text,
    column_definition text,
    add_to_indexes text DEFAULT NULL
) RETURNS boolean
```

**Parameters:**
- `template_name`: Name of the template to modify
- `column_name`: Name of the new column
- `column_definition`: Complete column definition (e.g., 'new_field TEXT')
- `add_to_indexes`: Optional index statement to add

**Returns:** `TRUE` on success, `FALSE` on failure

**What it does:**
1. Updates the `column_definitions` in `_wh.mv_templates`
2. Optionally adds new index definitions
3. Validates column definition syntax

**Example:**
```sql
SELECT _wh.schema_add_column_to_template(
    'foodlogstats',
    'waste_category',
    'waste_category TEXT',
    'CREATE INDEX idx_{SCHEMA}_{VIEW_NAME}_waste_category ON {SCHEMA}.{VIEW_NAME} (waste_category);'
);
```

### _wh.schema_add_column_to_all_mvs

Adds a column to all existing materialized views for a template in a schema.

**Signature:**
```sql
_wh.schema_add_column_to_all_mvs(
    template_name text,
    target_schema text,
    column_name text,
    column_definition text,
    default_value text DEFAULT 'NULL'
) RETURNS jsonb
```

**Parameters:**
- `template_name`: Name of the template
- `target_schema`: Schema containing the MVs
- `column_name`: Name of the new column
- `column_definition`: PostgreSQL column definition
- `default_value`: Default value for existing records

**Returns:** JSON object with operation results

**What it does:**
1. Finds all MVs matching the template pattern
2. Adds the column to each MV using `ALTER TABLE`
3. Sets default values for existing records
4. Creates any associated indexes

**Example:**
```sql
SELECT _wh.schema_add_column_to_all_mvs(
    'foodlogstats',
    'landb',
    'waste_category',
    'TEXT',
    '''unknown'''
);
```

### _wh.schema_add_column_to_union_views

Updates union views to include the new column after schema modifications.

**Signature:**
```sql
_wh.schema_add_column_to_union_views(
    template_name text,
    target_schema text DEFAULT NULL
) RETURNS boolean
```

**Parameters:**
- `template_name`: Name of the template
- `target_schema`: Specific schema to update, or NULL for all schemas

**Returns:** `TRUE` on success, `FALSE` on failure

**What it does:**
1. Recreates tenant union views with updated column list
2. Recreates public master view with updated schema
3. Ensures all views reflect the new column structure

**Example:**
```sql
-- Update specific schema
SELECT _wh.schema_add_column_to_union_views('foodlogstats', 'landb');

-- Update all schemas
SELECT _wh.schema_add_column_to_union_views('foodlogstats');
```



---

## Utility Functions

### _wh.mv_create_name

Generates standardized materialized view names with date suffixes.

**Signature:**
```sql
_wh.mv_create_name(mvname text, target_date date) RETURNS text
```

**Parameters:**
- `mvname`: Base name for the materialized view
- `target_date`: Date to append to the name

**Returns:** Formatted view name with date suffix

**Format:** `{mvname}_{YYYY_MM_DD}`

**Examples:**
```sql
SELECT _wh.mv_create_name('foodlogstats', '2024-01-15'::date);
-- Returns: 'foodlogstats_2024_01_15'

SELECT _wh.mv_create_name('inventory_stats', _wh.util_current_date_utc());
-- Returns: 'inventory_stats_2024_01_15' (if today is 2024-01-15 UTC)
```


### _wh.mv_does_exist

Checks if a materialized view exists in a specific schema.

**Signature:**
```sql
_wh.mv_does_exist(target_schema text, view_name text) RETURNS boolean
```

**Parameters:**
- `target_schema`: Schema name to check
- `view_name`: Materialized view name to check

**Returns:** `TRUE` if the materialized view exists, `FALSE` otherwise

**Examples:**
```sql
-- Check if specific MV exists
SELECT _wh.mv_does_exist('landb', 'foodlogstats_2024_01_15');
-- Returns: true or false

-- Use in conditional logic
IF _wh.mv_does_exist('landb', 'foodlogstats_2024_01_15') THEN
    PERFORM _wh.log_info('MV exists, will refresh');
ELSE
    PERFORM _wh.log_info('MV does not exist, will create');
END IF;
```

### _wh.mv_refresh

Refreshes an existing materialized view using CONCURRENT refresh.

**Signature:**
```sql
_wh.mv_refresh(target_schema text, view_name text) RETURNS boolean
```

**Parameters:**
- `target_schema`: Schema containing the materialized view
- `view_name`: Name of the materialized view to refresh

**Returns:** `TRUE` if successful, `FALSE` if failed

**What it does:**
1. Executes `REFRESH MATERIALIZED VIEW CONCURRENTLY`
2. Uses concurrent refresh to avoid locking the view during refresh
3. Provides comprehensive logging for success/failure
4. Handles exceptions gracefully

**Examples:**
```sql
-- Refresh a specific MV
SELECT _wh.mv_refresh('landb', 'foodlogstats_2024_01_15');

-- Use in conditional logic
IF NOT _wh.mv_refresh('landb', 'foodlogstats_2024_01_15') THEN
    PERFORM _wh.log_error('Failed to refresh MV');
END IF;
```

**Note:** Requires the materialized view to have a unique index for concurrent refresh.

**Backward Compatibility:** `_wh.refresh_mv` → `_wh.mv_refresh`

### _wh.util_get_tenant_connection_string

Retrieves a formatted connection string for dblink operations.

**Signature:**
```sql
_wh.util_get_tenant_connection_string(tenant_name text) RETURNS text
```

**Parameters:**
- `tenant_name`: Name of the tenant connection to retrieve

**Returns:** Formatted connection string for use with dblink

**Security:** Function is marked `SECURITY DEFINER` to allow controlled access to connection details

**What it does:**
1. Looks up connection details from `_wh.tenant_connections`
2. Formats as dblink-compatible connection string
3. Raises exception if tenant not found

**Examples:**
```sql
-- Get connection string
SELECT _wh.util_get_tenant_connection_string('landb-prod');
-- Returns: 'host=landb.rds.amazonaws.com port=5432 dbname=production user=readonly_user password=secret123'

-- Use in dblink query
SELECT * FROM dblink(
    _wh.util_get_tenant_connection_string('landb-prod'),
    'SELECT COUNT(*) FROM phood_foodlogsum'
) AS t(record_count bigint);
```

**Backward Compatibility:** `_wh.get_tenant_connection_string` → `_wh.util_get_tenant_connection_string`

### _wh.util_current_date_utc

Returns the current date in UTC timezone with optional interval offset.

**Signature:**
```sql
_wh.util_current_date_utc(interval_offset integer DEFAULT 0) RETURNS date
```

**Parameters:**
- `interval_offset`: Days to add/subtract from current UTC date (e.g., -14 for 14 days ago)

**Returns:** Current date in UTC timezone with optional offset

**Purpose:** Ensures consistent UTC date handling across all warehouse operations

**Examples:**
```sql
-- Get current UTC date
SELECT _wh.util_current_date_utc();
-- Returns: '2024-01-15' (if current UTC date is Jan 15, 2024)

-- Get date 14 days ago
SELECT _wh.util_current_date_utc(-14);
-- Returns: '2024-01-01' (if current UTC date is Jan 15, 2024)

-- Get tomorrow's date
SELECT _wh.util_current_date_utc(1);
-- Returns: '2024-01-16' (if current UTC date is Jan 15, 2024)

-- Use in MV operations
SELECT _wh.mv_update_by_template('foodlogstats', 'landb-prod', 'landb', _wh.util_current_date_utc());
```

**Note:** This function is critical for maintaining UTC consistency across the entire warehouse system and simplifies date arithmetic in cron jobs.

## Logging Functions

### _wh.log

Central logging function with multiple log levels and real-time timestamps.

**Signature:**
```sql
_wh.log(
    level text,
    message text,
    context jsonb DEFAULT NULL
) RETURNS void
```

**Parameters:**
- `level`: Log level ('DEBUG', 'INFO', 'WARN', 'ERROR')
- `message`: Log message text
- `context`: Optional JSONB context data

**Log Levels:**
- `DEBUG`: Detailed debugging information
- `INFO`: General information (uses NOTICE)
- `WARN`: Warning messages
- `ERROR`: Error messages (uses WARNING for DataDog compatibility)

**Features:**
- Uses `clock_timestamp()` for real-time timestamps during long operations
- Structured logging with optional JSON context
- Consistent formatting across all warehouse operations

**Examples:**
```sql
-- Simple info message
PERFORM _wh.log('INFO', 'Starting data warehouse refresh');

-- Error with context
PERFORM _wh.log('ERROR', 'Failed to connect to tenant database',
    jsonb_build_object('tenant', 'landb-prod', 'error_code', 'CONN_TIMEOUT'));

-- Debug message with structured data
PERFORM _wh.log('DEBUG', 'Processing date range',
    jsonb_build_object('start_date', '2024-01-01', 'end_date', '2024-01-31', 'total_days', 31));
```

**Output Format:** `[WH][real-time-timestamp] message`

### _wh.log_info, _wh.log_warn, _wh.log_error, _wh.log_debug

Convenience wrapper functions for specific log levels.

### _wh.log_function_start, _wh.log_function_end, _wh.log_function_error

Specialized logging functions for tracking function execution lifecycle.

**Signatures:**
```sql
_wh.log_function_start(function_name text, message text DEFAULT NULL) RETURNS void
_wh.log_function_end(function_name text, message text DEFAULT NULL, success boolean DEFAULT true) RETURNS void
_wh.log_function_error(function_name text, error_message text) RETURNS void
```

**Examples:**
```sql
-- Track function execution
PERFORM _wh.log_function_start('mv_update_by_template', 'Processing landb foodlogstats');
-- ... function logic ...
PERFORM _wh.log_function_end('mv_update_by_template', 'Completed successfully');

-- Handle errors
PERFORM _wh.log_function_error('mv_update_by_template', 'Connection timeout: ' || SQLERRM);
```

---

## Cron Wrapper Functions

These functions provide simplified interfaces for common cron job operations, replacing the verbose function calls with cleaner syntax.

### _wh.cron_refresh_today

Refreshes today's materialized view for a tenant, with optional yesterday refresh.

**Signature:**
```sql
_wh.cron_refresh_today(
    template_name text,
    tenant_connection_name text,
    target_schema text
) RETURNS boolean
```

**Parameters:**
- `template_name`: Name of the template (e.g., 'foodlogstats')
- `tenant_connection_name`: Connection identifier from `_wh.tenant_connections`
- `target_schema`: Schema to refresh the MV in

**Returns:** `TRUE` on success, `FALSE` on failure

**What it does:**
1. Calls `_wh.mv_update_by_template()` with current UTC date
2. Automatically refreshes yesterday's MV if it exists
3. Updates union view after completion
4. Provides comprehensive logging

**Example:**
```sql
SELECT _wh.cron_refresh_today('foodlogstats', 'af-prod', 'af');
```

**Replaces:**
```sql
SELECT _wh.mv_update_by_template('foodlogstats', 'af-prod', 'af');
```

### _wh.cron_refresh_recent

Refreshes recent materialized views (last N days) for a tenant.

**Signature:**
```sql
_wh.cron_refresh_recent(
    template_name text,
    tenant_connection_name text,
    target_schema text,
    days_back integer
) RETURNS jsonb
```

**Parameters:**
- `template_name`: Name of the template
- `tenant_connection_name`: Connection identifier
- `target_schema`: Schema to refresh MVs in
- `days_back`: Number of days back to refresh (e.g., 14 for 2 weeks)

**Returns:** JSON object with detailed summary statistics

**What it does:**
1. Calculates date range: (current_date - days_back) to (current_date - 1)
2. Calls `_wh.mv_update_window_by_template()` with calculated range
3. Updates union view after completion
4. Returns comprehensive statistics

**Example:**
```sql
SELECT _wh.cron_refresh_recent('foodlogstats', 'af-prod', 'af', 14);
```

**Replaces:**
```sql
SELECT _wh.mv_update_window_by_template('foodlogstats', 'af-prod', 'af',
    _wh.util_current_date_utc(-14), _wh.util_current_date_utc(-1));
```

### _wh.cron_combine_last_year

Creates a yearly combination table from last year's daily MVs.

**Signature:**
```sql
_wh.cron_combine_last_year(
    template_name text,
    target_schema text
) RETURNS jsonb
```

**Parameters:**
- `template_name`: Name of the template
- `target_schema`: Schema containing the daily MVs

**Returns:** JSON object with operation details

**What it does:**
1. Calculates last year: EXTRACT(YEAR FROM current_date - 1 year)
2. Calls `_wh.year_table_create_combined_from_template()`
3. Returns operation statistics and success status

**Example:**
```sql
SELECT _wh.cron_combine_last_year('foodlogstats', 'af');
```

**Replaces:**
```sql
SELECT _wh.year_table_create_combined_from_template('foodlogstats', 'af',
    EXTRACT(YEAR FROM _wh.util_current_date_utc(-365))::integer);
```

---

**Signatures:**
```sql
_wh.log_info(message text, context jsonb DEFAULT NULL) RETURNS void
_wh.log_warn(message text, context jsonb DEFAULT NULL) RETURNS void
_wh.log_error(message text, context jsonb DEFAULT NULL) RETURNS void
_wh.log_debug(message text, context jsonb DEFAULT NULL) RETURNS void
```

**Examples:**
```sql
-- Equivalent ways to log
PERFORM _wh.log('INFO', 'Operation completed');
PERFORM _wh.log_info('Operation completed');

-- With context
PERFORM _wh.log_error('Database connection failed',
    jsonb_build_object('tenant', 'landb-prod', 'operation', 'mv_create'));

-- Multiple context fields
PERFORM _wh.log_info('MV created successfully',
    jsonb_build_object(
        'mv_name', 'foodlogstats_2024_01_15',
        'schema', 'landb',
        'records', 15420,
        'duration_ms', 850
    ));
```


## Function Dependencies

### Function Call Hierarchy

```
_wh.mv_update_by_template
├── _wh.mv_create_name
├── _wh.mv_does_exist
├── _wh.mv_refresh
├── _wh.mv_create_from_template
│   ├── _wh.util_get_tenant_connection_string
│   ├── _wh.mv_create_name
│   └── _wh.log_info, _wh.log_error
├── _wh.union_view_update_tenant_by_template (optional)
└── _wh.log_info, _wh.log_error, _wh.log_debug

_wh.mv_update_window_by_template
├── Loop: _wh.mv_update_by_template (per date)
├── _wh.union_view_update_tenant_by_template (optional, at end)
└── _wh.log_info, _wh.log_error

_wh.union_view_update_tenant_by_template
├── _wh.util_current_date_utc
└── _wh.log_info, _wh.log_error

_wh.year_table_create_from_template
└── _wh.log_info, _wh.log_error
```

### Typical Usage Patterns

**Daily Operations:**
```sql
-- Update current day for foodlogstats (new cron wrapper)
SELECT _wh.cron_refresh_today('foodlogstats', 'landb-prod', 'landb');

-- Update union view (usually automatic)
SELECT _wh.union_view_update_tenant_by_template('foodlogstats', 'landb');

-- Update public master view
SELECT _wh.union_view_update_public_by_template('foodlogstats');
```

**Historical Backfill:**
```sql
-- Backfill date range (new cron wrapper for recent data)
SELECT _wh.cron_refresh_recent('foodlogstats', 'landb-prod', 'landb', 30);

-- Manual date range (union view updated at end)
SELECT _wh.mv_update_window_by_template(
    'foodlogstats', 'landb-prod', 'landb',
    '2024-01-01'::date, '2024-01-31'::date
);
```

**Yearly Operations:**
```sql
-- Create yearly combination table (new cron wrapper)
SELECT _wh.cron_combine_last_year('foodlogstats', 'landb');

-- Manual yearly combination
SELECT _wh.year_table_create_combined_from_template('foodlogstats', 'landb', 2024);

-- Check data integrity for a year
SELECT _wh.year_table_check_views_by_template_for_year('foodlogstats', 'landb', 2024);
```

**Schema Modifications:**
```sql
-- Add new column to template
SELECT _wh.schema_add_column_to_template('foodlogstats', 'waste_category', 'waste_category TEXT');

-- Add column to all existing MVs
SELECT _wh.schema_add_column_to_all_mvs('foodlogstats', 'landb', 'waste_category', 'TEXT', '''unknown''');

-- Update union views to include new column
SELECT _wh.schema_add_column_to_union_views('foodlogstats', 'landb');
```