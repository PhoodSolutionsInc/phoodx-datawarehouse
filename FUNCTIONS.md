# Data Warehouse Functions Reference

This document provides detailed reference for all `_wh` schema functions in the template-based data warehouse.

## Table of Contents

- [Template-Based Core Functions](#template-based-core-functions)
  - [_wh.create_mv_from_template](#_whcreate_mv_from_template)
  - [_wh.update_mv_by_template](#_whupdate_mv_by_template)
  - [_wh.update_mv_window_by_template](#_whupdate_mv_window_by_template)
- [Union View Functions](#union-view-functions)
  - [_wh.update_tenant_union_view_by_template](#_whupdate_tenant_union_view_by_template)
  - [_wh.update_public_view_by_template](#_whupdate_public_view_by_template)
- [Connection Management](#connection-management)
  - [_wh.get_tenant_connection_string](#_whget_tenant_connection_string)
- [Utility Functions](#utility-functions)
  - [_wh.create_mv_name](#_whcreate_mv_name)
  - [_wh.does_mv_exist](#_whdoes_mv_exist)
  - [_wh.refresh_mv](#_whrefresh_mv)
- [Logging Functions](#logging-functions)
  - [_wh.log_info, _wh.log_error, _wh.log_debug](#_whlog_helpers)

---

## Template-Based Core Functions

### _wh.create_mv_from_template

Creates a materialized view from a template definition.

**Signature:**
```sql
_wh.create_mv_from_template(
    template_name text,
    tenant_connection_name text,
    target_schema text,
    target_date date
) RETURNS boolean
```

**Parameters:**
- `template_name`: Name of the template in `_wh.mv_templates`
- `tenant_connection_name`: Connection identifier from `_wh.tenant_connections`
- `target_schema`: Schema to create the MV in
- `target_date`: Date for the MV (substituted as `{TARGET_DATE}`)

**Returns:** `TRUE` on success, `FALSE` on failure

**Example:**
```sql
SELECT _wh.create_mv_from_template('foodlogstats', 'tenant_a', 'reports', '2025-09-24'::date);
```

### _wh.update_mv_by_template

Updates or creates a materialized view using a template. If the MV exists, it refreshes; if not, it creates.

**Signature:**
```sql
_wh.update_mv_by_template(
    template_name text,
    tenant_connection_name text,
    target_schema text,
    target_date date,
    allow_refresh_yesterday boolean DEFAULT true
) RETURNS boolean
```

**Parameters:**
- `template_name`: Name of the template in `_wh.mv_templates`
- `tenant_connection_name`: Connection identifier
- `target_schema`: Schema to update the MV in
- `target_date`: Date for the MV
- `allow_refresh_yesterday`: If true and target_date is today, also refresh yesterday's MV

**Returns:** `TRUE` on success, `FALSE` on failure

**Example:**
```sql
SELECT _wh.update_mv_by_template('foodlogstats', 'tenant_a', 'reports', CURRENT_DATE);
```

### _wh.update_mv_window_by_template

Bulk updates materialized views for a date range using templates.

**Signature:**
```sql
_wh.update_mv_window_by_template(
    template_name text,
    tenant_connection_name text,
    target_schema text,
    start_date date,
    end_date date
) RETURNS jsonb
```

**Parameters:**
- `template_name`: Name of the template
- `tenant_connection_name`: Connection identifier
- `target_schema`: Schema to update MVs in
- `start_date`: Start date (inclusive)
- `end_date`: End date (inclusive)

**Returns:** JSON object with summary stats

**Example:**
```sql
SELECT _wh.update_mv_window_by_template('foodlogstats', 'tenant_a', 'reports', '2025-09-01'::date, '2025-09-30'::date);
```

---

## Union View Functions

### _wh.update_tenant_union_view_by_template

Creates or updates a tenant's union view that combines all daily MVs for a template.

**Signature:**
```sql
_wh.update_tenant_union_view_by_template(
    template_name text,
    tenant_connection_name text,
    target_schema text
) RETURNS boolean
```

**Creates:** `target_schema.template_name` view

**Example:**
```sql
SELECT _wh.update_tenant_union_view_by_template('foodlogstats', 'tenant_a', 'reports');
-- Creates: reports.foodlogstats
```

### _wh.update_public_view_by_template

Creates or updates the public master view that combines all tenant union views with schema context.

**Signature:**
```sql
_wh.update_public_view_by_template(
    template_name text
) RETURNS boolean
```

**Creates:** `public.template_name` view with `schema_name` column

**Example:**
```sql
SELECT _wh.update_public_view_by_template('foodlogstats');
-- Creates: public.foodlogstats (with schema_name column)
```

**Purpose**: Creates a single materialized view for food log statistics data for a specific date.

**Signature**:
```sql
_wh.create_foodlogstats_mv(
    tenant_connection_name text,
    target_schema text,
    base_view_name text,
    target_date date,
    client_name text DEFAULT NULL
) RETURNS boolean
```

**Parameters**:
- `tenant_connection_name`: Name of the tenant connection (from `_wh.tenant_connections`)
- `target_schema`: PostgreSQL schema where the MV will be created
- `base_view_name`: Base name for the view (usually 'foodlogstats')
- `target_date`: Date for which to extract data
- `client_name`: Optional client identifier (defaults to target_schema if NULL)

**Returns**: `TRUE` if successful, `FALSE` if failed

**What it does**:
1. Generates MV name using date pattern (e.g., `foodlogstats_2024_01_15`)
2. Connects to tenant database via dblink
3. Extracts food log data for the specified date
4. Creates materialized view with proper column definitions
5. Creates optimized indexes (id, logged_time, client, store, action_taken_id)

**Examples**:
```sql
-- Create MV for today's data
SELECT _wh.create_foodlogstats_mv(
    'tenant_a_conn',
    'tenant_a',
    'foodlogstats',
    CURRENT_DATE,
    'tenant_a'
);

-- Create MV for specific date
SELECT _wh.create_foodlogstats_mv(
    'landb_stage',
    'landb',
    'foodlogstats',
    '2024-01-15'::date,
    'Land Bank'
);
```

**Source Data**: Extracts from `phood_foodlogsum` table with joins to related tables (locations, regions, actions, inventory, etc.)

---

## Wrapper Functions

### _wh.update_foodlogstats

**Purpose**: High-level wrapper for creating/updating foodlogstats materialized views with intelligent logic.

**Signature**:
```sql
_wh.update_foodlogstats(
    tenant_connection_name text,
    target_schema text,
    target_date date,
    allow_refresh_yesterday boolean DEFAULT true
) RETURNS jsonb
```

**Parameters**:
- `tenant_connection_name`: Name of the tenant connection
- `target_schema`: Schema where MV will be created
- `target_date`: Date for the materialized view
- `allow_refresh_yesterday`: Whether to refresh previous day's MV

**Returns**: JSONB object with operation details and results

**What it does**:
1. Calls `_wh.update_mv_core` with foodlogstats-specific parameters
2. Handles the complete workflow for a single date
3. Provides detailed logging and error handling

**Examples**:
```sql
-- Update today's data with yesterday refresh
SELECT _wh.update_foodlogstats(
    'tenant_a_conn',
    'tenant_a',
    CURRENT_DATE
);

-- Update specific date without yesterday refresh
SELECT _wh.update_foodlogstats(
    'tenant_a_conn',
    'tenant_a',
    '2024-01-15'::date,
    false
);
```

**Return Example**:
```json
{
  "tenant": "tenant_a_conn",
  "schema": "tenant_a",
  "target_date": "2024-01-15",
  "view_name": "foodlogstats_2024_01_15",
  "operations": [
    {
      "operation": "refresh_yesterday",
      "view": "foodlogstats_2024_01_14",
      "success": true,
      "timestamp": "2024-01-15T10:30:00"
    },
    {
      "operation": "create_new",
      "view": "foodlogstats_2024_01_15",
      "success": true,
      "timestamp": "2024-01-15T10:31:30"
    }
  ],
  "success": true,
  "duration_seconds": 90.5
}
```

### _wh.update_mv_core

**Purpose**: Generic core function for creating/updating materialized views. Used by all wrapper functions.

**Signature**:
```sql
_wh.update_mv_core(
    tenant_connection_name text,
    target_schema text,
    target_date date,
    view_basename text,
    create_function_name text,
    allow_refresh_yesterday boolean DEFAULT true
) RETURNS jsonb
```

**Parameters**:
- `tenant_connection_name`: Name of the tenant connection
- `target_schema`: Target schema for the MV
- `target_date`: Date for the MV
- `view_basename`: Base name (e.g., 'foodlogstats', 'inventory_stats')
- `create_function_name`: Name of the specific create function to call
- `allow_refresh_yesterday`: Whether to refresh yesterday's MV first

**What it does**:
1. **First**: Refreshes yesterday's MV (if exists and allowed)
2. **Second**: Creates new MV or refreshes existing one for target date
3. Provides comprehensive logging and error handling
4. Returns detailed operation results

**Examples**:
```sql
-- Used internally by wrapper functions
SELECT _wh.update_mv_core(
    'tenant_a_conn',
    'tenant_a',
    CURRENT_DATE,
    'foodlogstats',
    '_wh.create_foodlogstats_mv',
    true
);
```

**Note**: This is typically not called directly - use the wrapper functions instead.

---

## Batch Operations

### _wh.update_mv_window

**Purpose**: Updates materialized views for a range of dates in sequence.

**Signature**:
```sql
_wh.update_mv_window(
    tenant_connection_name text,
    target_schema text,
    start_date date,
    end_date date,
    update_function_name text
) RETURNS jsonb
```

**Parameters**:
- `tenant_connection_name`: Name of the tenant connection
- `target_schema`: Target schema for MVs
- `start_date`: First date to process (inclusive)
- `end_date`: Last date to process (inclusive)
- `update_function_name`: Function to call for each date (e.g., '_wh.update_foodlogstats')

**Returns**: JSONB with summary statistics and detailed results for each date

**What it does**:
1. Validates date range (start_date <= end_date)
2. Loops through each date in the range
3. Calls the specified update function with `allow_refresh_yesterday=false`
4. Tracks success/failure for each date
5. Returns comprehensive summary

**Examples**:
```sql
-- Backfill foodlogstats for January 2024
SELECT _wh.update_mv_window(
    'tenant_a_conn',
    'tenant_a',
    '2024-01-01'::date,
    '2024-01-31'::date,
    '_wh.update_foodlogstats'
);

-- Backfill for a week
SELECT _wh.update_mv_window(
    'landb_stage',
    'landb',
    '2024-01-01'::date,
    '2024-01-07'::date,
    '_wh.update_foodlogstats'
);
```

**Return Example**:
```json
{
  "start_date": "2024-01-01",
  "end_date": "2024-01-31",
  "total_dates": 31,
  "success_count": 29,
  "error_count": 2,
  "duration_seconds": 1847.3,
  "date_results": [
    {
      "target_date": "2024-01-01",
      "success": true,
      "view_name": "foodlogstats_2024_01_01"
    },
    {
      "target_date": "2024-01-02",
      "success": false,
      "error": "connection timeout"
    }
  ]
}
```

### _wh.update_tenant_union_view

**Purpose**: Creates or updates a union view that combines all materialized views for a tenant.

**Signature**:
```sql
_wh.update_tenant_union_view(
    tenant_connection_name text,
    target_schema text,
    view_basename text
) RETURNS jsonb
```

**Parameters**:
- `tenant_connection_name`: Tenant connection name (used for logging)
- `target_schema`: Schema containing the materialized views
- `view_basename`: Base name to search for (e.g., 'foodlogstats')

**Returns**: JSONB with operation results

**What it does**:
1. Finds all materialized views matching pattern `{view_basename}_%`
2. Sorts them by name for consistent ordering
3. Builds a `UNION ALL` query combining all MVs
4. Creates or replaces the union view named `{target_schema}.{view_basename}`

**Examples**:
```sql
-- Create union view for all foodlogstats MVs
SELECT _wh.update_tenant_union_view(
    'tenant_a_conn',
    'tenant_a',
    'foodlogstats'
);

-- Result: Creates view tenant_a.foodlogstats that unions:
-- tenant_a.foodlogstats_2024_01_01
-- tenant_a.foodlogstats_2024_01_02
-- tenant_a.foodlogstats_2024_01_03
-- ... etc
```

**Generated SQL Example**:
```sql
CREATE OR REPLACE VIEW tenant_a.foodlogstats AS
SELECT * FROM tenant_a.foodlogstats_2024_01_01
UNION ALL
SELECT * FROM tenant_a.foodlogstats_2024_01_02
UNION ALL
SELECT * FROM tenant_a.foodlogstats_2024_01_03;
```

---

## Connection Management

### _wh.set_tenant_connection

**Purpose**: Adds or updates tenant database connection information.

**Signature**:
```sql
_wh.set_tenant_connection(
    name text,
    host text,
    port integer,
    dbname text,
    username text,
    password text
) RETURNS void
```

**Parameters**:
- `name`: Unique identifier for this connection
- `host`: Database hostname or IP address
- `port`: Database port (typically 5432)
- `dbname`: Database name
- `username`: Database username
- `password`: Database password (stored in plain text)

**What it does**:
1. Inserts new connection or updates existing one (UPSERT)
2. Automatically sets `updated_at` timestamp
3. Uses the `name` as the primary key

**Examples**:
```sql
-- Add new tenant connection
SELECT _wh.set_tenant_connection(
    'tenant_a_prod',
    'tenant-a.cluster-xyz.us-west-2.rds.amazonaws.com',
    5432,
    'production',
    'readonly_user',
    'secure_password_123'
);

-- Update existing connection (same name)
SELECT _wh.set_tenant_connection(
    'tenant_a_prod',
    'tenant-a-new.cluster-abc.us-west-2.rds.amazonaws.com',
    5432,
    'production',
    'readonly_user',
    'new_password_456'
);
```

**Security Note**: Passwords are stored in plain text. Consider using AWS Secrets Manager or similar for production.

### _wh.get_tenant_connection_string

**Purpose**: Retrieves a formatted connection string for dblink operations.

**Signature**:
```sql
_wh.get_tenant_connection_string(tenant_name text) RETURNS text
```

**Parameters**:
- `tenant_name`: Name of the tenant connection to retrieve

**Returns**: Formatted connection string for use with dblink

**Security**: Function is marked `SECURITY DEFINER` to allow controlled access to connection details

**What it does**:
1. Looks up connection details from `_wh.tenant_connections`
2. Formats as dblink-compatible connection string
3. Raises exception if tenant not found

**Examples**:
```sql
-- Get connection string
SELECT _wh.get_tenant_connection_string('tenant_a_prod');
-- Returns: 'host=tenant-a.rds.amazonaws.com port=5432 dbname=production user=readonly_user password=secret123'

-- Use in dblink query
SELECT * FROM dblink(
    _wh.get_tenant_connection_string('tenant_a_prod'),
    'SELECT COUNT(*) FROM orders'
) AS t(order_count bigint);
```

---

## Utility Functions

### _wh.create_mv_name

**Purpose**: Generates standardized materialized view names with date suffixes.

**Signature**:
```sql
_wh.create_mv_name(mvname text, target_date date) RETURNS text
```

**Parameters**:
- `mvname`: Base name for the materialized view
- `target_date`: Date to append to the name

**Returns**: Formatted view name with date suffix

**Format**: `{mvname}_{YYYY_MM_DD}`

**Examples**:
```sql
SELECT _wh.create_mv_name('foodlogstats', '2024-01-15'::date);
-- Returns: 'foodlogstats_2024_01_15'

SELECT _wh.create_mv_name('inventory_stats', CURRENT_DATE);
-- Returns: 'inventory_stats_2024_01_15' (if today is 2024-01-15)
```

### _wh.does_mv_exist

**Purpose**: Checks if a materialized view exists in a specific schema.

**Signature**:
```sql
_wh.does_mv_exist(target_schema text, view_name text) RETURNS boolean
```

**Parameters**:
- `target_schema`: Schema name to check
- `view_name`: Materialized view name to check

**Returns**: `TRUE` if the materialized view exists, `FALSE` otherwise

**Examples**:
```sql
-- Check if specific MV exists
SELECT _wh.does_mv_exist('tenant_a', 'foodlogstats_2024_01_15');
-- Returns: true or false

-- Use in conditional logic
DO $$
BEGIN
    IF _wh.does_mv_exist('tenant_a', 'foodlogstats_2024_01_15') THEN
        RAISE NOTICE 'MV exists, will refresh';
    ELSE
        RAISE NOTICE 'MV does not exist, will create';
    END IF;
END $$;
```

### _wh.refresh_mv

**Purpose**: Refreshes an existing materialized view using CONCURRENT refresh.

**Signature**:
```sql
_wh.refresh_mv(target_schema text, view_name text) RETURNS boolean
```

**Parameters**:
- `target_schema`: Schema containing the materialized view
- `view_name`: Name of the materialized view to refresh

**Returns**: `TRUE` if successful, `FALSE` if failed

**What it does**:
1. Executes `REFRESH MATERIALIZED VIEW CONCURRENTLY`
2. Uses concurrent refresh to avoid locking the view during refresh
3. Provides logging for success/failure

**Examples**:
```sql
-- Refresh a specific MV
SELECT _wh.refresh_mv('tenant_a', 'foodlogstats_2024_01_15');

-- Check result
DO $$
DECLARE
    refresh_result BOOLEAN;
BEGIN
    refresh_result := _wh.refresh_mv('tenant_a', 'foodlogstats_2024_01_15');
    IF refresh_result THEN
        RAISE NOTICE 'Refresh successful';
    ELSE
        RAISE NOTICE 'Refresh failed';
    END IF;
END $$;
```

**Note**: Requires the materialized view to have a unique index for concurrent refresh.

---

## Logging Functions

### _wh.log

**Purpose**: Central logging function with multiple log levels.

**Signature**:
```sql
_wh.log(
    level text,
    message text,
    context jsonb DEFAULT NULL
) RETURNS void
```

**Parameters**:
- `level`: Log level ('DEBUG', 'INFO', 'WARN', 'ERROR')
- `message`: Log message text
- `context`: Optional JSONB context data

**Log Levels**:
- `DEBUG`: Detailed debugging information
- `INFO`: General information (uses NOTICE)
- `WARN`: Warning messages
- `ERROR`: Error messages (uses WARNING for DataDog compatibility)

**Examples**:
```sql
-- Simple info message
PERFORM _wh.log('INFO', 'Starting data warehouse refresh');

-- Error with context
PERFORM _wh.log('ERROR', 'Failed to connect to tenant database',
    jsonb_build_object('tenant', 'tenant_a', 'error_code', 'CONN_TIMEOUT'));

-- Debug message
PERFORM _wh.log('DEBUG', 'Processing date range',
    jsonb_build_object('start_date', '2024-01-01', 'end_date', '2024-01-31'));
```

**Output Format**: `[WH][timestamp] message`

### _wh.log_info, _wh.log_warn, _wh.log_error, _wh.log_debug

**Purpose**: Convenience wrapper functions for specific log levels.

**Signatures**:
```sql
_wh.log_info(message text, context jsonb DEFAULT NULL) RETURNS void
_wh.log_warn(message text, context jsonb DEFAULT NULL) RETURNS void
_wh.log_error(message text, context jsonb DEFAULT NULL) RETURNS void
_wh.log_debug(message text, context jsonb DEFAULT NULL) RETURNS void
```

**Examples**:
```sql
-- Equivalent ways to log
PERFORM _wh.log('INFO', 'Operation completed');
PERFORM _wh.log_info('Operation completed');

-- With context
PERFORM _wh.log_error('Database connection failed',
    jsonb_build_object('tenant', 'tenant_a'));
```

---

## Function Dependencies

### Function Call Hierarchy

```
_wh.update_foodlogstats
└── _wh.update_mv_core
    ├── _wh.create_mv_name
    ├── _wh.does_mv_exist
    ├── _wh.refresh_mv
    ├── _wh.log_info, _wh.log_error, _wh.log_warn
    └── _wh.create_foodlogstats_mv
        ├── _wh.get_tenant_connection_string
        ├── _wh.create_mv_name
        └── _wh.log_info, _wh.log_error, _wh.log_debug

_wh.update_mv_window
└── [dynamic call to update function]
    └── (typically _wh.update_foodlogstats)

_wh.update_tenant_view
└── _wh.log_info, _wh.log_warn, _wh.log_error
```

### Typical Usage Patterns

**Daily Operations**:
```sql
-- Update current day
SELECT _wh.update_foodlogstats('tenant_conn', 'tenant_schema', CURRENT_DATE);

-- Update union view
SELECT _wh.update_tenant_view('tenant_conn', 'tenant_schema', 'foodlogstats');
```

**Historical Backfill**:
```sql
-- Backfill date range
SELECT _wh.update_mv_window(
    'tenant_conn', 'tenant_schema',
    '2024-01-01'::date, '2024-01-31'::date,
    '_wh.update_foodlogstats'
);

-- Update union view
SELECT _wh.update_tenant_view('tenant_conn', 'tenant_schema', 'foodlogstats');
```

**Setup New Tenant**:
```sql
-- Add connection
SELECT _wh.set_tenant_connection('new_tenant', 'host.com', 5432, 'db', 'user', 'pass');

-- Create schema
CREATE SCHEMA new_tenant;

-- Backfill data
SELECT _wh.update_mv_window('new_tenant', 'new_tenant', start_date, end_date, '_wh.update_foodlogstats');

-- Create union view
SELECT _wh.update_tenant_view('new_tenant', 'new_tenant', 'foodlogstats');
```