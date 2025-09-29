# Data Warehouse Architecture

## Overview

This document outlines the technical architecture of the PostgreSQL-based multi-tenant data warehouse system, including data flows, system components, database schemas, and function interactions.

## System Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           PRODUCTION TENANT DATABASES                          │
├─────────────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │   Tenant A   │  │   Tenant B   │  │   Tenant C   │  │   Tenant N   │       │
│  │  (RDS/Cloud) │  │  (RDS/Cloud) │  │  (RDS/Cloud) │  │  (RDS/Cloud) │       │
│  │              │  │              │  │              │  │              │       │
│  │ ┌──────────┐ │  │ ┌──────────┐ │  │ ┌──────────┐ │  │ ┌──────────┐ │       │
│  │ │phood_*   │ │  │ │phood_*   │ │  │ │phood_*   │ │  │ │phood_*   │ │       │
│  │ │tables    │ │  │ │tables    │ │  │ │tables    │ │  │ │tables    │ │       │
│  │ └──────────┘ │  │ └──────────┘ │  │ └──────────┘ │  │ └──────────┘ │       │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘       │
└─────────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ dblink connections
                                      │ (pg_cron scheduled)
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              DATA WAREHOUSE DATABASE                           │
│                                   (postgres)                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────────────┐    ┌──────────────────┐    ┌──────────────────┐          │
│  │    _wh schema   │    │  tenant_a schema │    │  tenant_b schema │          │
│  │   (operational) │    │                  │    │                  │          │
│  │                 │    │                  │    │                  │          │
│  │ ┌─────────────┐ │    │ ┌──────────────┐ │    │ ┌──────────────┐ │          │
│  │ │tenant_      │ │    │ │foodlogstats_ │ │    │ │foodlogstats_ │ │          │
│  │ │connections  │ │    │ │2024_01_01    │ │    │ │2024_01_01    │ │          │
│  │ │             │ │    │ │(daily MV)    │ │    │ │(daily MV)    │ │          │
│  │ └─────────────┘ │    │ └──────────────┘ │    │ └──────────────┘ │          │
│  │                 │    │ ┌──────────────┐ │    │ ┌──────────────┐ │          │
│  │ ┌─────────────┐ │    │ │foodlogstats_ │ │    │ │foodlogstats_ │ │          │
│  │ │mv_templates │ │    │ │2024_01_02    │ │    │ │2024_01_02    │ │          │
│  │ │             │ │    │ │(daily MV)    │ │    │ │(daily MV)    │ │          │
│  │ └─────────────┘ │    │ └──────────────┘ │    │ └──────────────┘ │          │
│  │                 │    │ ┌──────────────┐ │    │ ┌──────────────┐ │          │
│  │ ┌─────────────┐ │    │ │foodlogstats_ │ │    │ │foodlogstats_ │ │          │
│  │ │warehouse    │ │    │ │2023          │ │    │ │2023          │ │          │
│  │ │functions    │ │    │ │(yearly table)│ │    │ │(yearly table)│ │          │
│  │ └─────────────┘ │    │ └──────────────┘ │    │ └──────────────┘ │          │
│  └─────────────────┘    │ ┌──────────────┐ │    │ ┌──────────────┐ │          │
│                         │ │foodlogstats  │ │    │ │foodlogstats  │ │          │
│                         │ │(union view)  │ │    │ │(union view)  │ │          │
│                         │ └──────────────┘ │    │ └──────────────┘ │          │
│                         └──────────────────┘    └──────────────────┘          │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                         public schema                                   │   │
│  │                                                                         │   │
│  │ ┌─────────────────────────────────────────────────────────────────────┐ │   │
│  │ │                      foodlogstats                                   │ │   │
│  │ │                   (master union view)                               │ │   │
│  │ │                                                                     │ │   │
│  │ │  SELECT *, 'tenant_a' AS schema_name FROM tenant_a.foodlogstats     │ │   │
│  │ │  UNION ALL                                                          │ │   │
│  │ │  SELECT *, 'tenant_b' AS schema_name FROM tenant_b.foodlogstats     │ │   │
│  │ │  UNION ALL                                                          │ │   │
│  │ │  SELECT *, 'tenant_c' AS schema_name FROM tenant_c.foodlogstats     │ │   │
│  │ └─────────────────────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ SQL queries
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                               BI/REPORTING TOOLS                               │
├─────────────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │   Tableau    │  │  PowerBI     │  │  Looker      │  │  Custom Apps │       │
│  │              │  │              │  │              │  │              │       │
│  │  (phood_ro   │  │  (phood_ro   │  │  (phood_ro   │  │  (phood_ro   │       │
│  │   user)      │  │   user)      │  │   user)      │  │   user)      │       │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘       │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Database Schema Architecture

### User Roles and Permissions

```sql
-- Master admin (infrastructure only)
postgres: SUPERUSER, used for setup and emergency access

-- Warehouse operations admin
whadmin: NOSUPERUSER, CREATEDB, CREATEROLE
- Manages warehouse functions and data operations
- Owns all MVs, tables, and warehouse objects
- Executes all data transformation functions

-- Read-only BI user
phood_ro: NOSUPERUSER, NOCREATEDB, NOCREATEROLE
- SELECT access to all tenant schemas and public views
- Used by BI tools and reporting applications
- Cannot see operational schemas or connection details
```

### Schema Organization

#### `_wh` Schema (Operational/Private)
```sql
-- Tenant connection management
_wh.tenant_connections
├── tenant_name (PK)
├── host, port, dbname
├── username, password
└── created_at, updated_at

-- Template definitions for MV types
_wh.mv_templates
├── template_name (PK)
├── description
├── query_template (with {TARGET_DATE} placeholder)
├── column_definitions
└── indexes

-- All warehouse management functions
_wh.create_mv_from_template()
_wh.update_mv_by_template()
_wh.update_mv_window_by_template()
_wh.update_tenant_union_view_by_template()
_wh.update_public_view_by_template()
_wh.create_combined_table_from_template_by_year()
_wh.current_date_utc()
-- ... utility functions
```

#### Tenant Schemas (`tenant_a`, `tenant_b`, etc.)
```sql
    -- Add daily materialized views (pattern: template_name_YYYY_MM_DD)
-- Daily materialized views
{tenant}.{template}_{YYYY_MM_DD}
├── foodlogstats_2024_01_01  -- Daily MV
├── foodlogstats_2024_01_02  -- Daily MV
├── ...
├── foodlogstats_2024_12_31  -- Daily MV

-- Yearly tables (converted from daily MVs)
{tenant}.{template}_{YYYY}
├── foodlogstats_2023        -- Yearly table
├── foodlogstats_2022        -- Yearly table
└── ...

-- Tenant union view (combines daily MVs + yearly tables)
{tenant}.{template}
└── foodlogstats             -- UNION ALL view
    ├── SELECT * FROM foodlogstats_2023
    ├── UNION ALL SELECT * FROM foodlogstats_2024_01_01
    ├── UNION ALL SELECT * FROM foodlogstats_2024_01_02
    └── ...
```

#### `public` Schema (BI/Reporting)
```sql
-- Master union views (cross-tenant)
public.{template}
└── foodlogstats             -- Master view with schema_name column
    ├── SELECT *, 'tenant_a' AS schema_name FROM tenant_a.foodlogstats
    ├── UNION ALL SELECT *, 'tenant_b' AS schema_name FROM tenant_b.foodlogstats
    └── ...
```

## Data Flow Architecture

### 1. Data Ingestion Flow

```
Tenant Production DB → dblink → Daily MV Creation → Tenant Union View → Public Master View
```

**Function Chain:**
```sql
-- Scheduled via pg_cron
_wh.update_mv_by_template('foodlogstats', 'tenant_a', _wh.current_date_utc())
├── Validates template exists
├── Creates/refreshes daily MV using template query
├── Handles previous day refresh logic
└── Returns success/failure status

-- Manual union view updates (or triggered by MV changes)
_wh.update_tenant_union_view_by_template('foodlogstats', 'tenant_a')
├── Finds all daily MVs + yearly tables for template
├── Builds UNION ALL query
├── Creates/replaces tenant union view
└── Sets proper ownership and permissions

-- Public view updates (less frequent)
_wh.update_public_view_by_template('foodlogstats')
├── Finds all tenant schemas with template union views
├── Builds UNION ALL with schema_name injection
├── Creates/replaces public master view
└── Sets proper ownership and permissions
```

### 2. Template-Based MV Creation

```
Template Definition → dblink Query → Materialized View → Union View Integration
```

**Template Processing:**
```sql
-- Template stored in _wh.mv_templates
query_template: "SELECT ... WHERE (logged_time AT TIME ZONE 'UTC')::DATE = '{TARGET_DATE}'::DATE"

-- Function processes template
_wh.create_mv_from_template('foodlogstats', 'tenant_a', 'tenant_a', '2024-01-01')
├── Retrieves template from _wh.mv_templates
├── Replaces {TARGET_DATE} with actual date
├── Builds dblink query with tenant connection
├── Creates materialized view with template column definitions
├── Creates indexes using template index definitions
└── Sets ownership and permissions
```

### 3. Yearly Combination Flow

```
Daily MVs → Validation → Yearly Table Creation → Data Migration → Union View Update
```

**Yearly Combination Process:**
```sql
_wh.create_combined_table_from_template_by_year('foodlogstats', 'tenant_a', 2024)
├── BEGIN TRANSACTION
├── Validates template exists, yearly table doesn't exist
├── Temporarily modifies union view (excludes target year MVs)
├── Creates yearly table using template definitions
├── FOR EACH daily MV matching year pattern:
│   ├── INSERT INTO yearly_table SELECT * FROM daily_mv
│   └── DROP MATERIALIZED VIEW daily_mv
├── Recreates union view (includes new yearly table)
├── Validates record counts match expectations
├── COMMIT TRANSACTION
└── Returns detailed JSON results
```

### 4. Scheduled Operations (pg_cron)

```
pg_cron Scheduler → Function Execution → Data Refresh → Union View Updates
```

**Cron Job Types:**
```sql
-- Daily/Hourly MV Updates (staggered)
'10 */4 * * *' → _wh.update_mv_by_template('foodlogstats', 'tenant_a', _wh.current_date_utc())
'25 */4 * * *' → _wh.update_mv_by_template('foodlogstats', 'tenant_b', _wh.current_date_utc())
'40 */4 * * *' → _wh.update_mv_by_template('foodlogstats', 'tenant_c', _wh.current_date_utc())

-- Annual Yearly Combinations (February)
'0 2 15 2 *' → _wh.create_combined_table_from_template_by_year('foodlogstats', 'tenant_a',
                EXTRACT(YEAR FROM _wh.current_date_utc() - INTERVAL '1 year')::integer)
```

## Security Architecture

### Access Control Matrix

| User Role | `_wh` Schema | Tenant Schemas | `public` Schema | Cron Jobs |
|-----------|--------------|----------------|-----------------|-----------|
| `postgres` | FULL | FULL | FULL | FULL |
| `whadmin` | FULL | OWNER | OWNER | CREATE/MANAGE |
| `phood_ro` | NONE | SELECT | SELECT | NONE |

### Data Security Principles

1. **Operational Isolation**: BI users cannot access connection strings or templates
2. **Cross-Tenant Isolation**: Managed at schema level, not row level
3. **Function Security**: All warehouse functions use SECURITY DEFINER
4. **Connection Security**: Encrypted storage, SSL connections required
5. **Audit Trail**: All operations logged with structured context

## Performance Architecture

### Storage Optimization Strategy

```
Daily MVs (Current Year) → Yearly Tables (Previous Years) → Archive/Cold Storage
     │                           │                              │
   Hot Storage                Warm Storage                 Cold Storage
   (Fast SSD)                 (Standard SSD)               (Slower/Cheaper)
```

### Query Performance Patterns

```sql
-- Optimized for recent data queries (daily MVs)
SELECT * FROM public.foodlogstats
WHERE logged_time >= CURRENT_DATE - INTERVAL '30 days'
└── Hits daily MVs (fast, indexed)

-- Optimized for historical analysis (yearly tables)
SELECT * FROM public.foodlogstats
WHERE EXTRACT(YEAR FROM logged_time) = 2023
└── Hits yearly table (single large table, well-indexed)

-- Cross-year analysis (both daily MVs + yearly tables)
SELECT * FROM public.foodlogstats
WHERE logged_time BETWEEN '2023-11-01' AND '2024-02-01'
└── Hits yearly table + daily MVs (union view optimization)
```

### Index Strategy

```sql
-- Daily MVs: Optimized for date-range and operational queries
CREATE UNIQUE INDEX idx_{schema}_{view_name}_id ON {schema}.{view_name} (id);
CREATE INDEX idx_{schema}_{view_name}_logged_time ON {schema}.{view_name} (logged_time);
CREATE INDEX idx_{schema}_{view_name}_store ON {schema}.{view_name} (store);

-- Yearly Tables: Same index structure maintained
-- Union Views: No indexes (dynamic view composition)
```

## Disaster Recovery Architecture

### Backup Strategy

```
Operational Schemas → Full Backup (Small, Critical)
     │
     └── _wh.tenant_connections
     └── _wh.mv_templates
     └── _wh functions

Tenant Data → Recreatable from Source (No Backup Needed)
     │
     └── Daily MVs: Regenerate from templates
     └── Yearly Tables: Regenerate from daily MVs
     └── Union Views: Recreate from functions
```

### Recovery Process

```sql
-- 1. Restore operational infrastructure
\i sql/create.sql      -- Users, extensions, basic setup
\i sql/functions.sql   -- All warehouse functions

-- 2. Restore configurations
\i backup/_wh_schema.sql  -- Templates and connections

-- 3. Rebuild data warehouse (automated)
_wh.rebuild_entire_warehouse()  -- Recreates all MVs from sources
```

## Monitoring Architecture

### Key Metrics

```sql
-- System Health
SELECT COUNT(*) as total_connections FROM pg_stat_activity;
SELECT * FROM pg_stat_database WHERE datname = 'postgres';

-- Job Monitoring
SELECT jobname, status, start_time, end_time
FROM cron.job_run_details
WHERE start_time > NOW() - INTERVAL '24 hours';

-- Data Freshness
SELECT schemaname, matviewname,
       EXTRACT(EPOCH FROM (NOW() - last_refresh))::INTEGER / 3600 as hours_old
FROM pg_stat_user_tables WHERE relname LIKE '%foodlogstats%';

-- Storage Usage
SELECT schemaname, tablename, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename))
FROM pg_tables WHERE schemaname LIKE 'tenant_%';
```

### Error Handling

```sql
-- Function-level error handling: All functions return boolean/jsonb results
-- Transaction-level safety: Yearly combinations use full transaction protection
-- Logging: Structured JSON context in all warehouse operations
-- Alerting: pg_cron job failure monitoring via cron.job_run_details
```

## Scaling Architecture

### Horizontal Scaling (More Tenants)

```
Add Tenant → Create Schema → Load Templates → Start Cron Jobs
     │              │              │                │
  _wh.add_tenant  CREATE SCHEMA  Template Load   Cron Schedule
   (future)         Manual        \i template    _wh.setup_cron
```

### Vertical Scaling (More Data)

```
Storage Tiering → Read Replicas → Compute Scaling → Partitioning
      │                │               │               │
  Yearly Tables    Analytical      RDS Instance    Future: Monthly
   (Archive)       Workloads        Resize         Partitioning
```

### Template Scaling (More Data Types)

```
New Template → Template File → Load Template → Deploy to Tenants
     │              │              │               │
  Define Schema   Create SQL     INSERT INTO     Schedule Cron
  Column Defs     Template       mv_templates    Jobs per Tenant
```

---

This architecture provides a scalable, maintainable, and cost-effective data warehouse solution that leverages PostgreSQL's strengths while avoiding the complexity and cost of managed cloud data warehouses.