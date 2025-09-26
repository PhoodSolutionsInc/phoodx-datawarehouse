```markdown
# Multi-Tenant Data Warehouse Architecture Summary

## 🎯 The Goal
Replace expensive Snowflake/BigQuery setup ($8k/month) with cost-effective PostgreSQL-based data warehouse (~$500-1500/month) while maintaining functionality and performance.

## 🏗️ Architecture Overview

### Schema Organization
```
_wh                    # "Private" operational functions & config
├── tenant_connections # Connection string management
├── connection_audit   # Change tracking
└── functions/         # All warehouse management functions

public/                # User-facing master views
├── foodstats
├── summary views
└── etc

tenant_a/             # Individual tenant data
├── foodstats_2025_09_24 # Daily materialized views
├── foodstats_2025_09_25
└── foodstats     # Tenant's unified view

tenant_b/             # Repeat for each tenant
tenant_c/
```

### Data Flow
```
Source Tenant DBs → Read Replicas → Warehouse DB → Materialized Views → Reports
     (RDS)           (Optional)       (RDS)         (Daily + Hourly)
```

## 📊 Materialized View Strategy

### Daily Pattern
- **Daily MVs**: `tenant_a.foodlogstats_2025_09_24` (historical, immutable)
- **Current Day MV**: Updated as needed via function calls
- **Unified Views**: `tenant_a.foodlogstats` (UNION ALL of daily MVs)
- **Master View**: `public.foodlogstats_all_tenants` (UNION ALL across tenants)

### Usage Examples
```sql
-- Update a single date for a tenant
SELECT _wh.update_foodlogstats('tenant_a', 'reports', '2025-09-24'::date);

-- Update a date range for a tenant (bulk backfill)
SELECT _wh.update_mv_window('tenant_a', 'reports', '2025-09-01'::date, '2025-09-30'::date);

-- Create/update the unified view for a tenant
SELECT _wh.update_tenant_view('tenant_a', 'reports', 'foodlogstats');

-- Force recreate all MVs in a date range
SELECT _wh.update_mv_window('tenant_a', 'reports', '2025-09-01'::date, '2025-09-30'::date, true);
```

## 🔌 Remote Connection Management

### Connection String Table
```sql
CREATE TABLE _wh.tenant_connections (
    tenant_name TEXT PRIMARY KEY,
    host TEXT NOT NULL,
    port INTEGER DEFAULT 5432,
    dbname TEXT NOT NULL,
    username TEXT NOT NULL,
    password TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
```

### Smart Connection Function
```sql
-- Centralized, secure connection management
CREATE FUNCTION _wh.get_tenant_connection_string(tenant_name TEXT)
RETURNS TEXT SECURITY DEFINER;
```

### Data Access Method
Using **dblink** for simplicity over Foreign Data Wrappers:
```sql
CREATE MATERIALIZED VIEW tenant_a.foodlogstats_2025_09_24 AS
SELECT * FROM dblink(
    _wh.get_tenant_connection_string('tenant_a'),
    'SELECT pf.id, pl.name AS store, ... FROM phood_foodlogsum pf ...'
) AS t(
    id INTEGER,
    store TEXT,
    station TEXT,
    validation_status TEXT,
    region TEXT,
    action_taken_id INTEGER,
    action_taken TEXT,
    name TEXT,
    quantity NUMERIC,
    average_weight NUMERIC,
    weight NUMERIC,
    client TEXT,
    food_category TEXT,
    cost_per_lb NUMERIC,
    target_weight NUMERIC,
    price_per_lb NUMERIC,
    action_reason TEXT,
    client_id TEXT,
    logged_time TIMESTAMP,
    flags TEXT
);
```

## 🖥️ Infrastructure Sizing

### Recommended Progression
1. **Start**: `db.r6g.large` (4 vCPU, 32GB) ~$150/month
2. **Scale to**: `db.r6g.xlarge` (8 vCPU, 64GB) ~$300/month  
3. **Eventually**: `db.r6g.2xlarge` (16 vCPU, 128GB) ~$600/month

### Read Replica Strategy
- **Cost**: ~50% of primary instance cost
- **Benefit**: Isolate analytical queries from production OLTP workloads
- **Setup**: One replica per high-volume tenant database

### Storage Tiering
- **Hot data** (last 6 months): Fast gp3 storage on main instance
- **Cold data** (6+ months): Cheaper storage or separate archive instance
- **Estimated savings**: $200-400/month on storage costs

## 🔧 Operational Functions

### Core Management Functions
```sql

-- Core materialized view creation.  Dedicated primary function for each type of warehouse MV.  There needs to be a singular primary CREATE function for each warehouse primary table we want to be able to crate.
_wh.create_foodlogstats_mv(tenant_connection_name, target_schema, base_view_name, target_date, client_name)

-- Single date operations. A wrapper around the primary create function.  These are used in the cron jobs.
_wh.update_foodlogstats(tenant_connection_name, target_schema, target_date, allow_refresh_yesterday, force_recreate)

-- Date range operations (generic function)
_wh.update_mv_window(tenant_connection_name, target_schema, start_date, end_date, force_recreate, update_function_name)

-- Union view management
_wh.update_tenant_view(tenant_connection_name, target_schema, view_basename)

-- Connection management
_wh.set_tenant_connection(name, host, port, dbname, username, password)
_wh.get_tenant_connection_string(tenant_name)

-- Utility functions
_wh.does_mv_exist(target_schema, view_name)
_wh.refresh_mv(target_schema, view_name)
_wh.create_mv_name(mvname, target_date)
```

### Typical Workflow
```sql
-- 1. Set up tenant connection
SELECT _wh.set_tenant_connection('tenant_a', 'db.example.com', 5432, 'production', 'readonly_user', 'password123');

-- 2. Backfill historical data (one month at a time to avoid timeouts)
SELECT _wh.update_mv_window('tenant_a', 'reports', '2025-08-01'::date, '2025-08-31'::date);
SELECT _wh.update_mv_window('tenant_a', 'reports', '2025-09-01'::date, '2025-09-30'::date);

-- 3. Create the unified view
SELECT _wh.update_tenant_view('tenant_a', 'reports', 'foodlogstats');

-- 4. Daily operations: update current day
SELECT _wh.update_foodlogstats('tenant_a', 'reports', CURRENT_DATE);
```

### Disaster Recovery
```sql
-- Complete warehouse rebuild: Just re-run the workflow above
-- No need to backup massive MV data - just recreate from sources!
```

## ⏰ Scheduling with pg_cron

### Why pg_cron over Lambda
- **Zero additional cost** (runs inside RDS)
- **No timeout issues** for long-running MV refreshes
- **Perfect integration** with PL/pgSQL functions
- **Built-in job monitoring** and history

### Job Management
```sql
-- View all scheduled jobs
SELECT jobid, schedule, command, jobname, active FROM cron.job;

-- View job execution history
SELECT j.jobname, r.status, r.start_time, r.end_time, r.return_message
FROM cron.job j
LEFT JOIN cron.job_run_details r ON j.jobid = r.jobid
ORDER BY r.start_time DESC LIMIT 10;

-- Monitor failed jobs
SELECT j.jobname, r.status, r.return_message, r.start_time
FROM cron.job j
JOIN cron.job_run_details r ON j.jobid = r.jobid
WHERE r.status != 'succeeded' 
  AND r.start_time > NOW() - INTERVAL '24 hours';
```

## 📈 Monitoring & Scaling

### Key Metrics to Watch
- **CPU Utilization**: Scale when >80% sustained
- **Memory Usage**: Watch cache hit ratios
- **Connection Count**: vs max_connections limit
- **MV Refresh Times**: Should stay under 1 hour for daily batch

### Performance Queries
```sql
-- Connection usage
SELECT count(*) as current, setting as max 
FROM pg_stat_activity, pg_settings 
WHERE name = 'max_connections';

-- Cache performance
SELECT round((blks_hit::numeric/(blks_hit+blks_read)*100),2) as cache_hit_ratio
FROM pg_stat_database WHERE datname = 'warehouse';
```

## 💰 Cost Comparison

### Current State: $8,000/month
- Snowflake/BigQuery compute & storage
- Data ingestion fees
- Cross-region charges

### Target State: $500-1500/month  
- Warehouse RDS: $300-600/month
- Read replicas: $200-2000/month (depending on tenant count)
- Storage: $200-300/month
- Data transfer: ~$50/month (within VPC)

**Annual Savings: $60,000 - $90,000**

## 🔒 Security & Access

### Schema Permissions
```sql
-- Lock down operational schema
REVOKE ALL ON SCHEMA _wh FROM PUBLIC;
GRANT USAGE ON SCHEMA _wh TO warehouse_admin;

-- Open public views for reporting
GRANT USAGE ON SCHEMA public TO reporting_users;
```

### Connection Security
- Encrypted passwords in connection table
- SSL-only connections to RDS
- Consider AWS Secrets Manager for production

## 🚀 Implementation Path

1. **Phase 1**: Set up warehouse RDS + `_wh` schema functions
2. **Phase 2**: Connect to 2-3 pilot tenants, build daily MVs
3. **Phase 3**: Add hourly refresh + monitoring
4. **Phase 4**: Scale to all tenants
5. **Phase 5**: Add read replicas for production isolation
6. **Phase 6**: Implement storage tiering for cost optimization

## ✅ Key Benefits
- **90%+ cost reduction** vs cloud data warehouses
- **Complete control** over infrastructure and data
- **Simple disaster recovery** (rebuild from sources)
- **Predictable costs** (no per-query or storage surprises)
- **Easy scaling** with RDS instance resize
- **Standard PostgreSQL** tools and expertise

## 🔧 Development & Operations

### Database Access
Standard PostgreSQL tools work perfectly with RDS:
```bash
# Connect with psql
psql -h your-warehouse.rds.amazonaws.com -U warehouse_user -d warehouse

# Backup operational schema only (small, fast)
pg_dump --schema=_wh warehouse_db > wh_infrastructure.sql

# No need to backup tenant schemas - recreate from source data
```

### Schema Backup Strategy
- **DO backup**: `_wh` schema (functions, configuration)  
- **DON'T backup**: `tenant_*` schemas (recreatable from sources)
- **Disaster recovery**: Restore `_wh` schema, then run `_wh.rebuild_entire_warehouse()`

---

This architecture leverages PostgreSQL's strengths while avoiding the complexity and cost of managed cloud data warehouses, perfect for cost-conscious organizations with engineering resources.
```