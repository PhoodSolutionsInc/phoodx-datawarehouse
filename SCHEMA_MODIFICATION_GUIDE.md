# Schema Modification Guide

This guide covers how to safely modify table schemas (add, modify, or remove columns) across the entire multi-tenant data warehouse while maintaining data integrity and minimizing downtime.

## Table of Contents

- [Prerequisites and Required Skills](#prerequisites-and-required-skills)
- [Understanding the Challenge](#understanding-the-challenge)
- [The Architecture Problem](#the-architecture-problem)
- [Our Solution: Controlled Partial Reconstruction](#our-solution-controlled-partial-reconstruction)
- [Adding a Column](#adding-a-column)
- [Future: Modifying Columns](#future-modifying-columns)
- [Future: Removing Columns](#future-removing-columns)

---

## Prerequisites and Required Skills

### Engineer Qualifications

**⚠️ CRITICAL: Schema modifications affect production data across multiple tenants. Only experienced database engineers should perform these operations.**

#### Required Technical Skills

**PostgreSQL Expertise:**
- ✅ **Advanced SQL proficiency** - Complex queries, CTEs, window functions, transactions
- ✅ **PostgreSQL administration** - User management, permissions, schema operations
- ✅ **Database connection management** - Multiple database connections, connection strings
- ✅ **Transaction management** - BEGIN/COMMIT/ROLLBACK, understanding ACID properties
- ✅ **PostgreSQL system catalogs** - Query pg_tables, pg_matviews, information_schema

**Query Analysis and Optimization:**
- ✅ **EXPLAIN plan interpretation** - Understanding costs, row estimates, join strategies
- ✅ **Index management** - Creating, monitoring, and optimizing database indexes
- ✅ **Performance tuning** - Query optimization, identifying bottlenecks
- ✅ **Resource monitoring** - Understanding database load, memory usage, I/O patterns

**Data Warehouse Architecture:**
- ✅ **Materialized view concepts** - Refresh strategies, dependency management
- ✅ **Multi-tenant architecture** - Schema isolation, cross-tenant operations
- ✅ **ETL pipeline understanding** - Data flow from source systems to warehouse
- ✅ **Template-based systems** - Understanding our custom template architecture

#### Required Operational Skills

**Database Tools Proficiency:**
- ✅ **DBeaver or similar GUI** - Table navigation, query execution, data inspection
- ✅ **Command-line psql** - Scripting, batch operations, connection management
- ✅ **Version control** - Git operations for template and function updates

**Production Environment Management:**
- ✅ **Maintenance window planning** - Downtime coordination, stakeholder communication
- ✅ **Backup and recovery** - Understanding backup strategies, rollback procedures
- ✅ **Monitoring and alerting** - Recognizing system health indicators
- ✅ **Documentation practices** - Detailed logging of operations performed

**Problem-Solving and Risk Management:**
- ✅ **Troubleshooting complex issues** - Systematic debugging approach
- ✅ **Risk assessment** - Understanding impact of operations on production systems
- ✅ **Emergency response** - Quick decision-making under pressure
- ✅ **Testing methodology** - Validating changes in development environments

#### Required Business Knowledge

**Warehouse Operations:**
- ✅ **Understanding tenant data patterns** - Peak usage times, data volumes
- ✅ **Business impact assessment** - Which operations can tolerate downtime
- ✅ **Data dependencies** - Which reports/applications rely on specific schemas
- ✅ **Cron job scheduling** - Understanding automated warehouse operations

### Pre-Modification Checklist

**Development Environment Testing:**
- [ ] **Full procedure tested** on development database with realistic data volumes
- [ ] **Timing estimates** recorded for each phase of the operation
- [ ] **Rollback procedures** tested and verified working
- [ ] **Error scenarios** identified and mitigation strategies prepared

**Production Environment Preparation:**
- [ ] **Maintenance window scheduled** and communicated to stakeholders
- [ ] **Database connections verified** - All tenant connections working properly
- [ ] **Current system health confirmed** - No ongoing performance issues
- [ ] **Backup verification** - Recent backups available and tested
- [ ] **Monitoring setup** - Alerts configured for the maintenance window

**Team Coordination:**
- [ ] **Primary operator identified** - Experienced engineer performing operations
- [ ] **Secondary support available** - Backup engineer familiar with procedures
- [ ] **Business stakeholders notified** - Clear communication about downtime
- [ ] **Emergency contacts** - On-call personnel informed of maintenance window

**Technical Preparation:**
- [ ] **Template changes prepared** - New schema definitions ready
- [ ] **Function updates deployed** - Latest schema modification functions available
- [ ] **Scripts validated** - All SQL commands tested and parameterized correctly
- [ ] **Performance monitoring** - Tools ready to monitor operation progress

### Risk Assessment

**High-Risk Indicators (Consider postponing):**
- ❌ **Inexperienced operator** - First time performing schema modifications
- ❌ **Production issues ongoing** - Database performance problems, connectivity issues
- ❌ **Large data volumes** - Current year MVs exceed normal rebuild timeframes
- ❌ **Critical business period** - Month-end processing, financial reporting periods
- ❌ **Untested procedures** - Changes to standard operating procedures

**Medium-Risk Indicators (Proceed with caution):**
- ⚠️ **Complex column changes** - Data type conversions, constraint additions
- ⚠️ **Multiple tenant additions** - New tenants added recently, untested at scale
- ⚠️ **Recent architecture changes** - Function updates, template modifications
- ⚠️ **Extended maintenance window** - Operations expected to take >2 hours

**Low-Risk Indicators (Safe to proceed):**
- ✅ **Simple column addition** - Straightforward TEXT column with default value
- ✅ **Experienced operator** - Multiple successful schema modifications completed
- ✅ **Stable system performance** - No recent issues, normal operation patterns
- ✅ **Tested procedures** - Identical operation performed in development environment

### Emergency Response Plan

**If Major Issues Occur:**
1. **Immediate ROLLBACK** of current transaction if still in progress
2. **Restore public view access** using emergency restoration procedures
3. **Document all error messages** and system state for troubleshooting
4. **Escalate to senior database administrator** if unable to restore service
5. **Communicate status** to business stakeholders immediately

**Recovery Procedures:**
- **Template rollback** - Revert to previous template version if needed
- **Selective tenant restoration** - Restore individual tenants if some succeed/fail
- **Partial service restoration** - Restore read access to historical data while fixing current year
- **Data validation** - Comprehensive checks after any emergency recovery

**Success Criteria:**
- All tenant union views functional
- Public master view accessible
- Query performance within normal ranges
- Data integrity verified across all tenants
- No error messages in warehouse logs

### Skills Development Recommendations

**For Junior Engineers:**
1. **Shadow experienced operators** during schema modifications
2. **Practice on development environments** with realistic data volumes
3. **Study PostgreSQL documentation** on materialized views and schema operations
4. **Learn warehouse architecture** through code review and documentation

**For Senior Engineers:**
1. **Document lessons learned** from each schema modification
2. **Develop testing procedures** for complex modifications
3. **Create training materials** for team knowledge sharing
4. **Stay current** with PostgreSQL best practices and new features

**Continuous Improvement:**
- Regular review of procedures based on operational experience
- Performance optimization of schema modification functions
- Automation opportunities for routine operations
- Documentation updates based on real-world scenarios

---

## Understanding the Challenge

### The Warehouse Architecture

Our data warehouse uses a layered architecture:

```
Source Tables (Tenant DBs)
    ↓
Daily Materialized Views (Current Year)
    ↓
Yearly Tables (Historical Data)
    ↓
Tenant Union Views (Combine Daily MVs + Yearly Tables)
    ↓
Public Master View (Combine All Tenants)
```

### Why Schema Changes Are Complex

**Schema Dependencies:**
- Daily MVs must match the template schema
- Yearly tables must be compatible with daily MVs
- Union views require consistent schemas across all components
- Public views depend on all tenant schemas being compatible

**The Cascade Effect:**
When you add a column to the source table:
1. **New daily MVs** get the new column
2. **Existing daily MVs** don't have the new column
3. **Union views break** due to schema mismatch
4. **Yearly tables** lack the new column
5. **Public view fails** due to incompatible schemas

---

## The Architecture Problem

### What Happens Without Planning

```sql
-- This breaks everything:
SELECT * FROM tenant_schema.foodlogstats_2024_01_01  -- 15 columns
UNION ALL
SELECT * FROM tenant_schema.foodlogstats_2024_01_02  -- 16 columns (ERROR!)
```

### Why We Can't Use Gradual Migration

**Union View Requirements:**
- All daily MVs must have identical schemas
- Yearly tables must be compatible with daily MVs
- Cross-tenant views require consistent column sets

**Performance Implications:**
- COALESCE operations slow down queries
- Index effectiveness reduced
- Query complexity increases

---

## Our Solution: Controlled Partial Reconstruction

### The "Controlled Partial Nuke" Approach

Instead of trying to maintain compatibility across mixed schemas, we take a **planned downtime approach** that rebuilds only what's necessary:

**What We Preserve:**
- ✅ **Yearly tables** (add columns, keep data)
- ✅ **Template definitions** (update schema)
- ✅ **Tenant connections** (no changes needed)

**What We Rebuild:**
- 🔄 **Current year daily MVs** (fast to rebuild)
- 🔄 **Union views** (quick recreation)
- 🔄 **Public master view** (instant)

**What We Drop Temporarily:**
- ❌ **Public access** (planned downtime)
- ❌ **Current year MVs** (rebuilt from source)

### Why This Works

**Minimal Data Loss:**
- Yearly tables retain all historical data
- Only current year MVs are rebuilt (typically < 365 days)

**Predictable Timing:**
- Downtime is proportional to current year data only
- Historical data (90%+ of warehouse) remains intact

**Complete Consistency:**
- All components have identical schemas after completion
- No COALESCE hacks or compatibility layers needed

---

## Adding a Column

### Prerequisites

**Before Starting:**
1. **Plan maintenance window** - expect 30-60 minutes downtime
2. **Identify all tenants** affected by the change
3. **Test on development environment** first
4. **Backup tenant connection data** (optional safety measure)
5. **Coordinate with users** - public views will be unavailable

### Step-by-Step Procedure

#### Phase 1: Preparation and Safety

```sql
-- Connect as whadmin user for all operations
-- Run these commands to understand current state

-- 1. Check current template
SELECT template_name, description
FROM _wh.mv_templates
WHERE template_name = 'foodlogstats';

-- 2. Identify affected tenants
SELECT DISTINCT schemaname
FROM pg_views
WHERE viewname = 'foodlogstats'
AND schemaname NOT IN ('public', '_wh');

-- 3. Count current year MVs (will be rebuilt)
SELECT schemaname, COUNT(*) as current_year_mvs
FROM pg_matviews
WHERE matviewname LIKE 'foodlogstats_2024_%'
GROUP BY schemaname;

-- 4. Count yearly tables (will be preserved + modified)
SELECT schemaname, COUNT(*) as yearly_tables
FROM pg_tables
WHERE tablename ~ '^foodlogstats_\d{4}$'
GROUP BY schemaname;
```

#### Phase 2: Execute Schema Modification

**⚠️ CRITICAL: Run this as a single transaction for rollback safety**

```sql
-- Start planned downtime transaction
BEGIN;

-- Step 1: Drop public access (users lose access here)
SELECT _wh.drop_public_view_by_template('foodlogstats');
-- Result: {"success": true}

-- Step 2: Drop all tenant union views
SELECT _wh.drop_all_tenant_union_views_by_template('foodlogstats');
-- Result: {"success_count": 6, "error_count": 0}

-- Step 3: Add column to all yearly tables
SELECT _wh.add_column_to_yearly_tables_by_template(
    'foodlogstats',           -- template name
    'new_column_name',        -- column name
    'TEXT',                   -- column type (TEXT, INTEGER, BOOLEAN, etc.)
    'DEFAULT_VALUE',          -- default value for existing data
    true                      -- create index on new column
);
-- Result: {"success_count": 12, "error_count": 0, "column_name": "new_column_name"}

-- Step 4: Drop current year MVs (will be rebuilt)
SELECT _wh.drop_current_year_mvs_by_template('foodlogstats', 2024);
-- Result: {"success_count": 289, "error_count": 0, "target_year": 2024}

-- If everything looks good, commit the changes
COMMIT;
-- If anything failed, run: ROLLBACK;
```

#### Phase 3: Update Template

**Edit the template file:** `sql/template-foodlogstats.sql`

```sql
-- Update the template query to include new column
UPDATE _wh.mv_templates
SET query_template = $template$
SELECT
    pf.id,
    pl.name AS store,
    -- ... existing columns ...
    pf.flags,
    pf.new_column_name    -- Add your new column here
FROM phood_foodlogsum pf
-- ... existing joins ...
WHERE pf.logged_time >= '{TARGET_DATE} 00:00:00+00'::TIMESTAMPTZ
  AND pf.logged_time < '{TARGET_DATE} 00:00:00+00'::TIMESTAMPTZ + INTERVAL '1 day'
$template$,
column_definitions = $columns$
-- ... existing column definitions ...
flags TEXT,
new_column_name TEXT     -- Add column definition here
$columns$,
indexes = 'CREATE UNIQUE INDEX idx_{SCHEMA}_{VIEW_NAME}_id ON {SCHEMA}.{VIEW_NAME} (id);
CREATE INDEX idx_{SCHEMA}_{VIEW_NAME}_logged_time ON {SCHEMA}.{VIEW_NAME} (logged_time);
CREATE INDEX idx_{SCHEMA}_{VIEW_NAME}_store ON {SCHEMA}.{VIEW_NAME} (store);
CREATE INDEX idx_{SCHEMA}_{VIEW_NAME}_action_taken_id ON {SCHEMA}.{VIEW_NAME} (action_taken_id);
CREATE INDEX idx_{SCHEMA}_{VIEW_NAME}_new_column ON {SCHEMA}.{VIEW_NAME} (new_column_name);'  -- Add index
WHERE template_name = 'foodlogstats';
```

#### Phase 4: Rebuild Current Year Data

**For each tenant, rebuild current year MVs:**

```sql
-- Example for each tenant (repeat for all)
-- This rebuilds all 2024 daily MVs with the new column schema

SELECT _wh.update_mv_window_by_template(
    'foodlogstats',           -- template name
    'tenant_connection_name', -- connection name (e.g., 'landb-prod')
    'tenant_schema_name',     -- schema name (e.g., 'landb')
    '2024-01-01'::date,       -- start of current year
    _wh.current_date_utc() - INTERVAL '1 day'  -- up to yesterday
);
-- Result: Rebuilds all current year MVs with new schema
```

#### Phase 5: Recreate Views

```sql
-- Recreate tenant union views (for each tenant)
SELECT _wh.update_tenant_union_view_by_template(
    'foodlogstats',           -- template name
    'tenant_schema_name'      -- schema name
);

-- Recreate public master view (restores public access)
SELECT _wh.update_public_view_by_template('foodlogstats');
```

#### Phase 6: Verification

```sql
-- Verify new column exists in yearly tables
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'tenant_schema'
AND table_name = 'foodlogstats_2023'
AND column_name = 'new_column_name';

-- Test union view works
SELECT COUNT(*) FROM tenant_schema.foodlogstats;

-- Test public view works
SELECT COUNT(*) FROM public.foodlogstats;

-- Verify new column has data
SELECT
    new_column_name,
    COUNT(*)
FROM public.foodlogstats
GROUP BY new_column_name;
```

### Complete Example Script

```sql
-- COMPLETE EXAMPLE: Adding 'waste_category' column
-- Run during planned maintenance window

BEGIN;

-- Phase 1: Drop views
SELECT _wh.drop_public_view_by_template('foodlogstats');
SELECT _wh.drop_all_tenant_union_views_by_template('foodlogstats');

-- Phase 2: Modify yearly tables
SELECT _wh.add_column_to_yearly_tables_by_template(
    'foodlogstats',
    'waste_category',
    'TEXT',
    'UNKNOWN',
    true
);

-- Phase 3: Drop current year MVs
SELECT _wh.drop_current_year_mvs_by_template('foodlogstats', 2024);

COMMIT;

-- Phase 4: Update template (manual step - edit template file)
-- Phase 5: Rebuild data (run for each tenant)
-- Phase 6: Recreate views (run for each tenant + public)
```

### Expected Downtime

**Typical Timeline:**
- **Phase 2**: 2-5 minutes (dropping views, modifying yearly tables)
- **Template Update**: 1 minute (manual file edit)
- **Data Rebuild**: 15-45 minutes (depends on current year data volume)
- **View Recreation**: 1-2 minutes

**Total**: 20-60 minutes depending on data volume and number of tenants

### Rollback Plan

**If something goes wrong during Phases 1-3:**
```sql
-- Immediately rollback the transaction
ROLLBACK;

-- Recreate views from existing data
SELECT _wh.update_tenant_union_view_by_template('foodlogstats', 'tenant_schema');
SELECT _wh.update_public_view_by_template('foodlogstats');
```

**If problems occur during data rebuild:**
- Yearly tables and template are already updated
- Continue with individual tenant rebuilds
- Can rebuild tenants one at a time to isolate issues

---

## Future: Modifying Columns

### Approach for Column Type Changes

**Similar process but with additional considerations:**

1. **Data Conversion**: Yearly tables need `ALTER COLUMN` instead of `ADD COLUMN`
2. **Validation**: Ensure existing data can convert to new type
3. **Default Handling**: May need data migration for incompatible types

**Functions Needed:**
- `_wh.modify_column_in_yearly_tables_by_template()`
- Enhanced template update procedures
- Data validation functions

---

## Future: Removing Columns

### Approach for Column Removal

**Process:**
1. Update template to exclude column
2. Rebuild current year MVs (automatically excludes column)
3. Remove column from yearly tables with `ALTER TABLE ... DROP COLUMN`
4. Recreate union views

**Functions Needed:**
- `_wh.remove_column_from_yearly_tables_by_template()`
- Column dependency checking
- Index cleanup procedures

**Considerations:**
- Ensure no critical business logic depends on the column
- Update any hardcoded queries that reference the column
- Consider archiving data before removal

---

## Best Practices

### Before Any Schema Change

1. **Test on development environment** with realistic data volumes
2. **Document the business reason** for the schema change
3. **Plan the maintenance window** during low-usage periods
4. **Communicate with stakeholders** about expected downtime
5. **Have rollback procedures ready** and tested

### During Execution

1. **Run in transactions** where possible for rollback safety
2. **Monitor function results** for error counts
3. **Keep detailed logs** of all operations performed
4. **Verify each phase** before proceeding to the next

### After Completion

1. **Verify data integrity** across all tenants
2. **Test key business queries** to ensure they still work
3. **Monitor performance** for any degradation
4. **Update documentation** to reflect schema changes
5. **Update cron jobs** if any queries have changed

### Emergency Procedures

**If Public View Breaks:**
```sql
-- Emergency restoration of public access
SELECT _wh.update_public_view_by_template('foodlogstats');
```

**If Tenant Union View Breaks:**
```sql
-- Emergency restoration of tenant access
SELECT _wh.update_tenant_union_view_by_template('foodlogstats', 'affected_schema');
```

**If Data Rebuild Fails:**
- Yearly tables are preserved - no data loss
- Can rebuild individual tenants without affecting others
- Can restore views to provide read access to historical data while fixing current year

This controlled approach ensures **maximum data safety** with **predictable downtime** while maintaining the **performance and consistency** of the warehouse architecture.