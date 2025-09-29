-- Data Warehouse Initial Setup Script
--
-- Prerequisites:
-- 1. Connect to PostgreSQL as the master 'postgres' user
-- 2. Connect to the 'postgres' database (default database)
-- 3. IMPORTANT: Update the password stubs below with actual secure passwords
--
-- Usage:
--   psql -h your-rds-endpoint.amazonaws.com -U postgres -d postgres -f create.sql

-- =============================================================================
-- DATABASE SETUP
-- =============================================================================

-- Using default 'postgres' database (required for pg_cron)
-- No database renaming needed

-- Create required extensions
-- \c postgres postgres  -- Already connected to postgres database

-- Create dblink extension (required for cross-database queries)
CREATE EXTENSION IF NOT EXISTS dblink;

-- NOTE: pg_cron setup requires specific AWS RDS configuration
-- Follow AWS documentation before creating the extension:
-- https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL_pg_cron.html#PostgreSQL_pg_cron.enable
-- CREATE EXTENSION IF NOT EXISTS pg_cron;

-- =============================================================================
-- USER CREATION
-- =============================================================================

-- Create warehouse administrator user
-- TODO: Replace 'STUB_WHADMIN_PASSWORD' with actual secure password
CREATE USER whadmin WITH
    PASSWORD 'STUB_WHADMIN_PASSWORD'
    NOSUPERUSER
    CREATEDB
    CREATEROLE
    LOGIN;

-- Create read-only user for BI/reporting
-- TODO: Replace 'STUB_PHOOD_RO_PASSWORD' with actual secure password
CREATE USER phood_ro WITH
    PASSWORD 'STUB_PHOOD_RO_PASSWORD'
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    LOGIN;

-- Add comments for documentation
COMMENT ON ROLE whadmin IS 'Warehouse administrator - manages data warehouse operations and schema';
COMMENT ON ROLE phood_ro IS 'Read-only access for BI tools, reporting, and analytics';

-- =============================================================================
-- DATABASE-LEVEL PERMISSIONS
-- =============================================================================

-- Grant database connection privileges
GRANT CONNECT ON DATABASE postgres TO whadmin;
GRANT CONNECT ON DATABASE postgres TO phood_ro;

-- Grant schema creation privileges to whadmin (for tenant schemas)
GRANT CREATE ON DATABASE postgres TO whadmin;

-- Grant temp tables for Materialized View updates
GRANT TEMP ON DATABASE postgres TO whadmin;



-- =============================================================================
-- SCHEMA PERMISSIONS SETUP
-- =============================================================================

-- Grant whadmin access to public schema
GRANT USAGE ON SCHEMA public TO whadmin;
GRANT CREATE ON SCHEMA public TO whadmin;

-- Grant phood_ro read access to public schema
GRANT USAGE ON SCHEMA public TO phood_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO phood_ro;

-- Set default privileges for future objects in public schema
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO phood_ro;

-- =============================================================================
-- CREATE COMMON SCHEMAS
-- =============================================================================

-- Create warehouse operational schema
CREATE SCHEMA _wh AUTHORIZATION postgres;

-- Create tenant connections table
CREATE TABLE _wh.tenant_connections (
	tenant_name text NOT NULL,
	host text NOT NULL,
	port int4 DEFAULT 5432 NULL,
	dbname text NOT NULL,
	username text NOT NULL,
	"password" text NOT NULL,
	created_at timestamp DEFAULT now() NULL,
	updated_at timestamp DEFAULT now() NULL,
	CONSTRAINT tenant_connections_pkey PRIMARY KEY (tenant_name)
);

-- Create materialized view templates table
CREATE TABLE _wh.mv_templates (
	template_name text NOT NULL,
	description text,
	query_template text NOT NULL,
	column_definitions text NOT NULL,
	indexes text,
	created_at timestamp DEFAULT now() NULL,
	updated_at timestamp DEFAULT now() NULL,
	CONSTRAINT mv_templates_pkey PRIMARY KEY (template_name)
);

-- Set table ownership and permissions
ALTER TABLE _wh.tenant_connections OWNER TO postgres;
GRANT ALL ON TABLE _wh.tenant_connections TO postgres;
GRANT ALL ON TABLE _wh.tenant_connections TO whadmin;

ALTER TABLE _wh.mv_templates OWNER TO postgres;
GRANT ALL ON TABLE _wh.mv_templates TO postgres;
GRANT ALL ON TABLE _wh.mv_templates TO whadmin;

-- =============================================================================
-- SECURITY CLEANUP
-- =============================================================================

-- Remove default public schema privileges from PUBLIC role
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON DATABASE postgres FROM PUBLIC;

-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================

-- Display created users
SELECT
    rolname,
    rolsuper,
    rolcreatedb,
    rolcreaterole,
    rolcanlogin,
    rolconnlimit
FROM pg_roles
WHERE rolname IN ('whadmin', 'phood_ro', 'postgres')
ORDER BY rolname;

-- Display available extensions
SELECT
    extname,
    extversion,
    extrelocatable
FROM pg_extension
WHERE extname IN ('dblink', 'pg_cron')
ORDER BY extname;

-- NOTE: pg_cron may not show up until AWS RDS parameter group is configured

-- =============================================================================
-- NEXT STEPS
-- =============================================================================

-- 1. Disconnect from postgres user
-- 2. Connect as whadmin user:
--    psql -h your-rds-endpoint.amazonaws.com -U whadmin -d postgres
-- 3. Run the sql/functions.sql script to create warehouse schema and functions
-- 4. Load templates and begin adding tenant connections and schemas

NOTICE 'Database setup complete!';
NOTICE 'Next steps:';
NOTICE '1. Update password stubs with secure passwords';
NOTICE '2. Reconnect as whadmin user to postgres database';
NOTICE '3. Run sql/functions.sql script';
NOTICE '4. Load templates and begin tenant onboarding';