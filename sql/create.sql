-- Data Warehouse Initial Setup Script
--
-- Prerequisites:
-- 1. Connect to PostgreSQL as the master 'postgres' user
-- 2. Connect to the 'template1' database
-- 3. IMPORTANT: Update the password stubs below with actual secure passwords
--
-- Usage:
--   psql -h your-rds-endpoint.amazonaws.com -U postgres -d template1 -f create.sql

-- =============================================================================
-- DATABASE SETUP
-- =============================================================================

-- Rename default database to our warehouse name
ALTER DATABASE postgres RENAME TO phood_warehouse;

-- Create required extensions
\c phood_warehouse postgres

-- Create dblink extension (required for cross-database queries)
CREATE EXTENSION IF NOT EXISTS dblink;

-- Create pg_cron extension (for scheduled jobs)
CREATE EXTENSION IF NOT EXISTS pg_cron;

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
GRANT CONNECT ON DATABASE phood_warehouse TO whadmin;
GRANT CONNECT ON DATABASE phood_warehouse TO phood_ro;

-- Grant schema creation privileges to whadmin (for tenant schemas)
GRANT CREATE ON DATABASE phood_warehouse TO whadmin;

-- Grant temp tables for Materialized View updates
GRANT TEMP ON DATABASE phood_warehouse TO whadmin;



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
REVOKE ALL ON DATABASE phood_warehouse FROM PUBLIC;

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

-- =============================================================================
-- NEXT STEPS
-- =============================================================================

-- 1. Disconnect from postgres user
-- 2. Connect as whadmin user:
--    psql -h your-rds-endpoint.amazonaws.com -U whadmin -d phood_warehouse
-- 3. Run the _wh-ddl.sql script to create warehouse schema and functions
-- 4. Begin adding tenant connections and schemas

NOTICE 'Database setup complete!';
NOTICE 'Next steps:';
NOTICE '1. Update password stubs with secure passwords';
NOTICE '2. Reconnect as whadmin user';
NOTICE '3. Run _wh-ddl.sql script';
NOTICE '4. Begin tenant onboarding';