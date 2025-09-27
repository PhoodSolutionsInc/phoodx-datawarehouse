-- Tenant Connection Data
-- This file contains INSERT statements for tenant database connections
-- Copy this file, update the values, and run it to add tenant connections

-- Example tenant connection (replace with actual values)
-- INSERT INTO _wh.tenant_connections (
--     tenant_name,
--     host,
--     port,
--     dbname,
--     username,
--     password
-- ) VALUES (
--     'example_tenant',
--     'example-db.amazonaws.com',
--     5432,
--     'production_db',
--     'readonly_user',
--     'secure_password_here'
-- );

-- Template for additional tenants:
-- INSERT INTO _wh.tenant_connections (tenant_name, host, port, dbname, username, password)
-- VALUES ('tenant_name', 'host.com', 5432, 'dbname', 'username', 'password');

-- Use the _wh.set_tenant_connection function instead for secure password handling:
-- SELECT _wh.set_tenant_connection('tenant_name', 'host.com', 5432, 'dbname', 'username', 'password');