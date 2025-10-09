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

---------------------
-- LandB Stage
insert into _wh.tenant_connections (tenant_name, host, port, dbname, username, password) values (
    'landb-stage',	
    'phood-api-landb-stage.cluster-ro-ca2a5qa99qrs.us-east-1.rds.amazonaws.com',
    5432,
    'phood_api_server',
    'phood_rw',
    'xfTawAydkKMTVxFv'
);
    
-- LandB Prod
insert into _wh.tenant_connections (tenant_name, host, port, dbname, username, password) values (
    'landb-prod',	
    'phood-api-landb-prod.cluster-ro-ca2a5qa99qrs.us-east-1.rds.amazonaws.com',
    5432,
    'phood_api_server',
    'phood_ro',
    '3aWhAWLTx4pOTGer'
);


---------------------
-- MM Stage
insert into _wh.tenant_connections (tenant_name, host, port, dbname, username, password) values (
    'mm-stage',	
    'phood-api-mm-stage.cluster-ro-ca2a5qa99qrs.us-east-1.rds.amazonaws.com',
    5432,
    'phood_api_server',
    'phood_rw',
    'dI4SXWbHgxWBmpvu'
);
    
-- MM Prod
insert into _wh.tenant_connections (tenant_name, host, port, dbname, username, password) values (
    'mm-prod',	
    'phood-api-mm-prod.cluster-ro-ca2a5qa99qrs.us-east-1.rds.amazonaws.com',
    5432,
    'phood_api_server',
    'phood_ro',
    'gQe4KakEWtHnr1VK'
);


---------------------
-- AF Stage
insert into _wh.tenant_connections (tenant_name, host, port, dbname, username, password) values (
    'af-stage',	
    'phood-api-af-stage.cluster-ro-ca2a5qa99qrs.us-east-1.rds.amazonaws.com',
    5432,
    'phood_api_server',
    'phood_rw',
    '4n5T3o5cjve26rci'
);
    
-- AF Prod
insert into _wh.tenant_connections (tenant_name, host, port, dbname, username, password) values (
    'af-prod',	
    'phood-api-af-prod.cluster-ro-ca2a5qa99qrs.us-east-1.rds.amazonaws.com',
    5432,
    'phood_api_server',
    'phood_ro',
    'jDRKZ1Ld7EWkB0uO'
);


---------------------
-- RB Stage
insert into _wh.tenant_connections (tenant_name, host, port, dbname, username, password) values (
    'rb-stage',	
    'phood-api-rb-stage.cluster-ro-ca2a5qa99qrs.us-east-1.rds.amazonaws.com',
    5432,
    'phood_api_server',
    'phood_rw',
    'IyCF4isnMJGvBR0Z'
);
    
-- RB Prod (phood_ro creds are missing from AWS Secrets Manager)
insert into _wh.tenant_connections (tenant_name, host, port, dbname, username, password) values (
    'rb-prod',	
    'phood-api-rb-prod.cluster-ro-ca2a5qa99qrs.us-east-1.rds.amazonaws.com',
    5432,
    'phood_api_server',
    'phood_rw',
    '6nERdRZJRsqupvC6'
);


---------------------
-- WFM Stage (not working)
insert into _wh.tenant_connections (tenant_name, host, port, dbname, username, password) values (
    'wfm-stage',	
    'phood-api-wfm-stage.cluster-ro-ca2a5qa99qrs.us-east-1.rds.amazonaws.com',
    5432,
    'phood_api_server',
    'phood_rw',
    'ZEIsFcmZCFXXOH46'
);
    
-- WFM Prod
insert into _wh.tenant_connections (tenant_name, host, port, dbname, username, password) values (
    'wfm-prod',	
    'phood-api-wfm-prod.cluster-ro-ca2a5qa99qrs.us-east-1.rds.amazonaws.com',
    5432,
    'phood_api_server',
    'phood_ro',
    'WnCKfpIfwGLXpt2R'
);


---------------------
-- WFM-MK Stage
insert into _wh.tenant_connections (tenant_name, host, port, dbname, username, password) values (
    'wfmmk-stage',	
    'phood-api-wfm-mk-stage.cluster-ro-ca2a5qa99qrs.us-east-1.rds.amazonaws.com',
    5432,
    'phood_api_server',
    'phood_rw',
    'NpoQRJID8buS1bap'
);
    
-- WFM-MK Prod
insert into _wh.tenant_connections (tenant_name, host, port, dbname, username, password) values (
    'wfmmk-prod',	
    'phood-api-wfm-mk-prod.cluster-ro-ca2a5qa99qrs.us-east-1.rds.amazonaws.com',
    5432,
    'phood_api_server',
    'phood_ro',
    'RtjpVWV49QyG4GA7'
);


---------------------
-- TK Stage
insert into _wh.tenant_connections (tenant_name, host, port, dbname, username, password) values (
    'tk-stage',	
    'phood-api-tk-stage.cluster-ro-ca2a5qa99qrs.us-east-1.rds.amazonaws.com',
    5432,
    'phood_api_server',
    'phood_rw',
    'e8JnqqM02eCnucRZ'
);
    
-- TK Prod
insert into _wh.tenant_connections (tenant_name, host, port, dbname, username, password) values (
    'tk-prod',	
    'phood-api-tk-prod.cluster-ro-ca2a5qa99qrs.us-east-1.rds.amazonaws.com',
    5432,
    'phood_api_server',
    'phood_ro',
    'bFf4D2sFjGjdBay0'
);

