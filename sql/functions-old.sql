-- =============================================================================
-- WAREHOUSE FUNCTIONS
-- =============================================================================
-- This file contains all warehouse management functions.
-- Run this script after initial database setup with create.sql

-- =============================================================================
-- GENERIC TEMPLATE-BASED FUNCTIONS
-- =============================================================================

-- Generic materialized view creation from template
CREATE OR REPLACE FUNCTION _wh.create_mv_from_template(
    template_name text,
    tenant_connection_name text,
    target_schema text,
    target_date date,
    client_name text DEFAULT NULL::text
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
    connection_string TEXT;
    template_data RECORD;
    remote_query TEXT;
    view_name TEXT;
    full_mv_name TEXT;
    client_override TEXT;
    create_sql TEXT;
    context JSONB;
BEGIN
    -- Generate the full view name internally
    view_name := _wh.create_mv_name(template_name, target_date);

    -- Setup context for logging
    context := jsonb_build_object(
        'template', template_name,
        'tenant', tenant_connection_name,
        'schema', target_schema,
        'view_name', view_name,
        'target_date', target_date
    );

    -- Get template data
    BEGIN
        SELECT * INTO template_data
        FROM _wh.mv_templates
        WHERE mv_templates.template_name = create_mv_from_template.template_name;

        IF NOT FOUND THEN
            PERFORM _wh.log_error('Template not found: ' || template_name, context);
            RETURN FALSE;
        END IF;

        PERFORM _wh.log_debug('Found template: ' || template_name, context);
    EXCEPTION
        WHEN OTHERS THEN
            PERFORM _wh.log_error('Failed to get template: ' || template_name || ' - ' || SQLERRM, context);
            RETURN FALSE;
    END;

    -- Get connection string
    BEGIN
        connection_string := _wh.get_tenant_connection_string(tenant_connection_name);
        PERFORM _wh.log_debug('Got connection string for tenant: ' || tenant_connection_name, context);
    EXCEPTION
        WHEN OTHERS THEN
            PERFORM _wh.log_error('Failed to get connection string for tenant: ' || tenant_connection_name || ' - ' || SQLERRM, context);
            RETURN FALSE;
    END;

    full_mv_name := target_schema || '.' || view_name;

    -- Use client_name if provided, otherwise use target_schema
    client_override := COALESCE(client_name, target_schema);

    PERFORM _wh.log_info('Creating MV: ' || full_mv_name || ' for date: ' || target_date || ' client: ' || client_override, context);

    -- Substitute placeholders in template query
    remote_query := template_data.query_template;
    remote_query := replace(remote_query, '{CLIENT_NAME}', client_override);
    remote_query := replace(remote_query, '{TARGET_DATE}', target_date::text);

    -- Check if MV already exists
    IF _wh.does_mv_exist(target_schema, view_name) THEN
        PERFORM _wh.log_info('MV already exists, skipping: ' || full_mv_name, context);
        RETURN TRUE;
    END IF;

    -- Create the materialized view using dblink
    create_sql := format(
        'CREATE MATERIALIZED VIEW %I.%I AS SELECT * FROM dblink(%L, %L) AS t(%s)',
        target_schema,
        view_name,
        connection_string,
        remote_query,
        template_data.column_definitions
    );

    BEGIN
        EXECUTE create_sql;
        PERFORM _wh.log_info('Successfully created MV: ' || full_mv_name, context);

        -- Set ownership to whadmin
        EXECUTE format('ALTER MATERIALIZED VIEW %I.%I OWNER TO whadmin', target_schema, view_name);

        -- Grant public select access
        EXECUTE format('GRANT SELECT ON %I.%I TO PUBLIC', target_schema, view_name);

        -- Create indexes if defined
        IF template_data.indexes IS NOT NULL AND length(trim(template_data.indexes)) > 0 THEN
            DECLARE
                index_sql TEXT;
            BEGIN
                index_sql := template_data.indexes;
                index_sql := replace(index_sql, '{SCHEMA}', target_schema);
                index_sql := replace(index_sql, '{VIEW_NAME}', view_name);

                EXECUTE index_sql;
                PERFORM _wh.log_debug('Created indexes for MV: ' || full_mv_name, context);
            EXCEPTION
                WHEN OTHERS THEN
                    PERFORM _wh.log_error('Failed to create indexes for MV: ' || full_mv_name || ' - ' || SQLERRM, context);
                    -- Don't fail the entire operation for index creation issues
            END;
        END IF;

        RETURN TRUE;
    EXCEPTION
        WHEN OTHERS THEN
            PERFORM _wh.log_error('Failed to create MV: ' || full_mv_name || ' - ' || SQLERRM, context);
            RETURN FALSE;
    END;
END;
$function$;

-- Grant execute permission to whadmin only
GRANT EXECUTE ON FUNCTION _wh.create_mv_from_template(text, text, text, date, text) TO whadmin;

-- Generic materialized view update from template
CREATE OR REPLACE FUNCTION _wh.update_mv_by_template(
    template_name text,
    tenant_connection_name text,
    target_schema text,
    target_date date,
    allow_refresh_yesterday boolean DEFAULT true,
    client_name text DEFAULT NULL::text
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
    context JSONB;
    view_name TEXT;
    full_mv_name TEXT;
    yesterday_date DATE;
    refresh_result BOOLEAN;
BEGIN
    -- Generate the full view name internally
    view_name := _wh.create_mv_name(template_name, target_date);
    full_mv_name := target_schema || '.' || view_name;

    -- Setup context for logging
    context := jsonb_build_object(
        'template', template_name,
        'tenant', tenant_connection_name,
        'schema', target_schema,
        'view_name', view_name,
        'target_date', target_date,
        'allow_refresh_yesterday', allow_refresh_yesterday
    );

    PERFORM _wh.log_info('Starting MV update for template: ' || template_name || ' date: ' || target_date, context);

    -- Create or refresh the MV for target_date
    IF _wh.does_mv_exist(target_schema, view_name) THEN
        PERFORM _wh.log_info('MV exists, refreshing: ' || full_mv_name, context);
        refresh_result := _wh.refresh_mv(target_schema, view_name);
        IF NOT refresh_result THEN
            PERFORM _wh.log_error('Failed to refresh MV: ' || full_mv_name, context);
            RETURN FALSE;
        END IF;
    ELSE
        PERFORM _wh.log_info('MV does not exist, creating: ' || full_mv_name, context);
        refresh_result := _wh.create_mv_from_template(template_name, tenant_connection_name, target_schema, target_date, client_name);
        IF NOT refresh_result THEN
            PERFORM _wh.log_error('Failed to create MV: ' || full_mv_name, context);
            RETURN FALSE;
        END IF;
    END IF;

    -- Also refresh yesterday if allowed and today is the target date
    IF allow_refresh_yesterday AND target_date = CURRENT_DATE THEN
        yesterday_date := target_date - INTERVAL '1 day';
        DECLARE
            yesterday_view_name TEXT;
            yesterday_full_mv_name TEXT;
            yesterday_context JSONB;
        BEGIN
            yesterday_view_name := _wh.create_mv_name(template_name, yesterday_date);
            yesterday_full_mv_name := target_schema || '.' || yesterday_view_name;

            yesterday_context := jsonb_build_object(
                'template', template_name,
                'tenant', tenant_connection_name,
                'schema', target_schema,
                'view_name', yesterday_view_name,
                'target_date', yesterday_date,
                'parent_operation', 'refresh_yesterday'
            );

            IF _wh.does_mv_exist(target_schema, yesterday_view_name) THEN
                PERFORM _wh.log_info('Also refreshing yesterday MV: ' || yesterday_full_mv_name, yesterday_context);
                refresh_result := _wh.refresh_mv(target_schema, yesterday_view_name);
                IF NOT refresh_result THEN
                    PERFORM _wh.log_error('Failed to refresh yesterday MV: ' || yesterday_full_mv_name, yesterday_context);
                    -- Don't fail the main operation for yesterday refresh issues
                END IF;
            ELSE
                PERFORM _wh.log_debug('Yesterday MV does not exist, skipping refresh: ' || yesterday_full_mv_name, yesterday_context);
            END IF;
        END;
    END IF;

    PERFORM _wh.log_info('Successfully completed MV update for template: ' || template_name || ' date: ' || target_date, context);
    RETURN TRUE;
EXCEPTION
    WHEN OTHERS THEN
        PERFORM _wh.log_error('Error in update_mv_by_template: ' || SQLERRM, context);
        RETURN FALSE;
END;
$function$;

-- Grant execute permission to whadmin only
GRANT EXECUTE ON FUNCTION _wh.update_mv_by_template(text, text, text, date, boolean, text) TO whadmin;

-- =============================================================================
-- TEMPLATE MANAGEMENT
-- =============================================================================
-- Templates are managed via direct SQL INSERT/UPDATE on _wh.mv_templates table
-- Use DBeaver or similar tools to view/edit template data

-- Template-based bulk update function
CREATE OR REPLACE FUNCTION _wh.update_mv_window_by_template(
    template_name text,
    tenant_connection_name text,
    target_schema text,
    start_date date,
    end_date date,
    client_name text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
    loop_date DATE;
    date_results JSONB := '[]';
    success_count INTEGER := 0;
    error_count INTEGER := 0;
    start_time TIMESTAMP := NOW();
    date_result BOOLEAN;
    date_info JSONB;
BEGIN
    -- Validate date range
    IF start_date > end_date THEN
        RAISE EXCEPTION 'start_date (%) cannot be greater than end_date (%)', start_date, end_date;
    END IF;

    -- Validate template exists
    IF NOT EXISTS (SELECT 1 FROM _wh.mv_templates WHERE mv_templates.template_name = update_mv_window_by_template.template_name) THEN
        RAISE EXCEPTION 'Template not found: %', template_name;
    END IF;

    -- Loop through each date in the range (inclusive)
    loop_date := start_date;
    WHILE loop_date <= end_date LOOP
        -- Call template-based function with allow_refresh_yesterday=false
        date_result := _wh.update_mv_by_template(
            template_name,
            tenant_connection_name,
            target_schema,
            loop_date,
            false,  -- allow_refresh_yesterday=false for bulk operations
            client_name
        );

        -- Track results
        date_info := jsonb_build_object(
            'date', loop_date,
            'success', date_result
        );
        date_results := date_results || date_info;

        IF date_result THEN
            success_count := success_count + 1;
        ELSE
            error_count := error_count + 1;
        END IF;

        -- Move to next date
        loop_date := loop_date + INTERVAL '1 day';
    END LOOP;

    -- Return summary
    RETURN jsonb_build_object(
        'template_name', template_name,
        'tenant', tenant_connection_name,
        'schema', target_schema,
        'start_date', start_date,
        'end_date', end_date,
        'total_dates', success_count + error_count,
        'success_count', success_count,
        'error_count', error_count,
        'duration_seconds', EXTRACT(EPOCH FROM (NOW() - start_time)),
        'date_results', date_results
    );
EXCEPTION
    WHEN OTHERS THEN
        RETURN jsonb_build_object(
            'error', SQLERRM,
            'success', false,
            'template_name', template_name,
            'tenant', tenant_connection_name,
            'schema', target_schema
        );
END;
$function$;

-- Grant execute permission to whadmin only
GRANT EXECUTE ON FUNCTION _wh.update_mv_window_by_template(text, text, text, date, date, text) TO whadmin;

-- =============================================================================
-- UTILITY FUNCTIONS
-- =============================================================================
  DECLARE
      connection_string TEXT;
      remote_query TEXT;
      view_name TEXT;
      full_mv_name TEXT;
      client_override TEXT;
      create_sql TEXT;
      context JSONB;
  BEGIN
      -- Generate the full view name internally
      view_name := _wh.create_mv_name(base_view_name, target_date);

      -- Setup context for logging
      context := jsonb_build_object(
          'tenant', tenant_connection_name,
          'schema', target_schema,
          'view_name', view_name,
          'target_date', target_date
      );

      -- Get connection string
      BEGIN
          connection_string := _wh.get_tenant_connection_string(tenant_connection_name);
          PERFORM _wh.log_debug('Got connection string for tenant: ' || tenant_connection_name, context);
      EXCEPTION
          WHEN OTHERS THEN
              PERFORM _wh.log_error('Failed to get connection string for tenant: ' || tenant_connection_name || ' - ' || SQLERRM, context);
              RETURN FALSE;
      END;

      full_mv_name := target_schema || '.' || view_name;

      -- Use client_name if provided, otherwise use tenant_connection_name
      client_override := COALESCE(client_name, target_schema);

      PERFORM _wh.log_info('Creating MV: ' || full_mv_name || ' for date: ' || target_date || ' client: ' || client_override, context);

      -- Build the remote query
      remote_query := format($remote$
          SELECT
              pf.id,
              pl.name AS store,
              s.station,
              pf.validation_status,
              pr.name AS region,
              pf.action_taken_id,
              pa.name AS action_taken,
              pi.name,
              pf.quantity,
              pp.average_weight,
              (pf.quantity - pp.average_weight) AS weight,
              '%s' AS client,
              pfo.name AS food_category,
              pi.cost_per_lb,
              pi.target_weight,
              pi.price_per_lb,
              par.name AS action_reason,
              pi.client_id,
              pf.logged_time,
              pf.flags
          FROM phood_foodlogsum pf
          LEFT JOIN phood_actionreason par ON (pf.action_reason_id = par.id)
          LEFT JOIN phood_location pl ON (pf.location_id = pl.id)
          LEFT JOIN phood_region pr ON (pl.region_id = pr.id)
          LEFT JOIN phood_actiontaken pa ON pf.action_taken_id = pa.id
          LEFT JOIN phood_inventoryitem pi ON pf.inventory_item_id = pi.id
          LEFT JOIN phood_pan pp ON pp.id = pf.pan_id
          LEFT JOIN phood_foodcategory pfo ON pfo.id = pi.food_category_id
          LEFT JOIN (
              SELECT DISTINCT ps.name AS station
              , pm.inventory_item_id
              , pm.location_id
              , pm.station_id
              FROM phood_menuitem pm
              LEFT JOIN phood_station ps ON ps.id = pm.station_id
          ) s ON (pf.inventory_item_id = s.inventory_item_id AND s.location_id = pf.location_id)
          WHERE pf.logged_time::DATE = '%s'::DATE
      $remote$, client_override, target_date);

      -- Create the materialized view using explicit column definitions
      create_sql := format($create$
          CREATE MATERIALIZED VIEW %I.%I AS
          SELECT * FROM dblink(%L, %L)
          AS t(
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
          )
      $create$, target_schema, view_name, connection_string, remote_query);

      BEGIN
          PERFORM _wh.log_debug('Executing CREATE MATERIALIZED VIEW', context);
          EXECUTE create_sql;

          -- Ensure MV is owned by whadmin for consistency
          EXECUTE format('ALTER MATERIALIZED VIEW %I.%I OWNER TO whadmin', target_schema, view_name);

          -- Grant read access to all users
          EXECUTE format('GRANT SELECT ON %I.%I TO PUBLIC', target_schema, view_name);

          PERFORM _wh.log_info('Successfully created MV: ' || full_mv_name, context);
      EXCEPTION
          WHEN OTHERS THEN
              PERFORM _wh.log_error('Failed to create MV ' || full_mv_name || ': ' || SQLERRM, context);
              RETURN FALSE;
      END;

      -- Create all the indexes for optimal BI performance
      BEGIN
          PERFORM _wh.log_debug('Creating indexes for MV: ' || full_mv_name, context);
          EXECUTE format('CREATE UNIQUE INDEX ON %I.%I (id)', target_schema, view_name);
          EXECUTE format('CREATE INDEX ON %I.%I (logged_time)', target_schema, view_name);
          EXECUTE format('CREATE INDEX ON %I.%I (client)', target_schema, view_name);
          EXECUTE format('CREATE INDEX ON %I.%I (store)', target_schema, view_name);
          EXECUTE format('CREATE INDEX ON %I.%I (action_taken_id)', target_schema, view_name);
          PERFORM _wh.log_info('Successfully created all indexes for MV: ' || full_mv_name, context);
      EXCEPTION
          WHEN OTHERS THEN
              PERFORM _wh.log_error('Failed to create indexes for MV ' || full_mv_name || ': ' || SQLERRM, context);
              RETURN FALSE;
      END;

      PERFORM _wh.log_info('Successfully completed MV creation: ' || full_mv_name, context);
      RETURN TRUE;

  END;
  $function$
;

-- Permissions

ALTER FUNCTION _wh.create_foodlogstats_mv(text, text, text, date, text) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION _wh.create_foodlogstats_mv(text, text, text, date, text) TO whadmin;

-- DROP FUNCTION _wh.create_mv_name(text, date);

CREATE OR REPLACE FUNCTION _wh.create_mv_name(mvname text, target_date date)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
  BEGIN
      RETURN mvname || '_' || to_char(target_date, 'YYYY_MM_DD');
  END;
  $function$
;

-- Permissions

ALTER FUNCTION _wh.create_mv_name(text, date) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION _wh.create_mv_name(text, date) TO whadmin;

-- DROP FUNCTION _wh.does_mv_exist(text, text);

CREATE OR REPLACE FUNCTION _wh.does_mv_exist(target_schema text, view_name text)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
  BEGIN
      RETURN EXISTS (
          SELECT 1 FROM pg_matviews
          WHERE schemaname = target_schema
          AND matviewname = view_name
      );
  END;
  $function$
;

-- Permissions

ALTER FUNCTION _wh.does_mv_exist(text, text) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION _wh.does_mv_exist(text, text) TO whadmin;

-- DROP FUNCTION _wh.get_tenant_connection_string(text);

CREATE OR REPLACE FUNCTION _wh.get_tenant_connection_string(tenant_name text)
 RETURNS text
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
  DECLARE
      conn_string TEXT;
      tenant_rec RECORD;
  BEGIN
      -- Get connection details for the tenant
      SELECT host, port, dbname, username, password
      INTO tenant_rec
      FROM _wh.tenant_connections
      WHERE tenant_connections.tenant_name = get_tenant_connection_string.tenant_name;

      -- Check if tenant exists
      IF NOT FOUND THEN
          RAISE EXCEPTION 'Tenant % not found in tenant_connections', tenant_name;
      END IF;

      -- Build connection string for dblink
      conn_string := format('host=%s port=%s dbname=%s user=%s password=%s',
          tenant_rec.host,
          tenant_rec.port,
          tenant_rec.dbname,
          tenant_rec.username,
          tenant_rec.password
      );

      RETURN conn_string;
  END;
  $function$
;

-- Permissions

ALTER FUNCTION _wh.get_tenant_connection_string(text) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION _wh.get_tenant_connection_string(text) TO whadmin;

-- DROP FUNCTION _wh.log(text, text, jsonb);

CREATE OR REPLACE FUNCTION _wh.log(level text, message text, context jsonb DEFAULT NULL::jsonb)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
  BEGIN
      CASE upper(level)
          WHEN 'DEBUG' THEN
              RAISE DEBUG '[WH][%] %', NOW(), message;
          WHEN 'INFO' THEN
              RAISE NOTICE '[WH][%] %', NOW(), message;
          WHEN 'WARN' THEN
              RAISE WARNING '[WH][%] %', NOW(), message;
          WHEN 'ERROR' THEN
              RAISE WARNING '[WH][%] ERROR: %', NOW(), message;  -- Still use WARNING for DataDog
          ELSE
              RAISE NOTICE '[WH][%] %', NOW(), message;
      END CASE;

      -- Optional: Insert into log table for persistent logging
      -- INSERT INTO _wh.logs (level, message, context, logged_at) 
      -- VALUES (level, message, context, NOW());
  END;
  $function$
;

-- Permissions

ALTER FUNCTION _wh.log(text, text, jsonb) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION _wh.log(text, text, jsonb) TO whadmin;

-- DROP FUNCTION _wh.log_debug(text, jsonb);

CREATE OR REPLACE FUNCTION _wh.log_debug(message text, context jsonb DEFAULT NULL::jsonb)
 RETURNS void
 LANGUAGE plpgsql
AS $function$ BEGIN PERFORM _wh.log('DEBUG', message, context); END; $function$
;

-- Permissions

ALTER FUNCTION _wh.log_debug(text, jsonb) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION _wh.log_debug(text, jsonb) TO whadmin;

-- DROP FUNCTION _wh.log_error(text, jsonb);

CREATE OR REPLACE FUNCTION _wh.log_error(message text, context jsonb DEFAULT NULL::jsonb)
 RETURNS void
 LANGUAGE plpgsql
AS $function$ BEGIN PERFORM _wh.log('ERROR', message, context); END; $function$
;

-- Permissions

ALTER FUNCTION _wh.log_error(text, jsonb) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION _wh.log_error(text, jsonb) TO whadmin;

-- DROP FUNCTION _wh.log_info(text, jsonb);

CREATE OR REPLACE FUNCTION _wh.log_info(message text, context jsonb DEFAULT NULL::jsonb)
 RETURNS void
 LANGUAGE plpgsql
AS $function$ BEGIN PERFORM _wh.log('INFO', message, context); END; $function$
;

-- Permissions

ALTER FUNCTION _wh.log_info(text, jsonb) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION _wh.log_info(text, jsonb) TO whadmin;

-- DROP FUNCTION _wh.log_warn(text, jsonb);

CREATE OR REPLACE FUNCTION _wh.log_warn(message text, context jsonb DEFAULT NULL::jsonb)
 RETURNS void
 LANGUAGE plpgsql
AS $function$ BEGIN PERFORM _wh.log('WARN', message, context); END; $function$
;

-- Permissions

ALTER FUNCTION _wh.log_warn(text, jsonb) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION _wh.log_warn(text, jsonb) TO whadmin;

-- DROP FUNCTION _wh.refresh_mv(text, text);

CREATE OR REPLACE FUNCTION _wh.refresh_mv(target_schema text, view_name text)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
  DECLARE
      full_mv_name TEXT;
  BEGIN
      full_mv_name := target_schema || '.' || view_name;

      PERFORM _wh.log_info('Refreshing MV: ' || full_mv_name);

      EXECUTE format('REFRESH MATERIALIZED VIEW CONCURRENTLY %I.%I', target_schema, view_name);

      PERFORM _wh.log_info('Successfully refreshed MV: ' || full_mv_name);
      RETURN TRUE;
  EXCEPTION
      WHEN OTHERS THEN
          PERFORM _wh.log_error('Failed to refresh MV ' || full_mv_name || ': ' || SQLERRM);
          RETURN FALSE;
  END;
  $function$
;

-- Permissions

ALTER FUNCTION _wh.refresh_mv(text, text) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION _wh.refresh_mv(text, text) TO whadmin;

-- DROP FUNCTION _wh.set_tenant_connection(text, text, int4, text, text, text);

CREATE OR REPLACE FUNCTION _wh.set_tenant_connection(name text, host text, port integer, dbname text, username text, password text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
  BEGIN
      INSERT INTO _wh.tenant_connections (
          tenant_name,
          host,
          port,
          dbname,
          username,
          password
      ) VALUES (
          name,
          host,
          port,
          dbname,
          username,
          password  -- Encrypt with blowfish
      )
      ON CONFLICT (tenant_name)
      DO UPDATE SET
          host = EXCLUDED.host,
          port = EXCLUDED.port,
          dbname = EXCLUDED.dbname,
          username = EXCLUDED.username,
          password = EXCLUDED.password,
          updated_at = NOW();
  END;
  $function$
;

-- Permissions

ALTER FUNCTION _wh.set_tenant_connection(text, text, int4, text, text, text) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION _wh.set_tenant_connection(text, text, int4, text, text, text) TO whadmin;

-- DROP FUNCTION _wh.update_foodlogstats(text, text, date, bool);

CREATE OR REPLACE FUNCTION _wh.update_foodlogstats(tenant_connection_name text, target_schema text, target_date date, allow_refresh_yesterday boolean DEFAULT true)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
  BEGIN
      RETURN _wh.update_mv_core(
          tenant_connection_name,
          target_schema,
          target_date,
          'foodlogstats',
          '_wh.create_foodlogstats_mv',
          allow_refresh_yesterday
      );
  END;
  $function$
;

-- Permissions

ALTER FUNCTION _wh.update_foodlogstats(text, text, date, bool) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION _wh.update_foodlogstats(text, text, date, bool) TO whadmin;

-- DROP FUNCTION _wh.update_mv_core(text, text, date, text, text, bool);

CREATE OR REPLACE FUNCTION _wh.update_mv_core(tenant_connection_name text, target_schema text, target_date date, view_basename text, create_function_name text, allow_refresh_yesterday boolean DEFAULT true)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
  DECLARE
      result JSONB := '{}';
      view_name TEXT;
      yesterday_view_name TEXT;
      yesterday_date DATE;
      mv_exists BOOLEAN;
      yesterday_mv_exists BOOLEAN;
      operation_count INTEGER := 0;
      start_time TIMESTAMP := NOW();
      today_mv_created BOOLEAN := FALSE;
      context JSONB;
      create_result BOOLEAN;
  BEGIN
      -- Generate view names
      view_name := _wh.create_mv_name(view_basename, target_date);
      yesterday_date := target_date - INTERVAL '1 day';
      yesterday_view_name := _wh.create_mv_name(view_basename, yesterday_date);

      -- Setup context for logging
      context := jsonb_build_object(
          'tenant', tenant_connection_name,
          'schema', target_schema,
          'target_date', target_date,
          'view_name', view_name,
          'view_basename', view_basename,
          'create_function', create_function_name
      );

      result := jsonb_build_object(
          'tenant', tenant_connection_name,
          'schema', target_schema,
          'target_date', target_date,
          'view_name', view_name,
          'view_basename', view_basename,
          'start_time', start_time,
          'operations', '[]'::jsonb
      );

      -- Check if today's MV exists
      mv_exists := _wh.does_mv_exist(target_schema, view_name);

      PERFORM _wh.log_info('Processing ' || view_name || ': MV exists=' || mv_exists, context);

      -- First, refresh yesterday's MV if it exists and refresh is allowed
      IF allow_refresh_yesterday THEN
          yesterday_mv_exists := _wh.does_mv_exist(target_schema, yesterday_view_name);
          IF yesterday_mv_exists THEN
              PERFORM _wh.log_info('Refreshing yesterday''s MV first: ' || yesterday_view_name, context);
              IF _wh.refresh_mv(target_schema, yesterday_view_name) THEN
                  PERFORM _wh.log_info('Successfully refreshed yesterday''s MV: ' || yesterday_view_name, context);
                  result := jsonb_set(result, '{operations}',
                      (result->'operations') || jsonb_build_object('operation', 'refresh_yesterday', 'view', yesterday_view_name, 'timestamp', NOW(), 'success', true));
                  operation_count := operation_count + 1;
              ELSE
                  PERFORM _wh.log_error('Failed to refresh yesterday''s MV: ' || yesterday_view_name, context);
                  result := jsonb_set(result, '{operations}',
                      (result->'operations') || jsonb_build_object('operation', 'refresh_yesterday', 'view', yesterday_view_name, 'timestamp', NOW(), 'success', false));
              END IF;
          ELSE
              PERFORM _wh.log_warn('Yesterday''s MV does not exist: ' || yesterday_view_name, context);
          END IF;
      END IF;

      -- Now handle today's MV
      IF NOT mv_exists THEN
          -- Create new MV using dynamic function call
          PERFORM _wh.log_info('Creating new MV: ' || view_name || ' using ' || create_function_name, context);

          -- Dynamic function call
          EXECUTE format('SELECT %s(%L, %L, %L, %L, %L)',
              create_function_name,
              tenant_connection_name,
              target_schema,
              view_basename,
              target_date,
              target_schema
          ) INTO create_result;

          IF create_result THEN
              today_mv_created := TRUE;
              PERFORM _wh.log_info('Successfully created new MV: ' || view_name, context);
              result := jsonb_set(result, '{operations}',
                  (result->'operations') || jsonb_build_object('operation', 'create_new', 'view', view_name, 'timestamp', NOW(), 'success', true));
              operation_count := operation_count + 1;
          ELSE
              PERFORM _wh.log_error('Failed to create new MV: ' || view_name, context);
              result := jsonb_set(result, '{operations}',
                  (result->'operations') || jsonb_build_object('operation', 'create_new', 'view', view_name, 'timestamp', NOW(), 'success', false));
          END IF;
      ELSE
          -- MV exists, try to refresh
          PERFORM _wh.log_info('Refreshing existing MV: ' || view_name, context);
          IF _wh.refresh_mv(target_schema, view_name) THEN
              PERFORM _wh.log_info('Successfully refreshed existing MV: ' || view_name, context);
              result := jsonb_set(result, '{operations}',
                  (result->'operations') || jsonb_build_object('operation', 'refresh_existing', 'view', view_name, 'timestamp', NOW(), 'success', true));
              operation_count := operation_count + 1;
          ELSE
              PERFORM _wh.log_error('Failed to refresh existing MV: ' || view_name, context);
              result := jsonb_set(result, '{operations}',
                  (result->'operations') || jsonb_build_object('operation', 'refresh_existing', 'view', view_name, 'timestamp', NOW(), 'success', false));
          END IF;
      END IF;

      -- Add summary to result
      result := result || jsonb_build_object(
          'end_time', NOW(),
          'duration_seconds', EXTRACT(EPOCH FROM (NOW() - start_time)),
          'operation_count', operation_count,
          'success', true
      );

      PERFORM _wh.log_info('Completed processing for ' || view_name || ': ' || operation_count || ' operations in ' || EXTRACT(EPOCH FROM (NOW() - start_time)) || ' seconds', context);

      RETURN result;

  EXCEPTION
      WHEN OTHERS THEN
          result := result || jsonb_build_object(
              'end_time', NOW(),
              'duration_seconds', EXTRACT(EPOCH FROM (NOW() - start_time)),
              'error', SQLERRM,
              'success', false
          );
          PERFORM _wh.log_error('Failed processing ' || view_name || ': ' || SQLERRM, context);
          RETURN result;
  END;
  $function$
;

-- Permissions

ALTER FUNCTION _wh.update_mv_core(text, text, date, text, text, bool) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION _wh.update_mv_core(text, text, date, text, text, bool) TO whadmin;

-- DROP FUNCTION _wh.update_mv_window(text, text, date, date, text);

CREATE OR REPLACE FUNCTION _wh.update_mv_window(tenant_connection_name text, target_schema text, start_date date, end_date date, update_function_name text)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
    DECLARE
        loop_date DATE;
        date_results JSONB := '[]';
        success_count INTEGER := 0;
        error_count INTEGER := 0;
        start_time TIMESTAMP := NOW();
        date_result JSONB;
    BEGIN
        -- Validate date range
        IF start_date > end_date THEN
            RAISE EXCEPTION 'start_date (%) cannot be greater than end_date (%)', start_date, end_date;
        END IF;

        -- Loop through each date in the range (inclusive)
        loop_date := start_date;
        WHILE loop_date <= end_date LOOP
            -- Dynamic function call with allow_refresh_yesterday=false
            EXECUTE format('SELECT %s(%L, %L, %L, false)',
                update_function_name,
                tenant_connection_name,
                target_schema,
                loop_date
            ) INTO date_result;

            -- Track results
            date_results := date_results || date_result;

            IF (date_result->>'success')::boolean THEN
                success_count := success_count + 1;
            ELSE
                error_count := error_count + 1;
            END IF;

            -- Move to next date
            loop_date := loop_date + INTERVAL '1 day';
        END LOOP;

        -- Return summary
        RETURN jsonb_build_object(
            'start_date', start_date,
            'end_date', end_date,
            'total_dates', success_count + error_count,
            'success_count', success_count,
            'error_count', error_count,
            'duration_seconds', EXTRACT(EPOCH FROM (NOW() - start_time)),
            'date_results', date_results
        );

    EXCEPTION
        WHEN OTHERS THEN
            RETURN jsonb_build_object(
                'error', SQLERRM,
                'success', false
            );
    END;
    $function$
;

-- Permissions

ALTER FUNCTION _wh.update_mv_window(text, text, date, date, text) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION _wh.update_mv_window(text, text, date, date, text) TO whadmin;

-- DROP FUNCTION _wh.update_tenant_union_view(text, text, text);

CREATE OR REPLACE FUNCTION _wh.update_tenant_union_view(tenant_connection_name text, target_schema text, view_basename text)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
    DECLARE
        mv_record RECORD;
        union_query TEXT := '';
        view_name TEXT;
        mv_count INTEGER := 0;
        start_time TIMESTAMP := NOW();
        context JSONB;
    BEGIN
        -- Setup context for logging
        context := jsonb_build_object(
            'tenant', tenant_connection_name,
            'schema', target_schema,
            'view_basename', view_basename
        );

        view_name := target_schema || '.' || view_basename;

        PERFORM _wh.log_info('Creating union view: ' || view_name, context);

        -- Find all materialized views matching the pattern and sort by name
        FOR mv_record IN
            SELECT matviewname
            FROM pg_matviews
            WHERE schemaname = target_schema
              AND matviewname LIKE view_basename || '_%'
            ORDER BY matviewname
        LOOP
            -- Add UNION ALL between queries (except for the first one)
            IF mv_count > 0 THEN
                union_query := union_query || ' UNION ALL ';
            END IF;

            -- Add the SELECT statement for this materialized view
            union_query := union_query || format('SELECT * FROM %I.%I', target_schema, mv_record.matviewname);

            mv_count := mv_count + 1;
        END LOOP;

        -- Check if we found any materialized views
        IF mv_count = 0 THEN
            PERFORM _wh.log_warn('No materialized views found matching pattern: ' || view_basename || '_%', context);
            RETURN jsonb_build_object(
                'tenant', tenant_connection_name,
                'schema', target_schema,
                'view_basename', view_basename,
                'view_name', view_name,
                'mv_count', 0,
                'success', false,
                'error', 'No matching materialized views found'
            );
        END IF;

        -- Create or replace the view
        EXECUTE format('CREATE OR REPLACE VIEW %I.%I AS %s', target_schema, view_basename, union_query);

        -- Grant read access to all users
        EXECUTE format('GRANT SELECT ON %I.%I TO PUBLIC', target_schema, view_basename);

        PERFORM _wh.log_info('Successfully created union view: ' || view_name || ' with ' || mv_count || ' materialized views', context);

        -- Return success result
        RETURN jsonb_build_object(
            'tenant', tenant_connection_name,
            'schema', target_schema,
            'view_basename', view_basename,
            'view_name', view_name,
            'mv_count', mv_count,
            'duration_seconds', EXTRACT(EPOCH FROM (NOW() - start_time)),
            'success', true
        );

    EXCEPTION
        WHEN OTHERS THEN
            PERFORM _wh.log_error('Failed to create union view ' || view_name || ': ' || SQLERRM, context);
            RETURN jsonb_build_object(
                'tenant', tenant_connection_name,
                'schema', target_schema,
                'view_basename', view_basename,
                'view_name', view_name,
                'mv_count', mv_count,
                'error', SQLERRM,
                'success', false
            );
    END;
    $function$
;

-- Permissions

ALTER FUNCTION _wh.update_tenant_union_view(text, text, text) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION _wh.update_tenant_union_view(text, text, text) TO whadmin;


-- Schema-level permissions

-- Remove public access to _wh schema
REVOKE ALL ON SCHEMA _wh FROM PUBLIC;

-- Grant schema access
GRANT ALL ON SCHEMA _wh TO postgres;
GRANT USAGE ON SCHEMA _wh TO whadmin;

-- Grant default privileges for future objects in _wh schema
ALTER DEFAULT PRIVILEGES IN SCHEMA _wh GRANT EXECUTE ON FUNCTIONS TO whadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA _wh GRANT ALL ON TABLES TO whadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA _wh GRANT ALL ON SEQUENCES TO whadmin;

