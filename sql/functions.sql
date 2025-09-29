-- =============================================================================
-- WAREHOUSE FUNCTIONS - TEMPLATE-BASED SYSTEM
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
    target_date date
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

    PERFORM _wh.log_info('Creating MV: ' || full_mv_name || ' for date: ' || target_date, context);

    -- Substitute placeholders in template query (only TARGET_DATE now)
    remote_query := template_data.query_template;
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
GRANT EXECUTE ON FUNCTION _wh.create_mv_from_template(text, text, text, date) TO whadmin;

-- Generic materialized view update from template
CREATE OR REPLACE FUNCTION _wh.update_mv_by_template(
    template_name text,
    tenant_connection_name text,
    target_schema text,
    target_date date,
    allow_refresh_yesterday boolean DEFAULT true
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
    target_mv_exists BOOLEAN;
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

    -- STEP 1: Check if target MV already exists
    target_mv_exists := _wh.does_mv_exist(target_schema, view_name);
    PERFORM _wh.log_info('Target MV exists: ' || target_mv_exists || ', allow_refresh_yesterday: ' || allow_refresh_yesterday, context);

    -- STEP 2: If creating NEW MV and allow_refresh_yesterday is true, refresh previous day FIRST
    IF allow_refresh_yesterday AND NOT target_mv_exists THEN
        PERFORM _wh.log_info('Creating new MV - will refresh previous day first', context);
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
                'parent_operation', 'refresh_previous_before_create'
            );

            IF _wh.does_mv_exist(target_schema, yesterday_view_name) THEN
                PERFORM _wh.log_info('Refreshing previous day MV before creating new MV: ' || yesterday_full_mv_name, yesterday_context);
                refresh_result := _wh.refresh_mv(target_schema, yesterday_view_name);
                IF NOT refresh_result THEN
                    PERFORM _wh.log_error('Failed to refresh previous day MV: ' || yesterday_full_mv_name, yesterday_context);
                    -- Don't fail the main operation for previous day refresh issues
                END IF;
            ELSE
                PERFORM _wh.log_info('Previous day MV does not exist, skipping refresh: ' || yesterday_full_mv_name, yesterday_context);
            END IF;
        END;
    ELSE
        IF target_mv_exists THEN
            PERFORM _wh.log_info('Target MV exists - skipping previous day refresh', context);
        ELSE
            PERFORM _wh.log_info('allow_refresh_yesterday is false - skipping previous day refresh', context);
        END IF;
    END IF;

    -- STEP 3: Create or refresh the target MV
    IF target_mv_exists THEN
        PERFORM _wh.log_info('Target MV exists, refreshing: ' || full_mv_name, context);
        refresh_result := _wh.refresh_mv(target_schema, view_name);
        IF NOT refresh_result THEN
            PERFORM _wh.log_error('Failed to refresh MV: ' || full_mv_name, context);
            RETURN FALSE;
        END IF;
    ELSE
        PERFORM _wh.log_info('Target MV does not exist, creating: ' || full_mv_name, context);
        refresh_result := _wh.create_mv_from_template(template_name, tenant_connection_name, target_schema, target_date);
        IF NOT refresh_result THEN
            PERFORM _wh.log_error('Failed to create MV: ' || full_mv_name, context);
            RETURN FALSE;
        END IF;
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
GRANT EXECUTE ON FUNCTION _wh.update_mv_by_template(text, text, text, date, boolean) TO whadmin;

-- Template-based bulk update function
CREATE OR REPLACE FUNCTION _wh.update_mv_window_by_template(
    template_name text,
    tenant_connection_name text,
    target_schema text,
    start_date date,
    end_date date
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
            false  -- allow_refresh_yesterday=false for bulk operations
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
GRANT EXECUTE ON FUNCTION _wh.update_mv_window_by_template(text, text, text, date, date) TO whadmin;

-- =============================================================================
-- UTILITY FUNCTIONS
-- =============================================================================

CREATE OR REPLACE FUNCTION _wh.create_mv_name(mvname text, target_date date)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
  BEGIN
      RETURN mvname || '_' || to_char(target_date, 'YYYY_MM_DD');
  END;
  $function$
;

ALTER FUNCTION _wh.create_mv_name(text, date) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION _wh.create_mv_name(text, date) TO whadmin;

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

ALTER FUNCTION _wh.does_mv_exist(text, text) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION _wh.does_mv_exist(text, text) TO whadmin;

CREATE OR REPLACE FUNCTION _wh.get_tenant_connection_string(tenant_name text)
 RETURNS text
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
  DECLARE
      conn_string TEXT;
  BEGIN
      SELECT format('host=%s port=%s dbname=%s user=%s password=%s',
                    host, port, dbname, username, password)
      INTO conn_string
      FROM _wh.tenant_connections
      WHERE tenant_connections.tenant_name = get_tenant_connection_string.tenant_name;

      IF conn_string IS NULL THEN
          RAISE EXCEPTION 'No connection found for tenant: %', tenant_name;
      END IF;

      RETURN conn_string;
  END;
  $function$
;

ALTER FUNCTION _wh.get_tenant_connection_string(text) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION _wh.get_tenant_connection_string(text) TO whadmin;

CREATE OR REPLACE FUNCTION _wh.refresh_mv(target_schema text, view_name text)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
  DECLARE
      full_mv_name TEXT;
      context JSONB;
  BEGIN
      full_mv_name := target_schema || '.' || view_name;
      context := jsonb_build_object('schema', target_schema, 'view_name', view_name);

      BEGIN
          EXECUTE format('REFRESH MATERIALIZED VIEW %I.%I', target_schema, view_name);
          PERFORM _wh.log_info('Successfully refreshed MV: ' || full_mv_name, context);
          RETURN TRUE;
      EXCEPTION
          WHEN OTHERS THEN
              PERFORM _wh.log_error('Failed to refresh MV ' || full_mv_name || ': ' || SQLERRM, context);
              RETURN FALSE;
      END;
  END;
  $function$
;

ALTER FUNCTION _wh.refresh_mv(text, text) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION _wh.refresh_mv(text, text) TO whadmin;

-- =============================================================================
-- LOGGING FUNCTIONS
-- =============================================================================

CREATE OR REPLACE FUNCTION _wh.log_info(message text, context jsonb DEFAULT '{}'::jsonb)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
  BEGIN
      RAISE INFO '[WH][%] INFO: % %', NOW(), message, context;
  END;
  $function$
;

CREATE OR REPLACE FUNCTION _wh.log_error(message text, context jsonb DEFAULT '{}'::jsonb)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
  BEGIN
      RAISE NOTICE '[WH][%] ERROR: % %', NOW(), message, context;
  END;
  $function$
;

CREATE OR REPLACE FUNCTION _wh.log_debug(message text, context jsonb DEFAULT '{}'::jsonb)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
  BEGIN
      -- Debug messages can be commented out in production
      RAISE DEBUG '[WH][%] DEBUG: % %', NOW(), message, context;
  END;
  $function$
;

ALTER FUNCTION _wh.log_info(text, jsonb) OWNER TO postgres;
ALTER FUNCTION _wh.log_error(text, jsonb) OWNER TO postgres;
ALTER FUNCTION _wh.log_debug(text, jsonb) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION _wh.log_info(text, jsonb) TO whadmin;
GRANT EXECUTE ON FUNCTION _wh.log_error(text, jsonb) TO whadmin;
GRANT EXECUTE ON FUNCTION _wh.log_debug(text, jsonb) TO whadmin;

-- =============================================================================
-- UNION VIEW FUNCTIONS
-- =============================================================================

-- Create tenant union view from template-based MVs and yearly tables
CREATE OR REPLACE FUNCTION _wh.update_tenant_union_view_by_template(
    template_name text,
    target_schema text,
    exclude_pattern text DEFAULT NULL
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
    union_sql TEXT;
    mv_record RECORD;
    table_record RECORD;
    context JSONB;
    view_count INTEGER := 0;
    view_basename TEXT;
BEGIN
    view_basename := template_name;

    context := jsonb_build_object(
        'template', template_name,
        'schema', target_schema,
        'view_basename', view_basename,
        'exclude_pattern', exclude_pattern
    );

    PERFORM _wh.log_info('Creating tenant union view for template: ' || template_name || ' in schema: ' || target_schema, context);

    -- Build UNION ALL query from all matching materialized views and yearly tables
    union_sql := '';

    -- Add yearly tables first (pattern: template_name_YYYY)
    FOR table_record IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = target_schema
        AND tablename ~ ('^' || template_name || '_[0-9]{4}$')
        ORDER BY tablename
    LOOP
        IF view_count > 0 THEN
            union_sql := union_sql || ' UNION ALL ';
        END IF;
        union_sql := union_sql || format('SELECT * FROM %I.%I', target_schema, table_record.tablename);
        view_count := view_count + 1;
        PERFORM _wh.log_debug('Added yearly table to union: ' || table_record.tablename, context);
    END LOOP;

    -- Add daily materialized views (pattern: template_name_YYYY_MM_DD)
    FOR mv_record IN
        SELECT matviewname
        FROM pg_matviews
        WHERE schemaname = target_schema
        AND matviewname LIKE template_name || '_%'
        AND (exclude_pattern IS NULL OR matviewname NOT LIKE '%' || exclude_pattern || '%')
        ORDER BY matviewname
    LOOP
        IF view_count > 0 THEN
            union_sql := union_sql || ' UNION ALL ';
        END IF;
        union_sql := union_sql || format('SELECT * FROM %I.%I', target_schema, mv_record.matviewname);
        view_count := view_count + 1;
        PERFORM _wh.log_debug('Added daily MV to union: ' || mv_record.matviewname, context);
    END LOOP;

    IF view_count = 0 THEN
        PERFORM _wh.log_error('No materialized views or yearly tables found for template pattern: ' || target_schema || '.' || template_name || '_*', context);
        RETURN FALSE;
    END IF;

    -- Create or replace the union view (no dropping)
    EXECUTE format('CREATE OR REPLACE VIEW %I.%I AS %s', target_schema, view_basename, union_sql);

    -- Set ownership and permissions
    EXECUTE format('ALTER VIEW %I.%I OWNER TO whadmin', target_schema, view_basename);
    EXECUTE format('GRANT SELECT ON %I.%I TO PUBLIC', target_schema, view_basename);

    PERFORM _wh.log_info('Successfully created tenant union view with ' || view_count || ' sources (MVs + yearly tables)', context);
    RETURN TRUE;
EXCEPTION
    WHEN OTHERS THEN
        PERFORM _wh.log_error('Failed to create tenant union view: ' || SQLERRM, context);
        RETURN FALSE;
END;
$function$;

-- Grant execute permission to whadmin only
GRANT EXECUTE ON FUNCTION _wh.update_tenant_union_view_by_template(text, text, text) TO whadmin;

-- Create public master view from all tenant union views
CREATE OR REPLACE FUNCTION _wh.update_public_view_by_template(
    template_name text
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
    union_sql TEXT;
    schema_record RECORD;
    context JSONB;
    view_count INTEGER := 0;
    public_view_name TEXT;
BEGIN
    public_view_name := template_name;

    context := jsonb_build_object(
        'template', template_name,
        'public_view_name', public_view_name
    );

    PERFORM _wh.log_info('Creating public master view for template: ' || template_name, context);

    -- Build UNION ALL query from all tenant schemas that have the template view
    union_sql := '';
    FOR schema_record IN
        SELECT DISTINCT schemaname
        FROM pg_views
        WHERE viewname = template_name
        AND schemaname != 'public'
        AND schemaname != '_wh'
        ORDER BY schemaname
    LOOP
        IF view_count > 0 THEN
            union_sql := union_sql || ' UNION ALL ';
        END IF;
        union_sql := union_sql || format('SELECT *, %L AS schema_name FROM %I.%I', schema_record.schemaname, schema_record.schemaname, template_name);
        view_count := view_count + 1;
    END LOOP;

    IF view_count = 0 THEN
        PERFORM _wh.log_error('No tenant union views found for template: ' || template_name, context);
        RETURN FALSE;
    END IF;

    -- Create or replace the public master view (no dropping)
    EXECUTE format('CREATE OR REPLACE VIEW public.%I AS %s', public_view_name, union_sql);

    -- Set ownership and permissions
    EXECUTE format('ALTER VIEW public.%I OWNER TO whadmin', public_view_name);
    EXECUTE format('GRANT SELECT ON public.%I TO PUBLIC', public_view_name);

    PERFORM _wh.log_info('Successfully created public master view with ' || view_count || ' tenant schemas', context);
    RETURN TRUE;
EXCEPTION
    WHEN OTHERS THEN
        PERFORM _wh.log_error('Failed to create public master view: ' || SQLERRM, context);
        RETURN FALSE;
END;
$function$;

-- Grant execute permission to whadmin only
GRANT EXECUTE ON FUNCTION _wh.update_public_view_by_template(text) TO whadmin;

-- =============================================================================
-- YEARLY COMBINATION FUNCTIONS
-- =============================================================================

-- Create combined yearly table from daily MVs for a specific year
CREATE OR REPLACE FUNCTION _wh.create_combined_table_from_template_by_year(
    template_name text,
    target_schema text,
    target_year integer
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
    yearly_table_name TEXT;
    column_definitions TEXT;
    indexes_sql TEXT;
    mv_record RECORD;
    context JSONB;
    pre_count BIGINT := 0;
    post_count BIGINT := 0;
    processed_count INTEGER := 0;
    inserted_count BIGINT := 0;
    temp_inserted_count BIGINT;
    start_time TIMESTAMP := NOW();
    template_exists BOOLEAN := FALSE;
    table_exists BOOLEAN := FALSE;
    stub_sql TEXT;
    create_sql TEXT;
    insert_sql TEXT;
    year_pattern TEXT;
BEGIN
    yearly_table_name := template_name || '_' || target_year::text;
    year_pattern := template_name || '_' || target_year::text || '_%';

    context := jsonb_build_object(
        'template_name', template_name,
        'target_schema', target_schema,
        'target_year', target_year,
        'yearly_table_name', yearly_table_name
    );

    PERFORM _wh.log_info('Starting yearly combination for template: ' || template_name || ' year: ' || target_year, context);

    -- Step 1: Validate template exists
    SELECT EXISTS (
        SELECT 1 FROM _wh.mv_templates
        WHERE mv_templates.template_name = create_combined_table_from_template_by_year.template_name
    ) INTO template_exists;

    IF NOT template_exists THEN
        PERFORM _wh.log_error('Template not found: ' || template_name, context);
        RETURN jsonb_build_object('success', false, 'error', 'Template not found: ' || template_name);
    END IF;

    -- Step 2: Check if yearly table already exists
    SELECT EXISTS (
        SELECT 1 FROM pg_tables
        WHERE schemaname = target_schema
        AND tablename = yearly_table_name
    ) INTO table_exists;

    IF table_exists THEN
        PERFORM _wh.log_error('Yearly table already exists: ' || target_schema || '.' || yearly_table_name, context);
        RETURN jsonb_build_object('success', false, 'error', 'Yearly table already exists: ' || target_schema || '.' || yearly_table_name);
    END IF;

    -- Step 3: Get pre-count from tenant union view (using UTC to match MV creation logic)
    BEGIN
        EXECUTE format('SELECT COUNT(*) FROM %I.%I WHERE EXTRACT(YEAR FROM logged_time AT TIME ZONE ''UTC'') = %s',
                      target_schema, template_name, target_year) INTO pre_count;
        PERFORM _wh.log_info('Pre-combination record count (UTC): ' || pre_count, context);
    EXCEPTION
        WHEN OTHERS THEN
            PERFORM _wh.log_error('Failed to get pre-count from tenant union view: ' || SQLERRM, context);
            RETURN jsonb_build_object('success', false, 'error', 'Failed to get pre-count: ' || SQLERRM);
    END;

    -- Start transaction for the combination process
    BEGIN
        -- Step 4: Recreate tenant union view excluding the year we're about to combine
        IF NOT _wh.update_tenant_union_view_by_template(template_name, target_schema, '_' || target_year::text || '_') THEN
            RAISE EXCEPTION 'Failed to recreate tenant union view with exclusions';
        END IF;
        PERFORM _wh.log_info('Recreated tenant union view excluding year: ' || target_year, context);

        -- Step 6: Get template definitions
        SELECT mv_templates.column_definitions, mv_templates.indexes INTO column_definitions, indexes_sql
        FROM _wh.mv_templates
        WHERE mv_templates.template_name = create_combined_table_from_template_by_year.template_name;

        -- Step 7: Create yearly table
        create_sql := format('CREATE TABLE %I.%I (%s)', target_schema, yearly_table_name, column_definitions);
        EXECUTE create_sql;
        PERFORM _wh.log_info('Created yearly table: ' || target_schema || '.' || yearly_table_name, context);

        -- Step 8: Set table ownership
        EXECUTE format('ALTER TABLE %I.%I OWNER TO whadmin', target_schema, yearly_table_name);
        EXECUTE format('GRANT SELECT ON %I.%I TO PUBLIC', target_schema, yearly_table_name);

        -- Step 9: Create indexes
        IF indexes_sql IS NOT NULL AND trim(indexes_sql) != '' THEN
            -- Replace placeholders in index SQL
            indexes_sql := replace(indexes_sql, '{SCHEMA}', target_schema);
            indexes_sql := replace(indexes_sql, '{VIEW_NAME}', yearly_table_name);
            EXECUTE indexes_sql;
            PERFORM _wh.log_info('Created indexes for yearly table', context);
        END IF;

        -- Step 10: Process each matching MV
        FOR mv_record IN
            SELECT matviewname
            FROM pg_matviews
            WHERE schemaname = target_schema
            AND matviewname LIKE year_pattern
            ORDER BY matviewname
        LOOP
            -- Insert data from MV into yearly table
            insert_sql := format('INSERT INTO %I.%I SELECT * FROM %I.%I',
                                target_schema, yearly_table_name, target_schema, mv_record.matviewname);
            EXECUTE insert_sql;

            GET DIAGNOSTICS temp_inserted_count = ROW_COUNT;
            inserted_count := inserted_count + temp_inserted_count;
            processed_count := processed_count + 1;

            PERFORM _wh.log_info('Inserted ' || temp_inserted_count || ' records from MV: ' || mv_record.matviewname, context);

            -- Drop the materialized view
            EXECUTE format('DROP MATERIALIZED VIEW %I.%I', target_schema, mv_record.matviewname);
            PERFORM _wh.log_info('Dropped MV: ' || mv_record.matviewname, context);
        END LOOP;

        -- Step 11: Recreate tenant union view (include yearly table + remaining MVs)
        IF NOT _wh.update_tenant_union_view_by_template(template_name, target_schema) THEN
            RAISE EXCEPTION 'Failed to recreate tenant union view';
        END IF;
        PERFORM _wh.log_info('Recreated tenant union view with yearly table', context);

        -- Step 12: Get post-count from yearly table
        EXECUTE format('SELECT COUNT(*) FROM %I.%I', target_schema, yearly_table_name) INTO post_count;
        PERFORM _wh.log_info('Post-combination record count: ' || post_count, context);

        -- Step 13: Verify record counts match
        IF pre_count != post_count THEN
            RAISE EXCEPTION 'Record count mismatch: pre_count=% post_count=%', pre_count, post_count;
        END IF;

        PERFORM _wh.log_info('Record count verification passed: ' || post_count || ' records', context);

        -- Commit the transaction (implicit)
        PERFORM _wh.log_info('Successfully completed yearly combination for ' || target_year ||
                            ': processed ' || processed_count || ' MVs, combined ' || inserted_count || ' records', context);

        RETURN jsonb_build_object(
            'success', true,
            'template_name', template_name,
            'target_year', target_year,
            'yearly_table_name', yearly_table_name,
            'processed_mvs', processed_count,
            'total_records', inserted_count,
            'pre_count', pre_count,
            'post_count', post_count,
            'duration_seconds', EXTRACT(EPOCH FROM (NOW() - start_time))
        );

    EXCEPTION
        WHEN OTHERS THEN
            -- Transaction will auto-rollback on exception
            PERFORM _wh.log_error('Yearly combination failed, transaction rolled back: ' || SQLERRM, context);
            RETURN jsonb_build_object(
                'success', false,
                'error', SQLERRM,
                'template_name', template_name,
                'target_year', target_year,
                'processed_mvs', processed_count,
                'duration_seconds', EXTRACT(EPOCH FROM (NOW() - start_time))
            );
    END;
END;
$function$;

-- Grant execute permission to whadmin only
GRANT EXECUTE ON FUNCTION _wh.create_combined_table_from_template_by_year(text, text, integer) TO whadmin;

-- =============================================================================
-- CONNECTION MANAGEMENT
-- =============================================================================
-- Tenant connections managed via direct SQL INSERT/UPDATE on _wh.tenant_connections