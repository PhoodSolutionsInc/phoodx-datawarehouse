-- =============================================================================
-- WAREHOUSE FUNCTIONS - TEMPLATE-BASED SYSTEM
-- =============================================================================
-- This file contains all warehouse management functions.
-- Run this script after initial database setup with create.sql
--
-- NAMING CONVENTION: Functions use domain-first naming for better organization:
-- - mv_* : Materialized view operations
-- - union_view_* : Union view management
-- - year_table_* : Yearly table operations
-- - util_* : Utility functions
-- - log_* : Logging functions


-- =============================================================================
-- UTILITY FUNCTIONS
-- =============================================================================

-- Generate standardized MV names
CREATE OR REPLACE FUNCTION _wh.mv_create_name(mvname text, target_date date)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $function$
BEGIN
    RETURN mvname || '_' || TO_CHAR(target_date, 'YYYY_MM_DD');
END;
$function$;

-- Get current UTC date with optional day offset
CREATE OR REPLACE FUNCTION _wh.util_current_date_utc(interval_offset integer DEFAULT 0)
RETURNS date
LANGUAGE sql
STABLE
AS $function$
    SELECT ((NOW() AT TIME ZONE 'UTC') + (interval_offset || ' days')::INTERVAL)::DATE;
$function$;

-- Check if materialized view exists
CREATE OR REPLACE FUNCTION _wh.mv_does_exist(target_schema text, view_name text)
RETURNS boolean
LANGUAGE plpgsql
STABLE
AS $function$
BEGIN
    RETURN EXISTS (
        SELECT 1
        FROM pg_matviews
        WHERE schemaname = target_schema
        AND matviewname = view_name
    );
END;
$function$;

-- Get tenant connection string (SECURITY DEFINER for controlled access)
CREATE OR REPLACE FUNCTION _wh.util_get_tenant_connection_string(tenant_name text)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
STABLE
AS $function$
DECLARE
    conn_record RECORD;
BEGIN
    SELECT host, port, dbname, username, password
    INTO conn_record
    FROM _wh.tenant_connections
    WHERE tenant_connections.tenant_name = util_get_tenant_connection_string.tenant_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Tenant connection not found: %', tenant_name;
    END IF;

    RETURN format('host=%s port=%s dbname=%s user=%s password=%s',
                  conn_record.host,
                  conn_record.port,
                  conn_record.dbname,
                  conn_record.username,
                  conn_record.password);
END;
$function$;

-- Refresh materialized view
CREATE OR REPLACE FUNCTION _wh.mv_refresh(target_schema text, view_name text)
RETURNS boolean
LANGUAGE plpgsql
AS $function$
DECLARE
    context JSONB;
BEGIN
    context := jsonb_build_object('schema', target_schema, 'view', view_name);

    BEGIN
        EXECUTE format('REFRESH MATERIALIZED VIEW CONCURRENTLY %I.%I', target_schema, view_name);
        PERFORM _wh.log_info('Refreshed materialized view: ' || target_schema || '.' || view_name, context);
        RETURN TRUE;
    EXCEPTION
        WHEN OTHERS THEN
            PERFORM _wh.log_error('Failed to refresh materialized view: ' || target_schema || '.' || view_name || ' - ' || SQLERRM, context);
            RETURN FALSE;
    END;
END;
$function$;

-- =============================================================================
-- LOGGING FUNCTIONS
-- =============================================================================

-- Info logging
CREATE OR REPLACE FUNCTION _wh.log_info(message text, context jsonb DEFAULT '{}'::jsonb)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
  BEGIN
      RAISE INFO '[WH][%] INFO: % %', clock_timestamp(), message, context;
  END;
  $function$
;

-- Error logging
CREATE OR REPLACE FUNCTION _wh.log_error(message text, context jsonb DEFAULT '{}'::jsonb)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
  BEGIN
      RAISE NOTICE '[WH][%] ERROR: % %', clock_timestamp(), message, context;
  END;
  $function$
;

-- Debug logging
CREATE OR REPLACE FUNCTION _wh.log_debug(message text, context jsonb DEFAULT '{}'::jsonb)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
  BEGIN
      -- Debug messages can be commented out in production
      RAISE DEBUG '[WH][%] DEBUG: % %', clock_timestamp(), message, context;
  END;
  $function$
;

-- =============================================================================
-- MV TEMPLATE OPERATIONS
-- =============================================================================

-- Create materialized view from template
CREATE OR REPLACE FUNCTION _wh.mv_create_from_template(
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
    view_name := _wh.mv_create_name(template_name, target_date);

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
        WHERE mv_templates.template_name = mv_create_from_template.template_name;

        IF NOT FOUND THEN
            PERFORM _wh.log_error('Template not found: ' || template_name, context);
            RETURN FALSE;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            PERFORM _wh.log_error('Error accessing template: ' || SQLERRM, context);
            RETURN FALSE;
    END;

    -- Get connection string
    BEGIN
        connection_string := _wh.util_get_tenant_connection_string(tenant_connection_name);
    EXCEPTION
        WHEN OTHERS THEN
            PERFORM _wh.log_error('Failed to get connection string: ' || SQLERRM, context);
            RETURN FALSE;
    END;

    full_mv_name := target_schema || '.' || view_name;

    -- Build the remote query by substituting variables
    remote_query := REPLACE(template_data.query_template, '{TARGET_DATE}', target_date::text);

    -- Build and execute the CREATE MATERIALIZED VIEW statement
    BEGIN
        create_sql := format('CREATE MATERIALIZED VIEW %I.%I AS SELECT * FROM dblink(%L, %L) AS t(%s)',
                           target_schema, view_name, connection_string, remote_query, template_data.column_definitions);

        EXECUTE create_sql;
        PERFORM _wh.log_info('Created materialized view: ' || full_mv_name, context);

        -- Create indexes using template
        IF template_data.indexes IS NOT NULL AND template_data.indexes != '' THEN
            DECLARE
                index_sql TEXT;
                final_index_sql TEXT;
            BEGIN
                -- Substitute placeholders in index definitions
                index_sql := REPLACE(template_data.indexes, '{SCHEMA}', target_schema);
                final_index_sql := REPLACE(index_sql, '{VIEW_NAME}', view_name);

                EXECUTE final_index_sql;
                PERFORM _wh.log_info('Created indexes for: ' || full_mv_name, context);
            EXCEPTION
                WHEN OTHERS THEN
                    PERFORM _wh.log_error('Failed to create indexes for: ' || full_mv_name || ' - ' || SQLERRM, context);
                    -- Don't fail the entire operation for index creation failures
            END;
        END IF;

        RETURN TRUE;
    EXCEPTION
        WHEN OTHERS THEN
            PERFORM _wh.log_error('Failed to create materialized view: ' || full_mv_name || ' - ' || SQLERRM, context);
            RETURN FALSE;
    END;
END;
$function$;

-- Update materialized view by template (main workhorse function)
CREATE OR REPLACE FUNCTION _wh.mv_update_by_template(
    template_name text,
    tenant_connection_name text,
    target_schema text,
    target_date date DEFAULT (NOW() AT TIME ZONE 'UTC')::DATE,
    allow_refresh_yesterday boolean DEFAULT true,
    update_union_view boolean DEFAULT true
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
    view_name := _wh.mv_create_name(template_name, target_date);
    full_mv_name := target_schema || '.' || view_name;
    yesterday_date := target_date - INTERVAL '1 day';

    -- Setup context for logging
    context := jsonb_build_object(
        'template', template_name,
        'tenant', tenant_connection_name,
        'schema', target_schema,
        'view_name', view_name,
        'target_date', target_date,
        'allow_refresh_yesterday', allow_refresh_yesterday,
        'update_union_view', update_union_view
    );

    PERFORM _wh.log_info('Starting MV update for template: ' || template_name || ' date: ' || target_date, context);

    -- Phase 1: Refresh yesterday's MV if requested and exists
    IF allow_refresh_yesterday THEN
        DECLARE
            yesterday_view_name TEXT;
        BEGIN
            yesterday_view_name := _wh.mv_create_name(template_name, yesterday_date);

            IF _wh.mv_does_exist(target_schema, yesterday_view_name) THEN
                PERFORM _wh.log_info('Refreshing yesterday MV: ' || yesterday_view_name, context);
                refresh_result := _wh.mv_refresh(target_schema, yesterday_view_name);
                IF NOT refresh_result THEN
                    PERFORM _wh.log_error('Failed to refresh yesterday MV: ' || yesterday_view_name, context);
                    -- Continue anyway - don't fail entire operation
                END IF;
            ELSE
                PERFORM _wh.log_info('Yesterday MV does not exist, skipping refresh: ' || yesterday_view_name, context);
            END IF;
        END;
    END IF;

    -- Phase 2: Handle target date MV
    target_mv_exists := _wh.mv_does_exist(target_schema, view_name);

    IF target_mv_exists THEN
        -- Refresh existing MV
        PERFORM _wh.log_info('Refreshing existing MV: ' || full_mv_name, context);
        refresh_result := _wh.mv_refresh(target_schema, view_name);
        IF NOT refresh_result THEN
            PERFORM _wh.log_error('Failed to refresh MV: ' || full_mv_name, context);
            RETURN FALSE;
        END IF;
    ELSE
        -- Create new MV
        PERFORM _wh.log_info('Creating new MV: ' || full_mv_name, context);
        refresh_result := _wh.mv_create_from_template(template_name, tenant_connection_name, target_schema, target_date);
        IF NOT refresh_result THEN
            PERFORM _wh.log_error('Failed to create MV: ' || full_mv_name, context);
            RETURN FALSE;
        END IF;

        -- Update union view since we created a new MV (if requested)
        IF update_union_view THEN
            PERFORM _wh.log_info('Updating union view after creating new MV: ' || full_mv_name, context);
            IF NOT _wh.union_view_update_tenant_by_template(template_name, target_schema) THEN
                PERFORM _wh.log_error('Failed to update union view after creating MV: ' || full_mv_name, context);
                -- Don't fail the main operation, just log the error
            ELSE
                PERFORM _wh.log_info('Successfully updated union view after creating MV: ' || full_mv_name, context);
            END IF;
        ELSE
            PERFORM _wh.log_info('Skipping union view update (update_union_view=false)', context);
        END IF;
    END IF;

    PERFORM _wh.log_info('Successfully completed MV update for template: ' || template_name || ' date: ' || target_date, context);
    RETURN TRUE;
EXCEPTION
    WHEN OTHERS THEN
        PERFORM _wh.log_error('Error in mv_update_by_template: ' || SQLERRM, context);
        RETURN FALSE;
END;
$function$;

-- Bulk update MVs for date range
CREATE OR REPLACE FUNCTION _wh.mv_update_window_by_template(
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
    IF NOT EXISTS (SELECT 1 FROM _wh.mv_templates WHERE mv_templates.template_name = mv_update_window_by_template.template_name) THEN
        RAISE EXCEPTION 'Template not found: %', template_name;
    END IF;

    -- Loop through each date in the range (inclusive)
    loop_date := start_date;
    WHILE loop_date <= end_date LOOP
        -- Call template-based function with allow_refresh_yesterday=false and update_union_view=false for bulk operations
        date_result := _wh.mv_update_by_template(
            template_name,
            tenant_connection_name,
            target_schema,
            loop_date,
            false,  -- allow_refresh_yesterday=false for bulk operations
            false   -- update_union_view=false for bulk operations (updated once at end)
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

        -- Pause to avoid overwhelming source database
        PERFORM pg_sleep(1);
    END LOOP;

    -- Update union view once at the end of the window operation
    IF success_count > 0 THEN
        PERFORM _wh.log_info('Window operation complete - updating union view for template: ' || template_name,
                             jsonb_build_object('schema', target_schema, 'success_count', success_count));
        IF NOT _wh.union_view_update_tenant_by_template(template_name, target_schema) THEN
            PERFORM _wh.log_error('Failed to update union view after window operation',
                                  jsonb_build_object('template', template_name, 'schema', target_schema));
            -- Don't fail the entire operation, just log the error
        ELSE
            PERFORM _wh.log_info('Successfully updated union view after window operation',
                                 jsonb_build_object('template', template_name, 'schema', target_schema));
        END IF;
    END IF;

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
            'template_name', template_name,
            'tenant', tenant_connection_name,
            'schema', target_schema,
            'start_date', start_date,
            'end_date', end_date,
            'success_count', success_count,
            'error_count', error_count,
            'duration_seconds', EXTRACT(EPOCH FROM (NOW() - start_time))
        );
END;
$function$;

-- =============================================================================
-- YEAR TABLE OPERATIONS
-- =============================================================================

-- Check views by template for a specific year
CREATE OR REPLACE FUNCTION _wh.year_table_check_views_by_template(
    template_name text,
    target_schema text,
    target_year integer
)
RETURNS jsonb
LANGUAGE plpgsql
AS $function$
DECLARE
    start_date DATE;
    end_date DATE;
    expected_days INTEGER;
    actual_mvs INTEGER;
    missing_dates TEXT[] := '{}';
    loop_date DATE;
    view_name TEXT;
    context JSONB;
BEGIN
    context := jsonb_build_object(
        'template', template_name,
        'schema', target_schema,
        'year', target_year
    );

    -- Calculate expected date range for the year
    start_date := (target_year || '-01-01')::DATE;
    end_date := (target_year || '-12-31')::DATE;
    expected_days := end_date - start_date + 1;

    -- Count actual MVs for the year
    SELECT COUNT(*)
    INTO actual_mvs
    FROM pg_matviews
    WHERE schemaname = target_schema
    AND matviewname LIKE template_name || '_' || target_year || '_%';

    -- Find missing dates if not all days are present
    IF actual_mvs < expected_days THEN
        loop_date := start_date;
        WHILE loop_date <= end_date LOOP
            view_name := _wh.mv_create_name(template_name, loop_date);

            IF NOT _wh.mv_does_exist(target_schema, view_name) THEN
                missing_dates := array_append(missing_dates, loop_date::text);
            END IF;

            loop_date := loop_date + INTERVAL '1 day';
        END LOOP;
    END IF;

    RETURN jsonb_build_object(
        'template_name', template_name,
        'schema', target_schema,
        'year', target_year,
        'expected_days', expected_days,
        'actual_mvs', actual_mvs,
        'missing_count', array_length(missing_dates, 1),
        'missing_dates', missing_dates,
        'complete', (actual_mvs = expected_days)
    );
END;
$function$;

-- Create combined yearly table from daily MVs
CREATE OR REPLACE FUNCTION _wh.year_table_create_from_template(
    template_name text,
    target_schema text,
    target_year integer
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
    template_data RECORD;
    yearly_table_name TEXT;
    full_table_name TEXT;
    mv_record RECORD;
    insert_sql TEXT;
    create_sql TEXT;
    index_sql TEXT;
    processed_count INTEGER := 0;
    total_records BIGINT := 0;
    start_time TIMESTAMP := NOW();
    context JSONB;
BEGIN
    yearly_table_name := template_name || '_' || target_year;
    full_table_name := target_schema || '.' || yearly_table_name;

    context := jsonb_build_object(
        'template', template_name,
        'schema', target_schema,
        'year', target_year,
        'yearly_table', yearly_table_name
    );

    PERFORM _wh.log_info('Starting yearly combination for: ' || full_table_name, context);

    -- Get template data
    SELECT * INTO template_data
    FROM _wh.mv_templates
    WHERE mv_templates.template_name = year_table_create_from_template.template_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Template not found: %', template_name;
    END IF;

    -- Check if yearly table already exists
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = target_schema AND tablename = yearly_table_name) THEN
        RAISE EXCEPTION 'Yearly table already exists: %', full_table_name;
    END IF;

    -- Start transaction for the combination process
    BEGIN
        -- Step 4: Recreate tenant union view excluding the year we're about to combine
        IF NOT _wh.union_view_update_tenant_by_template(template_name, target_schema, '_' || target_year::text || '_') THEN
            RAISE EXCEPTION 'Failed to recreate tenant union view with exclusions';
        END IF;
        PERFORM _wh.log_info('Recreated tenant union view excluding year: ' || target_year, context);

        -- Step 5: Create yearly table with same structure as template
        create_sql := format('CREATE TABLE %I.%I (%s)',
                           target_schema, yearly_table_name, template_data.column_definitions);
        EXECUTE create_sql;
        PERFORM _wh.log_info('Created yearly table structure: ' || full_table_name, context);

        -- Step 6: Process each daily MV and insert data
        FOR mv_record IN
            SELECT matviewname
            FROM pg_matviews
            WHERE schemaname = target_schema
            AND matviewname LIKE template_name || '_' || target_year || '_%'
            ORDER BY matviewname
        LOOP
            -- Insert data from MV to yearly table
            insert_sql := format('INSERT INTO %I.%I SELECT * FROM %I.%I',
                                target_schema, yearly_table_name,
                                target_schema, mv_record.matviewname);
            EXECUTE insert_sql;

            -- Get row count and add to total
            GET DIAGNOSTICS processed_count = ROW_COUNT;
            total_records := total_records + processed_count;

            PERFORM _wh.log_info('Processed MV: ' || mv_record.matviewname || ' (' || processed_count || ' records)', context);

            -- Drop the daily MV after successful data transfer
            EXECUTE format('DROP MATERIALIZED VIEW %I.%I', target_schema, mv_record.matviewname);
            PERFORM _wh.log_info('Dropped MV: ' || mv_record.matviewname, context);

            processed_count := processed_count + 1;
        END LOOP;

        -- Step 7: Create indexes on yearly table
        IF template_data.indexes IS NOT NULL AND template_data.indexes != '' THEN
            index_sql := REPLACE(template_data.indexes, '{SCHEMA}', target_schema);
            index_sql := REPLACE(index_sql, '{VIEW_NAME}', yearly_table_name);
            EXECUTE index_sql;
            PERFORM _wh.log_info('Created indexes for yearly table: ' || full_table_name, context);
        END IF;

        -- Step 11: Recreate tenant union view (include yearly table + remaining MVs)
        IF NOT _wh.union_view_update_tenant_by_template(template_name, target_schema) THEN
            RAISE EXCEPTION 'Failed to recreate tenant union view';
        END IF;
        PERFORM _wh.log_info('Recreated tenant union view with yearly table', context);

        PERFORM _wh.log_info('Successfully completed yearly combination',
                             jsonb_build_object('yearly_table', full_table_name, 'processed_mvs', processed_count, 'total_records', total_records));

        RETURN jsonb_build_object(
            'success', true,
            'template_name', template_name,
            'schema', target_schema,
            'target_year', target_year,
            'yearly_table', yearly_table_name,
            'processed_mvs', processed_count,
            'total_records', total_records,
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

-- =============================================================================
-- UNION VIEW MANAGEMENT
-- =============================================================================

-- Update tenant union view by template
CREATE OR REPLACE FUNCTION _wh.union_view_update_tenant_by_template(
    template_name text,
    target_schema text,
    exclude_pattern text DEFAULT '__NEVER_MATCH_STUB__'
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
    mv_record RECORD;
    yearly_record RECORD;
    union_parts TEXT[] := '{}';
    final_query TEXT;
    view_name TEXT;
    context JSONB;
BEGIN
    view_name := template_name;
    context := jsonb_build_object(
        'template', template_name,
        'schema', target_schema,
        'view_name', view_name,
        'exclude_pattern', exclude_pattern
    );

    PERFORM _wh.log_info('Updating tenant union view: ' || target_schema || '.' || view_name, context);

    -- Add daily materialized views (excluding pattern if specified)
    FOR mv_record IN
        SELECT matviewname
        FROM pg_matviews
        WHERE schemaname = target_schema
        AND matviewname LIKE template_name || '_%'
        AND matviewname NOT LIKE '%' || exclude_pattern || '%'
        ORDER BY matviewname
    LOOP
        union_parts := array_append(union_parts, format('SELECT * FROM %I.%I', target_schema, mv_record.matviewname));
    END LOOP;

    -- Add yearly tables
    FOR yearly_record IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = target_schema
        AND tablename ~ ('^' || template_name || '_\d{4}$')
        ORDER BY tablename
    LOOP
        union_parts := array_append(union_parts, format('SELECT * FROM %I.%I', target_schema, yearly_record.tablename));
    END LOOP;

    -- Create the union view
    IF array_length(union_parts, 1) > 0 THEN
        final_query := format('CREATE OR REPLACE VIEW %I.%I AS %s',
            target_schema, view_name, array_to_string(union_parts, ' UNION ALL '));
        EXECUTE final_query;

        PERFORM _wh.log_info('Successfully updated tenant union view: ' || target_schema || '.' || view_name,
                             jsonb_build_object('template', template_name, 'union_parts', array_length(union_parts, 1)));
        RETURN TRUE;
    ELSE
        PERFORM _wh.log_error('No materialized views or yearly tables found for union view',
                              jsonb_build_object('template', template_name, 'schema', target_schema));
        RETURN FALSE;
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        PERFORM _wh.log_error('Failed to update tenant union view: ' || SQLERRM, context);
        RETURN FALSE;
END;
$function$;

-- Update public union view by template
CREATE OR REPLACE FUNCTION _wh.union_view_update_public_by_template(template_name text)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
    schema_record RECORD;
    union_parts TEXT[] := '{}';
    final_query TEXT;
    context JSONB;
BEGIN
    context := jsonb_build_object('template', template_name, 'operation', 'update_public_view');

    PERFORM _wh.log_info('Updating public union view: public.' || template_name, context);

    -- Find all schemas that have this template view
    FOR schema_record IN
        SELECT DISTINCT schemaname
        FROM pg_views
        WHERE viewname = template_name
        AND schemaname NOT IN ('public', '_wh')
        ORDER BY schemaname
    LOOP
        union_parts := array_append(union_parts, format(
            'SELECT *, %L as schema_name FROM %I.%I',
            schema_record.schemaname, schema_record.schemaname, template_name
        ));
    END LOOP;

    -- Create the public union view
    IF array_length(union_parts, 1) > 0 THEN
        final_query := format('CREATE OR REPLACE VIEW public.%I AS %s',
            template_name, array_to_string(union_parts, ' UNION ALL '));
        EXECUTE final_query;

        PERFORM _wh.log_info('Successfully updated public union view: public.' || template_name,
                             jsonb_build_object('template', template_name, 'tenant_count', array_length(union_parts, 1)));
        RETURN TRUE;
    ELSE
        PERFORM _wh.log_error('No tenant views found for public union view',
                              jsonb_build_object('template', template_name));
        RETURN FALSE;
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        PERFORM _wh.log_error('Failed to update public union view: ' || template_name || ' - ' || SQLERRM, context);
        RETURN FALSE;
END;
$function$;

-- =============================================================================
-- CRON WRAPPER FUNCTIONS
-- These functions provide clean interfaces for common cron job operations
-- =============================================================================

-- Daily refresh wrapper for cron jobs
CREATE OR REPLACE FUNCTION _wh.cron_refresh_today(
    template_name text,
    tenant_connection_name text,
    target_schema text
)
RETURNS boolean
LANGUAGE sql
SECURITY DEFINER
AS $function$
    SELECT _wh.mv_update_by_template($1, $2, $3);
$function$;

-- Recent days refresh wrapper for cron jobs
CREATE OR REPLACE FUNCTION _wh.cron_refresh_recent(
    template_name text,
    tenant_connection_name text,
    target_schema text,
    days_back integer
)
RETURNS jsonb
LANGUAGE sql
SECURITY DEFINER
AS $function$
    SELECT _wh.mv_update_window_by_template($1, $2, $3, _wh.util_current_date_utc(-$4), _wh.util_current_date_utc(-1));
$function$;

-- Last year combination wrapper for cron jobs
CREATE OR REPLACE FUNCTION _wh.cron_combine_last_year(
    template_name text,
    target_schema text
)
RETURNS jsonb
LANGUAGE sql
SECURITY DEFINER
AS $function$
    SELECT _wh.year_table_create_from_template($1, $2, EXTRACT(YEAR FROM _wh.util_current_date_utc(-365))::integer);
$function$;

-- =============================================================================
-- SCHEMA MODIFICATION FUNCTIONS
-- These functions help with adding/modifying columns across the warehouse
-- =============================================================================

-- Drop public union view for a template
CREATE OR REPLACE FUNCTION _wh.union_view_drop_public_by_template(template_name text)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
    context JSONB;
BEGIN
    context := jsonb_build_object('template', template_name, 'operation', 'drop_public_view');

    EXECUTE format('DROP VIEW IF EXISTS public.%I CASCADE', template_name);
    PERFORM _wh.log_info('Dropped public view: ' || template_name, context);
    RETURN TRUE;
EXCEPTION
    WHEN OTHERS THEN
        PERFORM _wh.log_error('Failed to drop public view: ' || template_name || ' - ' || SQLERRM, context);
        RETURN FALSE;
END;
$function$;

-- Drop all tenant union views for a template
CREATE OR REPLACE FUNCTION _wh.union_view_drop_all_by_template(template_name text)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
    schema_record RECORD;
    results jsonb := '[]';
    success_count integer := 0;
    error_count integer := 0;
    context JSONB;
BEGIN
    context := jsonb_build_object('template', template_name, 'operation', 'drop_tenant_union_views');

    FOR schema_record IN
        SELECT DISTINCT schemaname
        FROM pg_views
        WHERE viewname = template_name
        AND schemaname NOT IN ('public', '_wh')
    LOOP
        BEGIN
            EXECUTE format('DROP VIEW IF EXISTS %I.%I CASCADE', schema_record.schemaname, template_name);
            success_count := success_count + 1;
            PERFORM _wh.log_info('Dropped tenant union view: ' || schema_record.schemaname || '.' || template_name, context);
        EXCEPTION
            WHEN OTHERS THEN
                error_count := error_count + 1;
                PERFORM _wh.log_error('Failed to drop tenant union view: ' || schema_record.schemaname || '.' || template_name || ' - ' || SQLERRM, context);
        END;
    END LOOP;

    PERFORM _wh.log_info('Completed dropping tenant union views',
                         jsonb_build_object('template', template_name, 'success_count', success_count, 'error_count', error_count));

    RETURN jsonb_build_object('success_count', success_count, 'error_count', error_count);
END;
$function$;

-- Add column to all yearly tables for a template
CREATE OR REPLACE FUNCTION _wh.year_table_add_column_by_template(
    template_name text,
    column_name text,
    column_type text,
    default_value text DEFAULT NULL,
    create_index boolean DEFAULT true
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
    table_record RECORD;
    success_count integer := 0;
    error_count integer := 0;
    alter_sql text;
    index_sql text;
    context JSONB;
BEGIN
    context := jsonb_build_object(
        'template', template_name,
        'operation', 'add_column_to_yearly_tables',
        'column_name', column_name,
        'column_type', column_type,
        'default_value', default_value,
        'create_index', create_index
    );

    FOR table_record IN
        SELECT schemaname, tablename
        FROM pg_tables
        WHERE tablename ~ ('^' || template_name || '_\d{4}$')
        AND schemaname NOT IN ('information_schema', 'pg_catalog', '_wh', 'public')
        ORDER BY schemaname, tablename
    LOOP
        BEGIN
            -- Build ALTER TABLE statement
            alter_sql := format('ALTER TABLE %I.%I ADD COLUMN IF NOT EXISTS %I %s',
                               table_record.schemaname, table_record.tablename, column_name, column_type);

            IF default_value IS NOT NULL THEN
                alter_sql := alter_sql || format(' DEFAULT %L', default_value);
            END IF;

            EXECUTE alter_sql;

            -- Create index if requested
            IF create_index THEN
                index_sql := format('CREATE INDEX IF NOT EXISTS idx_%s_%s_%s ON %I.%I (%I)',
                                   table_record.schemaname, table_record.tablename, column_name,
                                   table_record.schemaname, table_record.tablename, column_name);
                EXECUTE index_sql;
            END IF;

            success_count := success_count + 1;
            PERFORM _wh.log_info('Added column to yearly table: ' || table_record.schemaname || '.' || table_record.tablename, context);
        EXCEPTION
            WHEN OTHERS THEN
                error_count := error_count + 1;
                PERFORM _wh.log_error('Failed to add column to: ' || table_record.schemaname || '.' || table_record.tablename || ' - ' || SQLERRM, context);
        END;
    END LOOP;

    PERFORM _wh.log_info('Completed adding column to yearly tables',
                         jsonb_build_object('template', template_name, 'column_name', column_name, 'success_count', success_count, 'error_count', error_count));

    RETURN jsonb_build_object(
        'success_count', success_count,
        'error_count', error_count,
        'column_name', column_name,
        'column_type', column_type
    );
END;
$function$;

-- Drop all current year MVs for a template
CREATE OR REPLACE FUNCTION _wh.mv_drop_current_year_by_template(
    template_name text,
    target_year integer DEFAULT EXTRACT(YEAR FROM _wh.util_current_date_utc())::integer
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
    mv_record RECORD;
    success_count integer := 0;
    error_count integer := 0;
    context JSONB;
BEGIN
    context := jsonb_build_object(
        'template', template_name,
        'operation', 'drop_current_year_mvs',
        'target_year', target_year
    );

    FOR mv_record IN
        SELECT schemaname, matviewname
        FROM pg_matviews
        WHERE matviewname LIKE template_name || '_' || target_year::text || '_%'
        AND schemaname NOT IN ('information_schema', 'pg_catalog', '_wh', 'public')
        ORDER BY schemaname, matviewname
    LOOP
        BEGIN
            EXECUTE format('DROP MATERIALIZED VIEW %I.%I', mv_record.schemaname, mv_record.matviewname);
            success_count := success_count + 1;
            PERFORM _wh.log_info('Dropped current year MV: ' || mv_record.schemaname || '.' || mv_record.matviewname, context);
        EXCEPTION
            WHEN OTHERS THEN
                error_count := error_count + 1;
                PERFORM _wh.log_error('Failed to drop MV: ' || mv_record.schemaname || '.' || mv_record.matviewname || ' - ' || SQLERRM, context);
        END;
    END LOOP;

    PERFORM _wh.log_info('Completed dropping current year MVs',
                         jsonb_build_object('template', template_name, 'target_year', target_year, 'success_count', success_count, 'error_count', error_count));

    RETURN jsonb_build_object(
        'success_count', success_count,
        'error_count', error_count,
        'target_year', target_year
    );
END;
$function$;


-- =============================================================================
-- GRANT PERMISSIONS
-- =============================================================================

-- Grant execute permissions for all functions to whadmin
GRANT EXECUTE ON FUNCTION _wh.mv_create_name(text, date) TO whadmin;
GRANT EXECUTE ON FUNCTION _wh.util_current_date_utc(integer) TO whadmin;
GRANT EXECUTE ON FUNCTION _wh.mv_does_exist(text, text) TO whadmin;
GRANT EXECUTE ON FUNCTION _wh.util_get_tenant_connection_string(text) TO whadmin;
GRANT EXECUTE ON FUNCTION _wh.mv_refresh(text, text) TO whadmin;
GRANT EXECUTE ON FUNCTION _wh.log_info(text, jsonb) TO whadmin;
GRANT EXECUTE ON FUNCTION _wh.log_error(text, jsonb) TO whadmin;
GRANT EXECUTE ON FUNCTION _wh.log_debug(text, jsonb) TO whadmin;
GRANT EXECUTE ON FUNCTION _wh.mv_create_from_template(text, text, text, date) TO whadmin;
GRANT EXECUTE ON FUNCTION _wh.mv_update_by_template(text, text, text, date, boolean, boolean) TO whadmin;
GRANT EXECUTE ON FUNCTION _wh.mv_update_window_by_template(text, text, text, date, date) TO whadmin;
GRANT EXECUTE ON FUNCTION _wh.year_table_check_views_by_template(text, text, integer) TO whadmin;
GRANT EXECUTE ON FUNCTION _wh.year_table_create_from_template(text, text, integer) TO whadmin;
GRANT EXECUTE ON FUNCTION _wh.union_view_update_tenant_by_template(text, text, text) TO whadmin;
GRANT EXECUTE ON FUNCTION _wh.union_view_update_public_by_template(text) TO whadmin;
GRANT EXECUTE ON FUNCTION _wh.union_view_drop_public_by_template(text) TO whadmin;
GRANT EXECUTE ON FUNCTION _wh.union_view_drop_all_by_template(text) TO whadmin;
GRANT EXECUTE ON FUNCTION _wh.year_table_add_column_by_template(text, text, text, text, boolean) TO whadmin;
GRANT EXECUTE ON FUNCTION _wh.mv_drop_current_year_by_template(text, integer) TO whadmin;

-- Grant execute permissions for cron wrapper functions
GRANT EXECUTE ON FUNCTION _wh.cron_refresh_today(text, text, text) TO whadmin;
GRANT EXECUTE ON FUNCTION _wh.cron_refresh_recent(text, text, text, integer) TO whadmin;
GRANT EXECUTE ON FUNCTION _wh.cron_combine_last_year(text, text) TO whadmin;


-- =============================================================================
-- CONNECTION MANAGEMENT
-- =============================================================================
-- Tenant connections managed via direct SQL INSERT/UPDATE on _wh.tenant_connections
-- No specific functions needed - use standard SQL operations