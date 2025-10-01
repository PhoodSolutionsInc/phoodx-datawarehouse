-- pg_cron Job Setup Script
--
-- Prerequisites:
-- 1. pg_cron extension must be installed and configured
-- 2. Connect as whadmin user (must have cron permissions)
-- 3. Update tenant names, schedules, and templates as needed
--
-- Usage:
--   psql -h your-rds-endpoint.amazonaws.com -U whadmin -d postgres -f cron.sql

-- =============================================================================
-- GRANT CRON PERMISSIONS (run as postgres superuser first)
-- =============================================================================
-- Run these commands as postgres user before running this script:
--
-- GRANT USAGE ON SCHEMA cron TO whadmin;
-- GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA cron TO whadmin;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA cron TO whadmin;
-- GRANT EXECUTE ON FUNCTION cron.schedule(text, text, text) TO whadmin;
-- GRANT EXECUTE ON FUNCTION cron.unschedule(text) TO whadmin;

-- =============================================================================
-- DAILY MV UPDATE JOBS (STAGGERED SCHEDULE)
-- These generally run quite quick, so we can space them out every minute, or 2 minutes.
-- Check for tenants that might be slow to update though.  They might need to be less
-- frequent than hourly.  eg.  wfm.
-- =============================================================================

-- LandB: Every 1 hour at :10
SELECT cron.schedule(
    'foodlogstats_update_landb',
    '10 * * * *',
    'SELECT _wh.update_mv_by_template(''foodlogstats'', ''landb-prod'', ''landb'', _wh.current_date_utc());'
);

-- MM: Every 1 hour at :11
SELECT cron.schedule(
    'foodlogstats_update_mm',
    '11 * * * *',
    'SELECT _wh.update_mv_by_template(''foodlogstats'', ''mm-prod'', ''mm'', _wh.current_date_utc());'
);


  -- =============================================================================
  -- NIGHTLY 2-WEEK MV REFRESH JOBS
  -- These refresh the last 2 weeks of MVs to catch any late-arriving data
  -- =============================================================================

  -- LandB: Daily at 3:30 AM (Central) That's 8:30 AM UTC
  SELECT cron.schedule(
      'landb_2week_refresh',
      '30 8 * * *',
      'SELECT _wh.update_mv_window_by_template(''foodlogstats'', ''landb-prod'', ''landb'', _wh.current_date_utc() - INTERVAL ''14 days'', _wh.current_date_utc() - INTERVAL ''1 day'');'
  );

  -- LandB: Daily at 3:32 AM (Central) That's 8:32 AM UTC
  SELECT cron.schedule(
      'mm_2week_refresh',
      '32 8 * * *',
      'SELECT _wh.update_mv_window_by_template(''foodlogstats'', ''mm-prod'', ''mm'', _wh.current_date_utc() - INTERVAL ''14 days'', _wh.current_date_utc() - INTERVAL ''1 day'');'
  );


-- =============================================================================
-- YEARLY COMBINATION JOBS (ANNUAL - FEBRUARY)
-- These jobs are much more expensive, and run the risk of table locks.
-- They need to be run during tenant off-hours, and they need to be staggered
-- to avoid overloading the DB.  LandB takes about 3 minutes to run. MM takes less than 2 minutes.
-- =============================================================================

-- Run yearly combinations in February after year-end data is stable
-- These jobs combine all daily MVs from the previous year into yearly tables

-- LandB: February 15th at 2:00 AM (CENTRAL). That's 7:00 AM UTC
SELECT cron.schedule(
    'yearly_combination_landb',
    '0 7 15 2 *',
    'SELECT _wh.create_combined_table_from_template_by_year(''foodlogstats'', ''landb'', EXTRACT(YEAR FROM _wh.current_date_utc() - INTERVAL ''1 year'')::integer);'
);

-- MM: February 15th at 2:10 AM (CENTRAL) 7:10 AM UTC
SELECT cron.schedule(
    'yearly_combination_mm',
    '10 7 15 2 *',
    'SELECT _wh.create_combined_table_from_template_by_year(''foodlogstats'', ''mm'', EXTRACT(YEAR FROM _wh.current_date_utc() - INTERVAL ''1 year'')::integer);'
);


-- =============================================================================
-- MAINTENANCE JOBS
-- =============================================================================

-- Clean up old cron job run details (keep last 30 days)
SELECT cron.schedule(
    'cleanup_cron_history',
    '0 3 * * 0',  -- Weekly on Sunday at 3 AM
    'DELETE FROM cron.job_run_details WHERE start_time < NOW() - INTERVAL ''30 days'';'
);

-- Database maintenance (analyze tables weekly)
SELECT cron.schedule(
    'weekly_analyze',
    '0 4 * * 0',  -- Weekly on Sunday at 4 AM
    'ANALYZE;'
);

-- =============================================================================
-- JOB MANAGEMENT QUERIES
-- =============================================================================

-- View all scheduled jobs
-- SELECT jobid, jobname, schedule, command, active FROM cron.job ORDER BY jobname;

-- View recent job execution history
-- SELECT j.jobname, r.status, r.start_time, r.end_time, r.return_message
-- FROM cron.job j
-- LEFT JOIN cron.job_run_details r ON j.jobid = r.jobid
-- ORDER BY r.start_time DESC LIMIT 20;

-- Monitor failed jobs from last 24 hours
-- SELECT j.jobname, r.status, r.return_message, r.start_time
-- FROM cron.job j
-- JOIN cron.job_run_details r ON j.jobid = r.jobid
-- WHERE r.status != 'succeeded'
--   AND r.start_time > NOW() - INTERVAL '24 hours'
-- ORDER BY r.start_time DESC;

-- Unschedule a job (example)
-- SELECT cron.unschedule('job_name_here');

-- =============================================================================
-- SCALING NOTES
-- =============================================================================

-- Adding New Tenants:
-- 1. Add daily MV update job with unique minute offset
-- 2. Add union view update job with staggered timing
-- 3. Add yearly combination job with unique hour
-- 4. Test job execution and monitor for conflicts

-- Adding New Templates:
-- 1. Load template into _wh.mv_templates
-- 2. Create similar job pattern for new template
-- 3. Use different job names (e.g., tenant_a_inventory_update)
-- 4. Consider different schedules based on data volume

-- Performance Tuning:
-- 1. Monitor job execution times in cron.job_run_details
-- 2. Adjust schedules if jobs overlap or take too long
-- 3. Consider splitting large tenants into multiple jobs
-- 4. Scale cron schedule frequency based on business needs

NOTICE 'pg_cron jobs scheduled successfully!';
NOTICE 'Run: SELECT jobid, jobname, schedule, active FROM cron.job ORDER BY jobname;';
NOTICE 'To monitor job status and execution history.';