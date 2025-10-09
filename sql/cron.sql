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
    'SELECT _wh.cron_refresh_today(''foodlogstats'', ''landb-prod'', ''landb'');'
);

-- MM: Every 1 hour at :11
SELECT cron.schedule(
    'foodlogstats_update_mm',
    '11 * * * *',
    'SELECT _wh.cron_refresh_today(''foodlogstats'', ''mm-prod'', ''mm'');'
);

-- AF: Every 1 hour at :12
SELECT cron.schedule(
    'foodlogstats_update_af',
    '12 * * * *',
    'SELECT _wh.cron_refresh_today(''foodlogstats'', ''af-prod'', ''af'');'
);

-- RB: Every 1 hour at :13
SELECT cron.schedule(
    'foodlogstats_update_rb',
    '13 * * * *',
    'SELECT _wh.cron_refresh_today(''foodlogstats'', ''rb-prod'', ''rb'');'
);

-- WFMMK: Every 1 hour at :14
SELECT cron.schedule(
    'foodlogstats_update_wfmmk',
    '14 * * * *',
    'SELECT _wh.cron_refresh_today(''foodlogstats'', ''wfmmk-prod'', ''wfmmk'');'
);

-- TK: Every 1 hour at :15
SELECT cron.schedule(
    'foodlogstats_update_tk',
    '15 * * * *',
    'SELECT _wh.cron_refresh_today(''foodlogstats'', ''tk-prod'', ''tk'');'
);

-- WFM: Every 1 hour at :16
SELECT cron.schedule(
    'foodlogstats_update_wfm',
    '16 * * * *',
    'SELECT _wh.cron_refresh_today(''foodlogstats'', ''wfm-prod'', ''wfm'');'
);


  -- =============================================================================
  -- NIGHTLY 2-WEEK MV REFRESH JOBS
  -- These refresh the last 2 weeks of MVs to catch any late-arriving data
  -- =============================================================================

  -- LandB: Daily at 3:30 AM (Central) That's 8:30 AM UTC
  SELECT cron.schedule(
      '2week_refresh_landb',
      '30 8 * * *',
      'SELECT _wh.cron_refresh_recent(''foodlogstats'', ''landb-prod'', ''landb'', 14);'
  );

  -- MM: Daily at 3:32 AM (Central) That's 8:32 AM UTC
  SELECT cron.schedule(
      '2week_refresh_mm',
      '32 8 * * *',
      'SELECT _wh.cron_refresh_recent(''foodlogstats'', ''mm-prod'', ''mm'', 14);'
  );

  -- AF: Daily at 3:34 AM (Central) That's 8:34 AM UTC
  SELECT cron.schedule(
      '2week_refresh_af',
      '34 8 * * *',
      'SELECT _wh.cron_refresh_recent(''foodlogstats'', ''af-prod'', ''af'', 14);'
  );

  -- RB: Daily at 3:36 AM (Central) That's 8:36 AM UTC
  SELECT cron.schedule(
      '2week_refresh_rb',
      '36 8 * * *',
      'SELECT _wh.cron_refresh_recent(''foodlogstats'', ''rb-prod'', ''rb'', 14);'
  );

  -- WFMMK: Daily at 3:38 AM (Central) That's 8:38 AM UTC
  SELECT cron.schedule(
      '2week_refresh_wfmmk',
      '38 8 * * *',
      'SELECT _wh.cron_refresh_recent(''foodlogstats'', ''wfmmk-prod'', ''wfmmk'', 14);'
  );

  -- TK: Daily at 3:40 AM (Central) That's 8:40 AM UTC
  SELECT cron.schedule(
      '2week_refresh_tk',
      '40 8 * * *',
      'SELECT _wh.cron_refresh_recent(''foodlogstats'', ''tk-prod'', ''tk'', 14);'
  );

  -- WFM: Daily at 3:42 AM (Central) That's 8:42 AM UTC
  SELECT cron.schedule(
      '2week_refresh_wfm',
      '42 8 * * *',
      'SELECT _wh.cron_refresh_recent(''foodlogstats'', ''wfm-prod'', ''wfm'', 14);'
  );




-- =============================================================================
-- YEARLY COMBINATION JOBS (ANNUAL - FEBRUARY)
-- These jobs are much more expensive, and run the risk of table locks.
-- They need to be run during tenant off-hours, and they need to be staggered
-- to avoid overloading the DB.  LandB takes about 3 minutes to run. MM takes less than 2 minutes.
-- =============================================================================

-- Run yearly combinations in February after year-end data is stable
-- These jobs combine all daily MVs from the previous year into yearly tables

-- LandB: February 15th at 2:00 AM (CENTRAL). That's 7:00 AM UTC  (largish data set. leave room)
SELECT cron.schedule(
    'yearly_combination_landb',
    '00 7 15 2 *',
    'SELECT _wh.cron_combine_last_year(''foodlogstats'', ''landb'');'
);

-- MM: February 15th at 2:15 AM (CENTRAL) 7:15 AM UTC
SELECT cron.schedule(
    'yearly_combination_mm',
    '15 7 15 2 *',
    'SELECT _wh.cron_combine_last_year(''foodlogstats'', ''mm'');'
);

-- AF: February 15th at 2:20 AM (CENTRAL) 7:20 AM UTC
SELECT cron.schedule(
    'yearly_combination_af',
    '20 7 15 2 *',
    'SELECT _wh.cron_combine_last_year(''foodlogstats'', ''af'');'
);

-- RB: February 15th at 2:25 AM (CENTRAL) 7:25 AM UTC
SELECT cron.schedule(
    'yearly_combination_rb',
    '25 7 15 2 *',
    'SELECT _wh.cron_combine_last_year(''foodlogstats'', ''rb'');'
);

-- WFMMK: February 15th at 2:30 AM (CENTRAL) 7:30 AM UTC
SELECT cron.schedule(
    'yearly_combination_wfmmk',
    '30 7 15 2 *',
    'SELECT _wh.cron_combine_last_year(''foodlogstats'', ''wfmmk'');'
);

-- TK: February 15th at 2:35 AM (CENTRAL) 7:35 AM UTC
SELECT cron.schedule(
    'yearly_combination_tk',
    '35 7 15 2 *',
    'SELECT _wh.cron_combine_last_year(''foodlogstats'', ''tk'');'
);

-- WFM: February 15th at 2:40 AM (CENTRAL) 7:40 AM UTC (Very large data set. Leave room)
SELECT cron.schedule(
    'yearly_combination_wfm',
    '40 7 15 2 *',
    'SELECT _wh.cron_combine_last_year(''foodlogstats'', ''wfm'');'
);


-- =============================================================================
-- MAINTENANCE JOBS
-- =============================================================================

-- Clean up old cron job run details (keep last 30 days)
SELECT cron.schedule(
    'admin_cleanup_cron_history',
    '0 9 * * 0',  -- Weekly on Sunday at 4 AM (CENTRAL) 9 AM UTC
    'DELETE FROM cron.job_run_details WHERE start_time < NOW() - INTERVAL ''30 days'';'
);

-- Database maintenance (analyze tables weekly)
SELECT cron.schedule(
    'admin_weekly_analyze',
    '10 9 * * 0',  -- Weekly on Sunday at 4:10 AM (CENTRAL) 9:10 AM UTC
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