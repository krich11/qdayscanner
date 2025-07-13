-- High-Performance PostgreSQL Optimizations for Bitcoin P2PK Scanner
-- Run this script as a superuser (postgres) to apply system-level optimizations

-- Disable synchronous commits for maximum speed (data can be lost on crash)
ALTER SYSTEM SET synchronous_commit = 'off';

-- Increase WAL buffers for better write performance
ALTER SYSTEM SET wal_buffers = '256MB';

-- Optimize checkpoint settings for write-heavy workloads
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET checkpoint_timeout = '30min';
ALTER SYSTEM SET max_wal_size = '8GB';
ALTER SYSTEM SET min_wal_size = '2GB';

-- Optimize for bulk inserts
ALTER SYSTEM SET wal_writer_delay = '200ms';
ALTER SYSTEM SET commit_delay = 1000;
ALTER SYSTEM SET commit_siblings = 5;

-- Increase work memory for better performance
ALTER SYSTEM SET work_mem = '4GB';
ALTER SYSTEM SET maintenance_work_mem = '8GB';

-- Optimize for concurrent connections
ALTER SYSTEM SET max_connections = 200;
ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';

-- Disable expensive features during bulk load
ALTER SYSTEM SET autovacuum = 'off';
ALTER SYSTEM SET fsync = 'off';  -- DANGEROUS - only for bulk loading!
ALTER SYSTEM SET full_page_writes = 'off';  -- DANGEROUS - only for bulk loading!

-- Optimize for high-throughput inserts
ALTER SYSTEM SET random_page_cost = 1.1;
ALTER SYSTEM SET effective_io_concurrency = 200;

-- Increase connection limits
ALTER SYSTEM SET max_worker_processes = 32;
ALTER SYSTEM SET max_parallel_workers = 32;
ALTER SYSTEM SET max_parallel_workers_per_gather = 16;

-- Reload configuration
SELECT pg_reload_conf();

-- Show current settings
SHOW synchronous_commit;
SHOW wal_buffers;
SHOW work_mem;
SHOW fsync;
SHOW full_page_writes; 