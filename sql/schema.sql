CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY,
  pipeline TEXT NOT NULL CHECK (pipeline IN ('mongo','mapreduce','pig','hive')),
  dataset_manifest JSONB NOT NULL,
  batch_size BIGINT NOT NULL CHECK (batch_size > 0),
  batch_count BIGINT NOT NULL CHECK (batch_count >= 0),
  total_records BIGINT NOT NULL CHECK (total_records >= 0),
  malformed_records BIGINT NOT NULL CHECK (malformed_records >= 0),
  average_batch_size NUMERIC(20,4),
  started_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ,
  runtime_seconds NUMERIC(20,6),
  status TEXT NOT NULL CHECK (status IN ('loading','success','failed')),
  error_message TEXT
);
CREATE TABLE IF NOT EXISTS batches (
  run_id TEXT NOT NULL REFERENCES runs(run_id),
  batch_id BIGINT NOT NULL CHECK (batch_id >= 1),
  source_file TEXT NOT NULL,
  input_count BIGINT NOT NULL CHECK (input_count > 0),
  PRIMARY KEY (run_id, batch_id)
);
CREATE TABLE IF NOT EXISTS q1_daily_traffic (
  run_id TEXT NOT NULL REFERENCES runs(run_id),
  pipeline TEXT NOT NULL,
  batch_id BIGINT NOT NULL,
  executed_at TIMESTAMPTZ NOT NULL,
  log_date DATE NOT NULL,
  status_code INTEGER NOT NULL,
  request_count BIGINT NOT NULL,
  total_bytes BIGINT NOT NULL,
  PRIMARY KEY (run_id,log_date,status_code)
);
CREATE TABLE IF NOT EXISTS q2_top_resources (
  run_id TEXT NOT NULL REFERENCES runs(run_id),
  pipeline TEXT NOT NULL,
  batch_id BIGINT NOT NULL,
  executed_at TIMESTAMPTZ NOT NULL,
  resource_path TEXT NOT NULL,
  request_count BIGINT NOT NULL,
  total_bytes BIGINT NOT NULL,
  distinct_host_count BIGINT NOT NULL,
  PRIMARY KEY (run_id,resource_path)
);
CREATE TABLE IF NOT EXISTS q3_hourly_errors (
  run_id TEXT NOT NULL REFERENCES runs(run_id),
  pipeline TEXT NOT NULL,
  batch_id BIGINT NOT NULL,
  executed_at TIMESTAMPTZ NOT NULL,
  log_date DATE NOT NULL,
  log_hour INTEGER NOT NULL CHECK (log_hour BETWEEN 0 AND 23),
  error_request_count BIGINT NOT NULL,
  total_request_count BIGINT NOT NULL,
  error_rate NUMERIC(20,12) NOT NULL,
  distinct_error_hosts BIGINT NOT NULL,
  PRIMARY KEY (run_id,log_date,log_hour)
);
CREATE INDEX IF NOT EXISTS runs_success_idx ON runs(pipeline, finished_at DESC) WHERE status='success';
