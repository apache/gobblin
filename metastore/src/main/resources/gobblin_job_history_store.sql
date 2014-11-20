CREATE TABLE IF NOT EXISTS gobblin_job_executions (
	job_name VARCHAR(128) NOT NULL,
	job_id VARCHAR(128) NOT NULL,
	start_time TIMESTAMP,
	end_time TIMESTAMP,
	duration BIGINT(21),
	state ENUM('PENDING', 'RUNNING', 'SUCCESSFUL', 'COMMITTED', 'FAILED', 'CANCELLED'),
	launched_tasks INT,
	completed_tasks INT,
	created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	last_modified_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	PRIMARY KEY (job_id),
	INDEX (job_name),
	INDEX (state)
);

CREATE TABLE IF NOT EXISTS gobblin_task_executions (
	task_id VARCHAR(128) NOT NULL,
	job_id VARCHAR(128) NOT NULL,
	start_time TIMESTAMP,
	end_time TIMESTAMP,
	duration BIGINT(21),
	state ENUM('PENDING', 'RUNNING', 'SUCCESSFUL', 'COMMITTED', 'FAILED', 'CANCELLED'),
	low_watermark BIGINT(21),
	high_watermark BIGINT(21),
	table_namespace VARCHAR(128),
	table_name VARCHAR(128),
	table_type ENUM('SNAPSHOT_ONLY', 'SNAPSHOT_APPEND', 'APPEND_ONLY'),
	created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	last_modified_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	PRIMARY KEY (task_id),
	FOREIGN KEY (job_id) 
	REFERENCES gobblin_job_executions(job_id) 
	ON DELETE CASCADE,
	INDEX (state),
	INDEX (table_namespace),
	INDEX (table_name),
	INDEX (table_type)
);

CREATE TABLE IF NOT EXISTS gobblin_job_metrics (
	metric_id BIGINT(21) NOT NULL AUTO_INCREMENT,
	job_id VARCHAR(128) NOT NULL,
	metric_group VARCHAR(128) NOT NULL,
	metric_name VARCHAR(128) NOT NULL,
	metric_type ENUM('COUNTER', 'METER', 'GAUGE') NOT NULL,
	metric_value VARCHAR(256) NOT NULL,
	created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	last_modified_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	PRIMARY KEY (metric_id),
	FOREIGN KEY (job_id) 
	REFERENCES gobblin_job_executions(job_id) 
	ON DELETE CASCADE,
	INDEX (metric_group),
	INDEX (metric_name),
	INDEX (metric_type)
);

CREATE TABLE IF NOT EXISTS gobblin_task_metrics (
	metric_id BIGINT(21) NOT NULL AUTO_INCREMENT,
	task_id VARCHAR(128) NOT NULL,
	metric_group VARCHAR(128) NOT NULL,
	metric_name VARCHAR(128) NOT NULL,
	metric_type ENUM('COUNTER', 'METER', 'GAUGE') NOT NULL,
	metric_value VARCHAR(256) NOT NULL,
	created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	last_modified_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	PRIMARY KEY (metric_id),
	FOREIGN KEY (task_id) 
	REFERENCES gobblin_task_executions(task_id) 
	ON DELETE CASCADE,
	INDEX (metric_group),
	INDEX (metric_name),
	INDEX (metric_type)
);

