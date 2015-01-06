-- (c) 2014 LinkedIn Corp. All rights reserved.
--
-- Licensed under the Apache License, Version 2.0 (the "License"); you may not use
-- this file except in compliance with the License. You may obtain a copy of the
-- License at  http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software distributed
-- under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
-- CONDITIONS OF ANY KIND, either express or implied.

CREATE TABLE gobblin_job_executions (
	job_name VARCHAR(128) NOT NULL,
	job_id VARCHAR(128) NOT NULL,
	start_time TIMESTAMP,
	end_time TIMESTAMP,
	duration BIGINT,
	state VARCHAR(16),
	launched_tasks INT,
	completed_tasks INT,
	launcher_type VARCHAR(16),
  tracking_url VARCHAR(512),
	created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	last_modified_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (job_id)
);

CREATE TABLE gobblin_task_executions (
	task_id VARCHAR(128) NOT NULL,
	job_id VARCHAR(128) NOT NULL,
	start_time TIMESTAMP,
	end_time TIMESTAMP,
	duration BIGINT,
	state VARCHAR(16),
	failure_exception VARCHAR(1024),
	low_watermark BIGINT,
	high_watermark BIGINT,
	table_namespace VARCHAR(128),
	table_name VARCHAR(128),
	table_type VARCHAR(16),
	created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	last_modified_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (task_id),
	FOREIGN KEY (job_id) 
	REFERENCES gobblin_job_executions(job_id) 
	ON DELETE CASCADE
);

CREATE TABLE gobblin_job_metrics (
	metric_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),
	job_id VARCHAR(128) NOT NULL,
	metric_group VARCHAR(128) NOT NULL,
	metric_name VARCHAR(128) NOT NULL,
	metric_type VARCHAR(16) NOT NULL,
	metric_value VARCHAR(256) NOT NULL,
	created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	last_modified_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (metric_id),
	FOREIGN KEY (job_id) 
	REFERENCES gobblin_job_executions(job_id) 
	ON DELETE CASCADE
);

CREATE TABLE gobblin_task_metrics (
	metric_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),
	task_id VARCHAR(128) NOT NULL,
	metric_group VARCHAR(128) NOT NULL,
	metric_name VARCHAR(128) NOT NULL,
	metric_type VARCHAR(16) NOT NULL,
	metric_value VARCHAR(256) NOT NULL,
	created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	last_modified_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (metric_id),
	FOREIGN KEY (task_id) 
	REFERENCES gobblin_task_executions(task_id) 
	ON DELETE CASCADE
);

CREATE TABLE gobblin_job_properties (
  property_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),
  job_id VARCHAR(128) NOT NULL,
  property_key VARCHAR(128) NOT NULL,
  property_value VARCHAR(1024) NOT NULL,
	created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	last_modified_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (property_id),
	FOREIGN KEY (job_id)
  REFERENCES gobblin_job_executions(job_id)
  ON DELETE CASCADE
);

CREATE TABLE gobblin_task_properties (
  property_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),
  task_id VARCHAR(128) NOT NULL,
  property_key VARCHAR(128) NOT NULL,
  property_value VARCHAR(1024) NOT NULL,
	created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	last_modified_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (property_id),
	FOREIGN KEY (task_id)
  REFERENCES gobblin_task_executions(task_id)
  ON DELETE CASCADE
);
