-- Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
--
-- Licensed under the Apache License, Version 2.0 (the "License"); you may not use
-- this file except in compliance with the License. You may obtain a copy of the
-- License at  http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software distributed
-- under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
-- CONDITIONS OF ANY KIND, either express or implied.

--
-- Version: 1.0.1
--
-- Changes:
--   1. Changes the indexes on `gobblin_job_metrics`, `gobblin_task_metrics`,
--      `gobblin_job_properties`, and `gobblin_task_properties` to unique indexes.
--      This enables insert/update performance increases because the java code
--      can use upserts rather than a combination of exist/insert/update calls.
--   2. Increases the size of the `property_value` columns from TEXT to MEDIUMTEXT
--      to allow larger properties such as 'source.filebased.fs.snapshot'
--

ALTER TABLE `gobblin_job_metrics` ADD UNIQUE INDEX `ux_job_metric` (job_id, metric_group, metric_name, metric_type);
ALTER TABLE `gobblin_job_metrics` DROP INDEX `metric_group`;
ALTER TABLE `gobblin_job_metrics` DROP INDEX `metric_name`;
ALTER TABLE `gobblin_job_metrics` DROP INDEX `metric_type`;

ALTER TABLE `gobblin_task_metrics` ADD UNIQUE INDEX `ux_task_metric` (task_id, metric_group, metric_name, metric_type);
ALTER TABLE `gobblin_task_metrics` DROP INDEX `metric_group`;
ALTER TABLE `gobblin_task_metrics` DROP INDEX `metric_name`;
ALTER TABLE `gobblin_task_metrics` DROP INDEX `metric_type`;

ALTER TABLE `gobblin_job_properties` MODIFY `property_value` MEDIUMTEXT NOT NULL;
ALTER TABLE `gobblin_job_properties` ADD UNIQUE INDEX `ux_job_property` (job_id, property_key);
ALTER TABLE `gobblin_job_properties` DROP INDEX `property_key`;

ALTER TABLE `gobblin_task_properties` MODIFY `property_value` MEDIUMTEXT NOT NULL;
ALTER TABLE `gobblin_task_properties` ADD UNIQUE INDEX `ux_task_property` (task_id, property_key);
ALTER TABLE `gobblin_task_properties` DROP INDEX `property_key`;