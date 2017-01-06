--
--  Licensed to the Apache Software Foundation (ASF) under one or more
--  contributor license agreements.  See the NOTICE file distributed with
--  this work for additional information regarding copyright ownership.
--  The ASF licenses this file to You under the Apache License, Version 2.0
--  (the "License"); you may not use this file except in compliance with
--  the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
--  Unless required by applicable law or agreed to in writing, software
--  distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and
--  limitations under the License.
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