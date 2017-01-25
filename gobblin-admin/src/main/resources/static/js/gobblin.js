/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var Gobblin = Gobblin || {}
Gobblin.columnSchemas = {
  listJobs: [
    { name: 'Job Name', fn: 'getJobNameLink' },
    { name: 'State', fn: 'getJobStateElem' },
    { name: 'Schedule', fn: 'getSchedule' },
    { name: 'Last Run Started', fn: 'getJobStartTime' },
    { name: 'Last Run Ended', fn: 'getJobEndTime' },
    { name: 'Extracted Records (most recent run)', fn: 'getRecordMetrics' }
  ],
  listByJobName: [
    { name: 'Job Id', fn: 'getJobIdLink' },
    { name: 'State', fn: 'getJobStateElem' },
    { name: 'Schedule', fn: 'getSchedule' },
    { name: 'Completed/Launched Tasks', fn: 'getTaskRatio' },
    { name: 'Start Time', fn: 'getJobStartTime' },
    { name: 'End Time', fn: 'getJobEndTime' },
    { name: 'Duration (seconds)', fn: 'getDurationInSeconds' },
    { name: 'Extracted Records', fn: 'getRecordMetrics' }
  ],
  listTasksByJobId: [
    { name: 'Task Id', fn: 'getTaskId' },
    { name: 'State', fn: 'getTaskStateElem' },
    { name: 'Start Time', fn: 'getTaskStartTime' },
    { name: 'End Time', fn: 'getTaskEndTime' },
    { name: 'Duration (seconds)', fn: 'getTaskDurationInSeconds' }
  ]
}
Gobblin.colors = {
  // Common bootstrap colors
  primary: '#ffc700',
  success: '#159876',
  info: '#2c3a80',
  warning: '#fd820a',
  danger: '#eb172e',

  // Auxilliary colors
  infoLight: '#3E92CC',
  neutral: '#00B9AE',
  purple: '#54428E'
}
Gobblin.stateMap = {
  'COMMITTED': { color: Gobblin.colors.success, class: 'success' },
  'SUCCESSFUL': { color: Gobblin.colors.neutral, class: 'neutral' },
  'RUNNING': { color: Gobblin.colors.infoLight, class: 'info-light' },
  'PENDING': { color: Gobblin.colors.primary, class: 'primary' },
  'CANCELLED': { color: Gobblin.colors.warning, class: 'warning' },
  'FAILED': { color: Gobblin.colors.danger, class: 'danger' }
}
Gobblin.settings = {
  restServerUrl: 'localhost:8080'
}
