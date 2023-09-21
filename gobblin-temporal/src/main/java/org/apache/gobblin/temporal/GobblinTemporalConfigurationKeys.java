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

package org.apache.gobblin.temporal;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.temporal.cluster.NestingExecWorker;


/**
 * A central place for configuration related constants of a Gobblin Temporal.
 */
@Alpha
public interface GobblinTemporalConfigurationKeys {

  String PREFIX = "gobblin.temporal.";

  String WORKER_CLASS = PREFIX + "worker.class";
  String DEFAULT_WORKER_CLASS = NestingExecWorker.class.getName();

  String GOBBLIN_TEMPORAL_TASK_QUEUE = PREFIX + "task.queue.name";
  String DEFAULT_GOBBLIN_TEMPORAL_TASK_QUEUE = "GobblinTemporalTaskQueue";

  /**
   * Number of worker processes to spin up per task runner
   * NOTE: If this size is too large, your container can OOM and halt execution unexpectedly. It's recommended not to touch
   * this parameter
   */
  String TEMPORAL_NUM_WORKERS_PER_TASK_RUNNER = "temporal.num.worker.per.container";
  int DEFAULT_TEMPORAL_NUM_WORKERS_PER_TASK_RUNNER = 1;
}
