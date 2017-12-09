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

package org.apache.gobblin.cluster;

import org.apache.gobblin.annotation.Alpha;


/**
 * A central place for constants of {@link org.apache.gobblin.metrics.MetricContext} tag names for a Gobblin cluster.
 *
 * @author Yinan Li
 */
@Alpha
public class GobblinClusterMetricTagNames {

  public static final String APPLICATION_NAME = "application.name";
  public static final String APPLICATION_ID = "application.id";
  public static final String HELIX_INSTANCE_NAME = "helix.instance.name";
  public static final String TASK_RUNNER_ID = "task.runner.id";

  public static final String FLOW_GROUP = "flowGroup";
  public static final String FLOW_NAME = "flowName";
  public static final String FLOW_EXECUTION_ID = "flowExecutionId";
  public static final String JOB_GROUP = "jobGroup";
  public static final String JOB_NAME = "jobName";
  public static final String JOB_EXECUTION_ID = "jobExecutionId";
}
