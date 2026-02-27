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

package org.apache.gobblin.metrics.opentelemetry;

public class GobblinOpenTelemetryMetricsConstants {

  public static class DimensionKeys {
    public static final String STATE = "state";
    public static final String CURR_STATE = "currState";
    public static final String FS_SCHEME = "fsScheme";
  }

  public static class DimensionValues {
    public static final String GENERATE_WU = "generateWU";
    public static final String PROCESS_WU = "processWU";
    public static final String COMMIT_STEP = "commitStep";
    public static final String JOB_START = "jobStart";
    public static final String JOB_COMPLETE = "jobComplete";
    public static final String GENERATE_WU_START = "generateWUStart";
    public static final String GENERATE_WU_COMPLETE = "generateWUComplete";
    public static final String PROCESS_WU_START = "processWUStart";
    public static final String PROCESS_WU_COMPLETE = "processWUComplete";
    public static final String COMMIT_STEP_START = "commitStepStart";
    public static final String COMMIT_STEP_COMPLETE = "commitStepComplete";
  }
}
