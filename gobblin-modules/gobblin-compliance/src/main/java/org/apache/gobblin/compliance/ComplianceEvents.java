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
package org.apache.gobblin.compliance;

/**
 * Class for event namespaces for gobblin-compliance
 *
 * @author adsharma
 */
public class ComplianceEvents {
  public static final String NAMESPACE = "gobblin.compliance";
  public static final String DATASET_URN_METADATA_KEY = "datasetUrn";
  // Value for this key will be a stacktrace of any exception caused
  public static final String FAILURE_CONTEXT_METADATA_KEY = "failureContext";

  public static class Retention {
    public static final String FAILED_EVENT_NAME = "RetentionFailed";
  }

  public static class Validation {
    public static final String FAILED_EVENT_NAME = "ValidationFailed";
  }

  public static class Restore {
    public static final String FAILED_EVENT_NAME = "RestoreFailed";
  }

  public static class Purger {
    public static final String WORKUNIT_GENERATED = "WorkUnitGenerated";
    public static final String WORKUNIT_COMMITTED = "WorkUnitCommitted";
    public static final String WORKUNIT_FAILED = "WorkUnitFailed";
    public static final String CYCLE_COMPLETED = "PurgeCycleCompleted";
  }
}
