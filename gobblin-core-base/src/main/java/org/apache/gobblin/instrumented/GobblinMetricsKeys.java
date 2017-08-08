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
package org.apache.gobblin.instrumented;

import org.apache.gobblin.Constructs;

/**
 * Shared constants related to gobblin metrics
 */
public class GobblinMetricsKeys {

  /** The FQN of the class that emitted a tracking event */
  public static final String CLASS_META = "class";
  /** The gobblin construct that emitted the event
   * @see Constructs */
  public static final String CONSTRUCT_META = "construct";

  // JobSpec related keys
  /** The JobSpec URI for which the tracking event is */
  public static final String JOB_SPEC_URI_META = "jobSpecURI";
  /** The version of the JobSpec for which the tracking event is */
  public static final String JOB_SPEC_VERSION_META = "jobSpecVersion";

  // Generic keys
  public static final String OPERATION_TYPE_META = "operationType";

  // FlowSpec related keys
  /** The FlowSpec URI for which the tracking event is */
  public static final String SPEC_URI_META = "flowSpecURI";
  /** The version of the FlowSpec for which the tracking event is */
  public static final String SPEC_VERSION_META = "flowSpecVersion";
}
