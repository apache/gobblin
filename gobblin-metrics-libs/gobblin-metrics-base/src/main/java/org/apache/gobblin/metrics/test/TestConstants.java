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

package org.apache.gobblin.metrics.test;

/**
 * A central place for constants used in tests for gobblin-metrics.
 *
 * @author Yinan Li
 */
public class TestConstants {

  public static final String METRIC_PREFIX = "com.linkedin.example";

  public static final String GAUGE = "gauge";
  public static final String COUNTER = "counter";
  public static final String METER = "meter";
  public static final String HISTOGRAM = "histogram";
  public static final String TIMER = "timer";

  public static final String CONTEXT_NAME = "TestContext";
  public static final String RECORDS_PROCESSED = "recordsProcessed";
  public static final String RECORD_PROCESS_RATE = "recordProcessRate";
  public static final String RECORD_SIZE_DISTRIBUTION = "recordSizeDistribution";
  public static final String TOTAL_DURATION = "totalDuration";
  public static final String QUEUE_SIZE = "queueSize";
}
