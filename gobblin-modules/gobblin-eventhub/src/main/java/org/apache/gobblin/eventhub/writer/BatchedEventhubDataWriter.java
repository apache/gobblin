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
package org.apache.gobblin.eventhub.writer;

import org.apache.gobblin.writer.BufferedAsyncDataWriter;

/**
 * A batch writer for eventhub, composed by {@link EventhubBatchAccumulator} and {@link EventhubDataWriter}
 * {@link EventhubBatchAccumulator} provides a buffer to store pending records
 * {@link EventhubDataWriter} is the actual writer ships data to eventhub
 */
public class BatchedEventhubDataWriter extends BufferedAsyncDataWriter<String> {

  public static final String COMMIT_TIMEOUT_MILLIS_CONFIG = "writer.eventhub.commitTimeoutMillis";
  public static final long   COMMIT_TIMEOUT_MILLIS_DEFAULT = 60000; // 1 minute
  public static final String COMMIT_STEP_WAIT_TIME_CONFIG = "writer.eventhub.commitStepWaitTimeMillis";
  public static final long   COMMIT_STEP_WAIT_TIME_DEFAULT = 500; // 500ms
  public static final String FAILURE_ALLOWANCE_PCT_CONFIG = "writer.eventhub.failureAllowancePercentage";
  public static final double FAILURE_ALLOWANCE_PCT_DEFAULT = 20.0;

  public final static String  EVH_NAMESPACE = "eventhub.namespace";
  public final static String  EVH_HUBNAME = "eventhub.hubname";
  public final static String  EVH_SAS_KEYNAME = "eventhub.sas.keyname";
  public final static String  EVH_SAS_KEYVALUE = "eventhub.sas.keyvalue";

  public BatchedEventhubDataWriter (EventhubBatchAccumulator accumulator, EventhubDataWriter dataWriter) {
    super (accumulator, dataWriter);
  }
}
