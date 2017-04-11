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

package gobblin.kafka.writer;

/**
 * Listing of Metrics names used by the {@link KafkaDataWriter}
 */
public class KafkaWriterMetricNames {
  /**
   * A {@link com.codahale.metrics.Meter} measuring the number of records sent to
   * a {@link gobblin.kafka.writer.KafkaDataWriter}.
   */
  public static final String RECORDS_PRODUCED_METER = "gobblin.writer.kafka.records.produced";

  /**
   * A {@link com.codahale.metrics.Meter} measuring the number of records that failed to be written by
   * {@link gobblin.kafka.writer.KafkaDataWriter}.
   */
  public static final String RECORDS_FAILED_METER = "gobblin.writer.kafka.records.failed";

  /**
   * A {@link com.codahale.metrics.Meter} measuring the number of records that were successfully written by
   * {@link gobblin.kafka.writer.KafkaDataWriter}.
   */
  public static final String RECORDS_SUCCESS_METER = "gobblin.writer.kafka.records.success";

}
