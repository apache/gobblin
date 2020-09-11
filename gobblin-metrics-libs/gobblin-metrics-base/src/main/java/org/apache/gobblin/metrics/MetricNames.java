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

package org.apache.gobblin.metrics;

/**
 * Contains names for all metrics generated in gobblin-core.
 */
public class MetricNames {

  /**
   * Extractor metrics.
   */
  public static class ExtractorMetrics {
    public static final String RECORDS_READ_METER = "gobblin.extractor.records.read";
    public static final String RECORDS_FAILED_METER = "gobblin.extractor.records.failed";
    // Times extraction of records.
    public static final String EXTRACT_TIMER = "gobblin.extractor.extract.time";
  }

  /**
   * Converter metrics.
   */
  public static class ConverterMetrics {
    public static final String RECORDS_IN_METER = "gobblin.converter.records.in";
    public static final String RECORDS_OUT_METER = "gobblin.converter.records.out";
    // Records in which failed conversion.
    public static final String RECORDS_FAILED_METER = "gobblin.converter.records.failed";
    // Times the generation of the Iterable.
    public static final String CONVERT_TIMER = "gobblin.converter.convert.time";
  }

  /**
   * Fork Operator metrics
   */
  public static class ForkOperatorMetrics {
    public static final String RECORDS_IN_METER = "gobblin.fork.operator.records.in";
    // Counts total number of forks generated (e.g. (true, true, false) adds 2).
    public static final String FORKS_OUT_METER = "gobblin.fork.operator.forks.out";
    // Times the computation of the fork list.
    public static final String FORK_TIMER = "gobblin.fork.operator.fork.time";
  }

  /**
   * Row level policy metrics.
   */
  public static class RowLevelPolicyMetrics {
    public static final String RECORDS_IN_METER = "gobblin.qualitychecker.records.in";
    public static final String RECORDS_PASSED_METER = "gobblin.qualitychecker.records.passed";
    public static final String RECORDS_FAILED_METER = "gobblin.qualitychecker.records.failed";
    // Times the policy decision.
    public static final String CHECK_TIMER = "gobblin.qualitychecker.check.time";
  }

  /**
   * {@link org.apache.gobblin.writer.DataWriter} metrics.
   */
  public static class DataWriterMetrics {

    /**
     * A {@link com.codahale.metrics.Meter} measuring the number of records
     * given to a {@link org.apache.gobblin.writer.DataWriter}. This does not count retries.
     */
    public static final String RECORDS_IN_METER = "gobblin.writer.records.in";

    /**
     * A {@link com.codahale.metrics.Meter} measuring the number of records attempted
     * to be written by a {@link org.apache.gobblin.writer.DataWriter}. This includes retries.
     */
    public static final String RECORDS_ATTEMPTED_METER = "gobblin.writer.records.attempted";

    /**
     * A {@link com.codahale.metrics.Meter} measuring the number of successful write operations performed by a
     * {@link org.apache.gobblin.writer.DataWriter}.
     */
    public static final String SUCCESSFUL_WRITES_METER = "gobblin.writer.successful.writes";

    /**
     * A {@link com.codahale.metrics.Meter} measuring the number of failed write operations performed by a
     * {@link org.apache.gobblin.writer.DataWriter}.
     */
    public static final String FAILED_WRITES_METER = "gobblin.writer.failed.writes";

    /**
     * A {@link com.codahale.metrics.Meter} measuring the number records written by a {@link org.apache.gobblin.writer.DataWriter}
     * as reported by its {@link org.apache.gobblin.writer.DataWriter#recordsWritten()} method.
     */
    public static final String RECORDS_WRITTEN_METER = "gobblin.writer.records.written";

    /**
     * A {@link com.codahale.metrics.Meter} measuring the number bytes written by a {@link org.apache.gobblin.writer.DataWriter} as
     * reported by its {@link org.apache.gobblin.writer.DataWriter#bytesWritten()} method.
     */
    public static final String BYTES_WRITTEN_METER = "gobblin.writer.bytes.written";

    /**
     * A {@link com.codahale.metrics.Timer} measuring the time taken for each write operation.
     */
    public static final String WRITE_TIMER = "gobblin.writer.write.time";
  }
}
