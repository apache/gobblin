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

package org.apache.gobblin.writer;

/**
 * Configuration keys for the Gobblin ORC Writer
 */
public class GobblinOrcWriterConfigs {
  public static final String ORC_WRITER_PREFIX = "orcWriter.";
  /**
   * Default buffer size in the ORC Writer before sending the records to the native ORC Writer
   */
  public static final String ORC_WRITER_BATCH_SIZE = ORC_WRITER_PREFIX + "batchSize";
  /**
   * Configuration for enabling Gobblin Avro -> ORC Self tuning writer, optimized for Kafka Streaming Ingestion
   */
  public static final String ORC_WRITER_AUTO_SELFTUNE_ENABLED = ORC_WRITER_PREFIX + "auto.selfTune.enabled";
  /**
   * Max buffer size of the Gobblin ORC Writer that it can be tuned to
   */
  public static final String ORC_WRITER_AUTO_SELFTUNE_MAX_BATCH_SIZE = ORC_WRITER_PREFIX + "auto.selfTune.max.batch.size";
  /**
   * How often should the Gobblin ORC Writer check for tuning
   */
  public static final String ORC_WRITER_AUTO_SELFTUNE_ROWS_BETWEEN_CHECK = ORC_WRITER_PREFIX + "auto.selfTune.rowsBetweenCheck";
  /**
   * What percentage of the JVM memory should be reserved for the buffers of the Gobblin ORC Writer, approximately
   */
  public static final String ORC_WRITER_ROWBATCH_MEMORY_USAGE_FACTOR = ORC_WRITER_PREFIX + "auto.selfTune.memory.usage.factor";
  /**
   * In the self tuning writer, the minimum buffer size that can be configured for the initialization of the native ORC Writer,
   * size measured in records before checking to flush if buffer memory size exceeds stripe size. Note that the Gobblin ORC Writer
   * will initialize the native ORC Writer just once in its lifecycle to prevent multiple small files.
   */
  public static final String ORC_WRITER_MIN_ROWCHECK = ORC_WRITER_PREFIX + "min.rows.between.memory.checks";
  /**
   * In the self tuning writer, the maximum buffer size that can be configured for the initialization of the native ORC Writer,
   * size measured in records before checking to flush if buffer memory size exceeds stripe size. Note that the Gobblin ORC Writer
   * will initialize the native ORC Writer just once in its lifecycle to prevent multiple small files.
   */
  public static final String ORC_WRITER_MAX_ROWCHECK = ORC_WRITER_PREFIX + "max.rows.between.memory.checks";

  public static final String ORC_WRITER_INSTRUMENTED = ORC_WRITER_PREFIX + "instrumented";

  public static final int DEFAULT_ORC_WRITER_BATCH_SIZE = 1000;
  /**
   *  This value gives an estimation on how many writers are buffering records at the same time in a container.
   *    Since time-based partition scheme is a commonly used practice, plus the chances for late-arrival data,
   *    usually there would be 2-3 writers running during the hourly boundary. 3 is chosen here for being conservative.
   */
  public static final int DEFAULT_CONCURRENT_WRITERS = 3;
  public static final double DEFAULT_ORC_WRITER_BATCHSIZE_MEMORY_USAGE_FACTOR = 0.3;
  /**
   * The ratio of native ORC Writer buffer size to Gobblin ORC Writer buffer size
   */
  public static final int DEFAULT_ORC_WRITER_BATCHSIZE_ROWCHECK_FACTOR = 5;
  public static final int DEFAULT_MAX_ORC_WRITER_BATCH_SIZE = DEFAULT_ORC_WRITER_BATCH_SIZE;
  public static final int DEFAULT_ORC_AUTO_SELFTUNE_ROWS_BETWEEN_CHECK = 500;
  /**
   * Tune iff the new batch size is 10% different from the current batch size
   */
  public static final double DEFAULT_ORC_WRITER_TUNE_BATCHSIZE_SENSITIVITY = 0.1;
  public static final int DEFAULT_MIN_ORC_WRITER_ROWCHECK = 150;
  public static final int DEFAULT_MAX_ORC_WRITER_ROWCHECK = 5000;

  public static class RuntimeStateConfigs {
    public static final String ORC_WRITER_ESTIMATED_RECORD_SIZE = ORC_WRITER_PREFIX + "estimated.recordSize";
    public static final String ORC_WRITER_NATIVE_WRITER_MEMORY = ORC_WRITER_PREFIX + "estimated.native.writer.memory";
    public static final String ORC_WRITER_PREVIOUS_BATCH_SIZE = ORC_WRITER_PREFIX + "previous.batch.size";
    public static final String ORC_WRITER_CONCURRENT_TASKS = ORC_WRITER_PREFIX + "auto.selfTune.concurrent.tasks";
    public static final String ORC_WRITER_ESTIMATED_BYTES_ALLOCATED_CONVERTER_MEMORY = ORC_WRITER_PREFIX + "estimated.bytes.allocated.converter.memory";
  }
}
