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
package org.apache.gobblin.ingestion.google;

public class GoggleIngestionConfigurationKeys {
  public static final String DAY_PARTITIONER_KEY_PREFIX = "writer.partitioner.google_ingestion";
  /**
   * Optional. Default to String.Empty
   * Prepend a prefix to each partition
   */
  public static final String KEY_PARTITIONER_PREFIX = DAY_PARTITIONER_KEY_PREFIX + "prefix";
  /**
   * Optional. Default to false.
   * Determine whether to include column names into the partition path.
   */
  public static final String KEY_INCLUDE_COLUMN_NAMES = DAY_PARTITIONER_KEY_PREFIX + "column_names.include";
  /**
   * Optional. Default to "Date".
   * Configure the column name for "Date" field/column.
   */
  public static final String KEY_DATE_COLUMN_NAME = DAY_PARTITIONER_KEY_PREFIX + "date.column_name";
  /**
   * Optional. Default to "yyyy-MM-dd".
   * Configure the date string format for date value in records
   */
  public static final String KEY_DATE_FORMAT = DAY_PARTITIONER_KEY_PREFIX + "date.format";

  /**
   * Configure the size of underlying blocking queue of the asynchronized iterator - AsyncIteratorWithDataSink
   * Default to 2000.
   */
  public static final String SOURCE_ASYNC_ITERATOR_BLOCKING_QUEUE_SIZE = "source.async_iterator.blocking_queue_size";

  /**
   * Configure the poll blocking time of underlying blocking queue of the asynchronized iterator - AsyncIteratorWithDataSink
   * Default to 1 second.
   */
  public static final String SOURCE_ASYNC_ITERATOR_POLL_BLOCKING_TIME = "source.async_iterator.poll_blocking_time";
}
