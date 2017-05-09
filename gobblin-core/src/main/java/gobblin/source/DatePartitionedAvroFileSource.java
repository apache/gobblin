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

package gobblin.source;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.DatePartitionedAvroFileExtractor;
import gobblin.source.extractor.Extractor;
import gobblin.writer.partitioner.TimeBasedAvroWriterPartitioner;


/**
 * Implementation of {@link gobblin.source.Source} that reads over date-partitioned Avro data.
 * This source can be regarded as the reader equivalent of {@link TimeBasedAvroWriterPartitioner}.
 *
 * <p>
 * The class will iterate through all the data folders given by the base directory
 * {@link ConfigurationKeys#SOURCE_FILEBASED_DATA_DIRECTORY} and the partitioning type
 * {@link #DATE_PARTITIONED_SOURCE_PARTITION_PATTERN} or {@link #DATE_PARTITIONED_SOURCE_PARTITION_GRANULARITY}
 *
 * <p>
 * For example, if the base directory is set to /my/data/ and daily partitioning is used, then it is assumed that
 * /my/data/daily/[year]/[month]/[day] is present. It will iterate through all the data under these folders starting
 * from the date specified by {@link #DATE_PARTITIONED_SOURCE_MIN_WATERMARK_VALUE} until either
 * {@link #DATE_PARTITIONED_SOURCE_MAX_FILES_PER_JOB} files have been processed, or until there is no more data
 * to process. For example, if {@link #DATE_PARTITIONED_SOURCE_MIN_WATERMARK_VALUE} is set to 2015/01/01, then the job
 * will read from the folder /my/data/daily/2015/01/02/, /my/data/daily/2015/01/03/, /my/data/2015/01/04/ etc.
 * When partitions contain pre/suffixes in the form of /my/data/prefix/[year]/[month]/[day]/suffix, one can refer to
 * them via the {@link #DATE_PARTITIONED_SOURCE_PARTITION_PREFIX} and {@link #DATE_PARTITIONED_SOURCE_PARTITION_SUFFIX}
 * properties.
 * </p>
 *
 * </p>
 *
 * The class will only process data in Avro format.
 */
public class DatePartitionedAvroFileSource extends PartitionedFileSourceBase<Schema, GenericRecord> {

  public DatePartitionedAvroFileSource() {
    super(new DatePartitionedNestedRetriever(".avro"));
  }

  @Override
  public Extractor<Schema, GenericRecord> getExtractor(WorkUnitState state)
      throws IOException {
    return new DatePartitionedAvroFileExtractor(state);
  }
}