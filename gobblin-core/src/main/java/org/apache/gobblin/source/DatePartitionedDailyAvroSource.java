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

package org.apache.gobblin.source;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.util.DatePartitionType;


/**
 * Implementation of {@link org.apache.gobblin.source.Source} that reads over date-partitioned Avro data.
 *
 * <p>
 *
 * For example, if {@link ConfigurationKeys#SOURCE_FILEBASED_DATA_DIRECTORY} is set to /my/data/, then the class assumes
 * folders following the pattern /my/data/daily/[year]/[month]/[day] are present. It will iterate through all the data
 * under these folders starting from the date specified by {@link #DATE_PARTITIONED_SOURCE_MIN_WATERMARK_VALUE} until
 * either {@link #DATE_PARTITIONED_SOURCE_MAX_FILES_PER_JOB} files have been processed, or until there is no more data
 * to process. For example, if {@link #DATE_PARTITIONED_SOURCE_MIN_WATERMARK_VALUE} is set to 2015/01/01, then the job
 * will read from the folder /my/data/daily/2015/01/01/, /my/data/daily/2015/01/02/, /my/data/2015/01/03/ etc.
 *
 * <p>
 *
 * The class will only process data in Avro format.
 */
public class DatePartitionedDailyAvroSource extends DatePartitionedAvroFileSource {

  @Override
  protected void init(SourceState state) {
    state.setProp(DATE_PARTITIONED_SOURCE_PARTITION_PATTERN, DatePartitionType.DAY.getDateTimePattern());
    super.init(state);
  }

}
