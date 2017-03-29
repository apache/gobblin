/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.util.DatePartitionType;


/**
 * Implementation of {@link gobblin.source.Source} that reads over date-partitioned Avro data.
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
