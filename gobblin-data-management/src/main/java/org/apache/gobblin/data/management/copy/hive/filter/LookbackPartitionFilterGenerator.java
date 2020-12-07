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

package org.apache.gobblin.data.management.copy.hive.filter;

import java.util.Arrays;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import org.apache.gobblin.data.management.copy.hive.HiveDataset;
import org.apache.gobblin.data.management.copy.hive.HiveDatasetFinder;
import org.apache.gobblin.data.management.copy.hive.PartitionFilterGenerator;


/**
 * Filters partitions according to a lookback period. The partition values must be time formatted. User must specify
 * the partition column, lookback period (as ISO 8601 period), and datetime format of the column values.
 *
 * <p>
 *   The generated filter is of the form "datePartition >= 'date'", so the column must be of string type and its format
 *   must be such that lexycographical string and date ordering are compatible.
 * </p>
 */
@Slf4j
public class LookbackPartitionFilterGenerator implements PartitionFilterGenerator {

  public static final String PARTITION_COLUMN = HiveDatasetFinder.HIVE_DATASET_PREFIX + ".partition.filter.datetime.column";
  public static final String LOOKBACK = HiveDatasetFinder.HIVE_DATASET_PREFIX + ".partition.filter.datetime.lookback";
  public static final String DATETIME_FORMAT = HiveDatasetFinder.HIVE_DATASET_PREFIX + ".partition.filter.datetime.format";

  private final Properties prop;

  public LookbackPartitionFilterGenerator(Properties properties) {
    this.prop = (properties == null) ? System.getProperties(): properties;
  }

  @Override
  public String getFilter(HiveDataset hiveDataset) {

    if (isValidConfig()) {
      String partitionColumn = this.prop.getProperty(PARTITION_COLUMN);
      Period lookback = Period.parse(this.prop.getProperty(LOOKBACK));
      DateTimeFormatter formatter = DateTimeFormat.forPattern(this.prop.getProperty(DATETIME_FORMAT));

      DateTime limitDate = (new DateTime()).minus(lookback);

      String partitionFilter = String.format("%s >= \"%s\"", partitionColumn, formatter.print(limitDate));
      log.info(String.format("Getting partitions for %s using partition filter %s", ((hiveDataset == null) ? "null" :  hiveDataset.getTable()
          .getCompleteName()), partitionFilter));
      return partitionFilter;
    } else {
      log.error(LookbackPartitionFilterGenerator.class.getName()
          + " requires the following properties " + Arrays.toString(new String[]{PARTITION_COLUMN, LOOKBACK, DATETIME_FORMAT}));

      return null;
    }
  }

  private boolean isValidConfig() {
    return this.prop.containsKey(LookbackPartitionFilterGenerator.PARTITION_COLUMN)
        && this.prop.containsKey(LookbackPartitionFilterGenerator.DATETIME_FORMAT)
        && this.prop.containsKey(LookbackPartitionFilterGenerator.LOOKBACK);
  }
}
