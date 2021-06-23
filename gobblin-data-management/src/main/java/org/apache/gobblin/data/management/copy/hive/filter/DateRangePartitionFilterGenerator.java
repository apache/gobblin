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

import org.apache.gobblin.data.management.copy.hive.HiveDataset;
import org.apache.gobblin.data.management.copy.hive.HiveDatasetFinder;
import org.apache.gobblin.data.management.copy.hive.PartitionFilterGenerator;


/**
 * Filters hive partitions using BETWEEN START AND END date range.
 * Requires PARTITION_COLUMN, START_DATE, END_DATE
 *
 * <p>
 *   The generated filter is of the form "datePartition between 'start_date' and 'end_date' ".
 * </p>
 */
@Slf4j
public class DateRangePartitionFilterGenerator implements PartitionFilterGenerator {

  public static final String PARTITION_COLUMN = HiveDatasetFinder.HIVE_DATASET_PREFIX + ".partition.filter.datetime.column";
  public static final String START_DATE = HiveDatasetFinder.HIVE_DATASET_PREFIX + ".partition.filter.datetime.startdate";
  public static final String END_DATE = HiveDatasetFinder.HIVE_DATASET_PREFIX + ".partition.filter.datetime.enddate";

  private final Properties prop;

  public DateRangePartitionFilterGenerator(Properties properties) {
    this.prop = (properties == null) ? System.getProperties(): properties;
  }

  @Override
  public String getFilter(HiveDataset hiveDataset) {

    if (isValidConfig()) {
      String partitionColumn = this.prop.getProperty(PARTITION_COLUMN);
      String startDate = this.prop.getProperty(START_DATE);
      String endDate = this.prop.getProperty(END_DATE);

      String partitionFilter =String.format("%s between \"%s\" and \"%s\"", partitionColumn, startDate, endDate);

      log.info(String.format("Getting partitions for %s using partition filter %s", ((hiveDataset == null) ? "null" :  hiveDataset.getTable()
          .getCompleteName()), partitionFilter));
      return partitionFilter;
    } else {
      log.error(DateRangePartitionFilterGenerator.class.getName()
          + " requires the following properties " + Arrays.toString(new String[]{PARTITION_COLUMN, START_DATE, END_DATE}));

      return null;
    }
  }

  private boolean isValidConfig() {
    return this.prop.containsKey(DateRangePartitionFilterGenerator.PARTITION_COLUMN)
        && this.prop.containsKey(DateRangePartitionFilterGenerator.START_DATE)
        && this.prop.containsKey(DateRangePartitionFilterGenerator.END_DATE);
  }
}
