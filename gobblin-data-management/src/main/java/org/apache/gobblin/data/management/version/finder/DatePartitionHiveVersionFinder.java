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
package org.apache.gobblin.data.management.version.finder;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.typesafe.config.Config;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.data.management.version.TimestampedHiveDatasetVersion;
import org.apache.gobblin.util.ConfigUtils;


/**
 * A Hive Partition finder where the the version is the partition value.
 * <p>
 * The hive table needs to be date partitioned by prop value {@value #PARTITION_KEY_NAME_KEY}. The value of this key must be
 * a date pattern as per prop value {@value #PARTITION_VALUE_DATE_TIME_PATTERN_KEY}.
 * </p>
 * <p>
 * E.g if the hive partition is datepartition=2016-01-10-22/field1=f1Value.
 * The {@value #PARTITION_KEY_NAME_KEY}=datepartiton and {@value #PARTITION_VALUE_DATE_TIME_PATTERN_KEY}=yyyy-MM-dd-HH
 *
 * </p>
 */
public class DatePartitionHiveVersionFinder extends AbstractHiveDatasetVersionFinder {

  public static final String PARTITION_VALUE_DATE_TIME_PATTERN_KEY = "hive.partition.value.datetime.pattern";
  public static final String DEFAULT_PARTITION_VALUE_DATE_TIME_PATTERN = "yyyy-MM-dd-HH";

  public static final String PARTITION_VALUE_DATE_TIME_TIMEZONE_KEY = "hive.partition.value.datetime.timezone";
  public static final String DEFAULT_PARTITION_VALUE_DATE_TIME_TIMEZONE = ConfigurationKeys.PST_TIMEZONE_NAME;

  public static final String PARTITION_KEY_NAME_KEY = "hive.partition.key.name";
  public static final String DEFAULT_PARTITION_KEY_NAME = "datepartition";

  protected final DateTimeFormatter formatter;
  private final String partitionKeyName;
  private final Predicate<FieldSchema> partitionKeyNamePredicate;
  private final String pattern;

  public DatePartitionHiveVersionFinder(FileSystem fs, Config config) {

    this.pattern =
        ConfigUtils.getString(config, PARTITION_VALUE_DATE_TIME_PATTERN_KEY, DEFAULT_PARTITION_VALUE_DATE_TIME_PATTERN);

    if (config.hasPath(PARTITION_VALUE_DATE_TIME_TIMEZONE_KEY)) {
      this.formatter = DateTimeFormat.forPattern(pattern)
          .withZone(DateTimeZone.forID(config.getString(PARTITION_VALUE_DATE_TIME_TIMEZONE_KEY)));
    } else {
      this.formatter =
          DateTimeFormat.forPattern(pattern).withZone(DateTimeZone.forID(DEFAULT_PARTITION_VALUE_DATE_TIME_TIMEZONE));
    }

    this.partitionKeyName = ConfigUtils.getString(config, PARTITION_KEY_NAME_KEY, DEFAULT_PARTITION_KEY_NAME);
    this.partitionKeyNamePredicate = new Predicate<FieldSchema>() {

      @Override
      public boolean apply(FieldSchema input) {
        return StringUtils.equalsIgnoreCase(input.getName(), DatePartitionHiveVersionFinder.this.partitionKeyName);
      }
    };
  }

  /**
   * Create a {@link TimestampedHiveDatasetVersion} from a {@link Partition}. The hive table is expected
   * to be date partitioned by {@link #partitionKeyName}. The partition value format must be {@link #pattern}
   *
   * @throws IllegalArgumentException when {@link #partitionKeyName} is not found in the <code></code>
   * @throws IllegalArgumentException when a value can not be found for {@link #partitionKeyName} in the <code>partition</code>
   * @throws IllegalArgumentException if the partition value can not be parsed with {@link #pattern}
   * {@inheritDoc}
   */
  @Override
  protected TimestampedHiveDatasetVersion getDatasetVersion(Partition partition) {

    int index = Iterables.indexOf(partition.getTable().getPartitionKeys(), this.partitionKeyNamePredicate);

    if (index == -1) {
      throw new IllegalArgumentException(String
          .format("Failed to find partition key %s in the table %s", this.partitionKeyName,
              partition.getTable().getCompleteName()));
    }

    if (index >= partition.getValues().size()) {
      throw new IllegalArgumentException(String
          .format("Failed to find partition value for key %s in the partition %s", this.partitionKeyName,
              partition.getName()));
    }
    return new TimestampedHiveDatasetVersion(
        this.formatter.parseDateTime(partition.getValues().get(index).trim().substring(0, this.pattern.length())),
        partition);
  }
}
