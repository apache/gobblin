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

package org.apache.gobblin.data.management.copy.iceberg.predicates;

import java.util.List;
import java.util.Properties;
import java.util.function.Predicate;

import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableMetadata;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.gobblin.data.management.copy.iceberg.IcebergDatasetFinder;

public class IcebergDateTimePartitionFilterPredicate implements Predicate<StructLike> {

  private static final List<String> supportedTransforms = ImmutableList.of("identity");
  private static final String DATETIME_PARTITION_KEY = "partition.datetime";
  private static final String DATETIME_PARTITION_PATTERN_KEY = DATETIME_PARTITION_KEY + ".pattern";
  private static final String DATETIME_PARTITION_STARTDATE_KEY = DATETIME_PARTITION_KEY + ".startdate";
  private static final String DATETIME_PARTITION_ENDDATE_KEY = DATETIME_PARTITION_KEY + ".enddate";
  private final int partitionColumnIndex;
  private final DateTimeFormatter dateTimeFormatter;
  private final DateTime startDate;
  private final DateTime endDate;

  public IcebergDateTimePartitionFilterPredicate(String partitionColumnName, TableMetadata tableMetadata,
      Properties properties) {

    this.partitionColumnIndex = IcebergPartitionFilterPredicateUtil.getPartitionColumnIndex(partitionColumnName,
        tableMetadata, supportedTransforms);;
    Preconditions.checkArgument(this.partitionColumnIndex != -1,
        String.format("Partition column %s not found", partitionColumnName));

    String partitionPattern = IcebergDatasetFinder.getLocationQualifiedProperty(properties,
        IcebergDatasetFinder.CatalogLocation.SOURCE,
        DATETIME_PARTITION_PATTERN_KEY);

    String startDateVal = IcebergDatasetFinder.getLocationQualifiedProperty(properties,
        IcebergDatasetFinder.CatalogLocation.SOURCE,
        DATETIME_PARTITION_STARTDATE_KEY);

    String endDateVal = IcebergDatasetFinder.getLocationQualifiedProperty(properties,
        IcebergDatasetFinder.CatalogLocation.SOURCE,
        DATETIME_PARTITION_ENDDATE_KEY);

    Preconditions.checkArgument(StringUtils.isNotBlank(partitionPattern), "DateTime Partition pattern cannot be empty");
    Preconditions.checkArgument(StringUtils.isNotBlank(startDateVal), "DateTime Partition start date cannot be empty");
    Preconditions.checkArgument(StringUtils.isNotBlank(endDateVal), "DateTime Partition end date cannot be empty");

    this.dateTimeFormatter = DateTimeFormat.forPattern(partitionPattern).withZone(DateTimeZone.UTC);
    this.startDate = this.dateTimeFormatter.parseDateTime(startDateVal);
    this.endDate = this.dateTimeFormatter.parseDateTime(endDateVal);
  }

  @Override
  public boolean test(StructLike partition) {
    String partitionVal = partition.get(this.partitionColumnIndex, String.class);

    if (StringUtils.isBlank(partitionVal)) {
      return false;
    }

    DateTime partitionDateTime = this.dateTimeFormatter.parseDateTime(partitionVal);

    if (partitionDateTime.isEqual(this.startDate) || partitionDateTime.isEqual(this.endDate)) {
      return true;
    }
    return partitionDateTime.isAfter(this.startDate) && partitionDateTime.isBefore(this.endDate);
  }
}
