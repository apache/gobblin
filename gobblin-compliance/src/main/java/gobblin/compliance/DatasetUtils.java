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
package gobblin.compliance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.Partition;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import lombok.extern.slf4j.Slf4j;

import gobblin.data.management.copy.hive.HiveDataset;
import gobblin.data.management.copy.hive.HiveUtils;
import gobblin.util.AutoReturnableObject;


/**
 * @author adsharma
 */
@Slf4j
public class DatasetUtils {
  public static final Splitter EQUALITY_SPLITTER = Splitter.on("=").omitEmptyStrings().trimResults();
  public static final Splitter SLASH_SPLITTER = Splitter.on("/").omitEmptyStrings().trimResults();
  public static final Splitter AT_SPLITTER = Splitter.on("@").omitEmptyStrings().trimResults();

  /**
   * Sort all HiveDatasets on the basis of complete name ie dbName.tableName
   * @param hiveDatasets
   */
  public static List<HiveDataset> sortHiveDatasets(List<HiveDataset> hiveDatasets) {
    Collections.sort(hiveDatasets, new Comparator<HiveDataset>() {
      @Override
      public int compare(HiveDataset o1, HiveDataset o2) {
        return o1.getTable().getCompleteName().compareTo(o2.getTable().getCompleteName());
      }
    });
    return hiveDatasets;
  }

  /**
   * Sort all partitions on the basis of complete name ie dbName.tableName.partitionName
   * @param partitions
   */
  public static List<Partition> sortPartitions(List<Partition> partitions) {
    Collections.sort(partitions, new Comparator<Partition>() {
      @Override
      public int compare(Partition o1, Partition o2) {
        return o1.getCompleteName().compareTo(o2.getCompleteName());
      }
    });
    return partitions;
  }

  /**
   * This method returns a sorted list of partitions for a give dataset.
   * @param hiveDataset
   * @return {@link List< Partition >}
   */
  public static List<Partition> getPartitionsFromDataset(HiveDataset hiveDataset) {
    try (AutoReturnableObject<IMetaStoreClient> client = hiveDataset.getClientPool().getClient()) {
      List<Partition> partitions =
          HiveUtils.getPartitions(client.get(), hiveDataset.getTable(), Optional.<String>absent());
      return sortPartitions(partitions);
    } catch (IOException e) {
      log.info(
          "Failed to get partitions from the " + hiveDataset.getTable().getCompleteName() + " : " + e.getMessage());
    }
    return new ArrayList<>();
  }

  /**
   * Add single quotes to the string, if not present.
   * TestString will be converted to 'TestString'
   * @param st
   * @return
   */
  public static String getQuotedString(String st) {
    if (!st.startsWith("'")) {
      st = "'" + st;
    }
    if (!st.endsWith("'")) {
      st = st + "'";
    }
    return st;
  }

  /**
   * This method splits the multiple partitions separated by '/' to list.
   * datepartition=2016-01-01-00/size=12345 will be [datepartition=2016-01-01-00, size=12345]
   * @param record
   */
  public static List<String> getPartitionValues(HivePurgerPartitionRecord record) {
    return ImmutableList.<String>builder().addAll(SLASH_SPLITTER.splitToList(record.getHiveTablePartition().getName()))
        .build();
  }

  /**
   * This method splits the string on the basis of '=' into key value pairs and also add quotes to the values.
   * [datepartition=2016-01-01-00, size=12345] will be [datepartition : '2016-01-01-00', size : '12345']
   * @param record
   */
  public static Map<String, String> getPartitionMap(HivePurgerPartitionRecord record) {
    Map<String, String> partitionMap = new LinkedHashMap<>();
    for (String st : getPartitionValues(record)) {
      final List<String> list = EQUALITY_SPLITTER.splitToList(st);
      Preconditions.checkArgument(list.size() == 2, "Wrong partitioned value " + st);
      String key = list.get(0);
      String value = list.get(1);
      value = getQuotedString(value);
      partitionMap.put(key, value);
    }
    return partitionMap;
  }

  /**
   * This method returns partitioning column names separated by comma.
   * [datepartition=2016-01-01-00, size=12345] will be datepartition, size
   * @param record
   * @return
   */
  public static String getCommaSeparatedPartitionColumnNames(HivePurgerPartitionRecord record) {
    StringBuilder sb = new StringBuilder();
    for (String partitionName : getPartitionMap(record).keySet()) {
      if (!sb.toString().isEmpty()) {
        sb.append(",");
      }
      sb.append(partitionName);
    }
    return sb.toString();
  }

  /**
   * This method builds the where clause for the insertion query.
   * If prefix is a, then it builds a.datepartition='2016-01-01-00' AND a.size='12345' from [datepartition : '2016-01-01-00', size : '12345']
   * @param prefix
   * @param record
   */
  public static String getWhereClauseForPartition(String prefix, HivePurgerPartitionRecord record) {
    StringBuilder sb = new StringBuilder();
    for (String partitionName : getPartitionMap(record).keySet()) {
      if (!sb.toString().isEmpty()) {
        sb.append(" AND ");
      }
      sb.append(prefix + partitionName);
      sb.append("=");
      sb.append(getPartitionMap(record).get(partitionName));
    }
    return sb.toString();
  }

  /**
   * This method returns the partition spec of the partition.
   * Example : (datepartition='2016-01-01-00', size='12345')
   * @param record
   * @return
   */
  public static String getPartitionSpec(HivePurgerPartitionRecord record) {
    StringBuilder sb = new StringBuilder();
    for (String partitionName : getPartitionMap(record).keySet()) {
      if (!sb.toString().isEmpty()) {
        sb.append(",");
      }
      sb.append(partitionName);
      sb.append("=");
      sb.append(getPartitionMap(record).get(partitionName));
    }
    return sb.toString();
  }

  public static String getCompleteStagingTableName(HivePurgerPartitionRecord record) {
    StringBuilder sb = new StringBuilder();
    sb.append(record.getStagingDb());
    sb.append(".");
    sb.append(HivePurgerConfigurationKeys.STAGING_PREFIX);
    sb.append(record.getHiveTablePartition().getTable().getTableName());
    return sb.toString();
  }

  public static String getCompleteOriginalTableName(HivePurgerPartitionRecord record) {
    StringBuilder sb = new StringBuilder();
    sb.append(record.getHiveTablePartition().getTable().getDbName());
    sb.append(".");
    sb.append(record.getHiveTablePartition().getTable().getTableName());
    return sb.toString();
  }

  public static String getStagingTableLocation(HivePurgerPartitionRecord record) {
    return record.getStagingDir() + record.getHiveTablePartition().getTable().getTableName();
  }

  public static String getStagingPartitionLocation(HivePurgerPartitionRecord record) {
    return getStagingTableLocation(record) + "/" + record.getHiveTablePartition().getName();
  }

  public static String getFinalPartitionLocation(HivePurgerPartitionRecord record) {
    StringBuilder sb = new StringBuilder();
    sb.append(record.getHiveTablePartition().getTable().getDataLocation());
    sb.append("/");
    sb.append(record.getHiveTablePartition().getName());
    return sb.toString();
  }
}
