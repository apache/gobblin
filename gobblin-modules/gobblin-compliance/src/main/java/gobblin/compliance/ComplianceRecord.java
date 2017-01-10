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
package gobblin.compliance;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.Partition;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import lombok.Builder;
import lombok.Getter;


/**
 * Record type used by gobblin-compliance.
 * It contains all the necessary information needed for purging.
 *
 * @author adsharma
 */
@Getter
@Builder
public class ComplianceRecord extends QueryBaseHivePartitionRecord {
  private static final Splitter EQUALITY_SPLITTER = Splitter.on("=").omitEmptyStrings().trimResults();
  private static final Splitter SLASH_SPLITTER = Splitter.on("/").omitEmptyStrings().trimResults();
  private static final Splitter AT_SPLITTER = Splitter.on("@").omitEmptyStrings().trimResults();
  private static final String SINGLE_QUOTE = "'";

  private String complianceIdTable;
  private String complianceIdentifier;
  private String stagingDir;
  private String stagingDb;
  private Boolean commit;
  private String timeStamp;
  private String datasetComplianceId;

  public void setPurgeQueries(List<String> purgeQueries) {
    this.queries = purgeQueries;
  }

  public String getDbName() {
    return this.hivePartition.getTable().getDbName();
  }

  public String getTableName() {
    return this.hivePartition.getTable().getTableName();
  }

  public String getCompleteStagingTableName() {
    return this.stagingDb + "." + HivePurgerConfigurationKeys.STAGING_PREFIX + this.timeStamp + "_" + getTableName();
  }

  public String getCompleteOriginalTableName() {
    return getDbName() + "." + getTableName();
  }

  public String getStagingTableLocation() {
    return this.stagingDir + this.timeStamp + "/" + getTableName();
  }

  public String getPartitionName() {
    return this.hivePartition.getName();
  }

  public String getStagingPartitionLocation() {
    return getStagingTableLocation() + "/" + getPartitionName();
  }

  public String getTableLocation() {
    return this.hivePartition.getTable().getDataLocation().toString();
  }

  public String getFinalDirLocation() {
    return getTableLocation() + "/" + this.timeStamp;
  }

  public String getFinalPartitionLocation() {
    return getFinalDirLocation() + "/" + getPartitionName();
  }

  /**
   * Add single quotes to the string, if not present.
   * TestString will be converted to 'TestString'
   */
  public static String getQuotedString(String st) {
    Preconditions.checkNotNull(st);
    String quotedString = "";
    if (!st.startsWith(SINGLE_QUOTE)) {
      quotedString += SINGLE_QUOTE;
    }
    quotedString += st;
    if (!st.endsWith(SINGLE_QUOTE)) {
      quotedString += SINGLE_QUOTE;
    }
    return quotedString;
  }

  /**
   * This method splits the multiple partitions separated by '/' to list.
   * datepartition=2016-01-01-00/size=12345 will be [datepartition=2016-01-01-00, size=12345]
   * All "/" in the partition values are converted to %2 by Hive.
   */
  public List<String> getPartitionValues() {
    return ImmutableList.<String>builder().addAll(SLASH_SPLITTER.splitToList(getPartitionName())).build();
  }

  /**
   * This method splits the string on the basis of '=' into key value pairs and also add quotes to the values.
   * [datepartition=2016-01-01-00, size=12345] will be [datepartition : '2016-01-01-00', size : '12345']
   * All "=" in the partition values are converted to %3 by Hive.
   */
  public Map<String, String> getPartitionMap() {
    Map<String, String> partitionMap = new LinkedHashMap<>();
    for (String st : getPartitionValues()) {
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
   */
  public String getCommaSeparatedPartitionColumnNames() {
    StringBuilder sb = new StringBuilder();
    for (String partitionName : getPartitionMap().keySet()) {
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
   */
  public String getWhereClauseForPartition(String prefix) {
    StringBuilder sb = new StringBuilder();
    for (String partitionName : getPartitionMap().keySet()) {
      if (!sb.toString().isEmpty()) {
        sb.append(" AND ");
      }
      sb.append(prefix + partitionName);
      sb.append("=");
      sb.append(getPartitionMap().get(partitionName));
    }
    return sb.toString();
  }

  /**
   * This method returns the partition spec of the partition.
   * Example : (datepartition='2016-01-01-00', size='12345')
   */
  public String getPartitionSpec() {
    StringBuilder sb = new StringBuilder();
    for (String partitionName : getPartitionMap().keySet()) {
      if (!sb.toString().isEmpty()) {
        sb.append(",");
      }
      sb.append(partitionName);
      sb.append("=");
      sb.append(getPartitionMap().get(partitionName));
    }
    return sb.toString();
  }
}
