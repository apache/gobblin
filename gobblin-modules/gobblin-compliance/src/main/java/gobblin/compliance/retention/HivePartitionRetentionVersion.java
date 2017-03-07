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
package gobblin.compliance.retention;

import org.apache.hadoop.hive.ql.metadata.Partition;

import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

import gobblin.compliance.ComplianceConfigurationKeys;
import gobblin.compliance.HivePartitionVersion;


/**
 * A version class corresponding to the {@link CleanableHivePartitionDataset}
 *
 * @author adsharma
 */
@SuppressWarnings
public class HivePartitionRetentionVersion extends HivePartitionVersion {

  public HivePartitionRetentionVersion(Partition version) {
    super(version);
  }

  @Override
  public int compareTo(HivePartitionVersion version) {
    long thisAge = getAgeInMilliSeconds(this);
    long otherAge = getAgeInMilliSeconds((HivePartitionRetentionVersion) version);
    return (thisAge < otherAge) ? -1 : ((thisAge == otherAge) ? 0 : 1);
  }

  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj instanceof HivePartitionVersion) {
      return compareTo((HivePartitionVersion) obj) == 0;
    }
    return false;
  }

  public long getAgeInMilliSeconds(HivePartitionRetentionVersion version) {
    Preconditions.checkArgument(getTimeStamp(version).length() == ComplianceConfigurationKeys.TIME_STAMP_LENGTH,
        "Invalid time stamp for dataset : " + version.datasetURN() + " time stamp is :" + getTimeStamp(version));
    return System.currentTimeMillis() - Long.parseLong(getTimeStamp(version));
  }

  public Long getAgeInMilliSeconds() {
    return getAgeInMilliSeconds(this);
  }

  public static String getTimeStamp(HivePartitionRetentionVersion version) {
    String tableName = version.getTableName();
    return tableName.substring(tableName.lastIndexOf("_") + 1);
  }

  public String getTimeStamp() {
    return getTimeStamp(this);
  }
}
