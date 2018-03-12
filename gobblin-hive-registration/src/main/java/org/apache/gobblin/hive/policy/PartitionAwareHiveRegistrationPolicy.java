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

package org.apache.gobblin.hive.policy;

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.hive.HivePartition;
import org.apache.gobblin.hive.HiveRegistrationUnit;
import org.apache.gobblin.hive.HiveTable;


/**
 * This is the partition aware implementation of {@link HiveRegistrationPolicyBase}.
 * You can specify hive.partition.regex where the first match is the table location and the
 * rest match are the partitions.
 * The partition names can be specified with the hive.table.partition.keys property.
 * The order of the partition names should match the order of regexp matches in the hive.partition.regexp expression.
 *
 * For example in the case of path: s3://testbucket/myawesomlogs/compacted/dt=20170101/hr=22/
 * The hive.partition.regexp would look like: hive.partition.regex=(s3://testbucket/myawesomelogs/compacted/)dt=(.*)/hr=(.*)
 * and the hive.table.partition.keys=dt,hr
 *
 * @author Tamas Nemeth
 */

public class PartitionAwareHiveRegistrationPolicy extends HiveRegistrationPolicyBase{
  private static final Logger LOG = LoggerFactory.getLogger(PartitionAwareHiveRegistrationPolicy.class);

  public static final String HIVE_PARTITION_REGEX = "hive.partition.regex";
  public static final String HIVE_TABLE_PARTITION_KEYS = "hive.table.partition.keys";

  public PartitionAwareHiveRegistrationPolicy(State props)
      throws IOException {
    super(props);
  }

  protected HiveTable getTable(Path path, String dbName, String tableName) throws IOException {
    LOG.debug("Getting table definition for {}", tableName);

    HiveTable table = super.getTable(path, dbName, tableName);

    LOG.debug("Getting partition keys for {}", tableName);
    if (!Strings.isNullOrEmpty(this.props.getProp(HIVE_TABLE_PARTITION_KEYS))) {
      List<HiveRegistrationUnit.Column> partitionKeyColumns = Lists.<HiveRegistrationUnit.Column> newArrayList();

      for (String key : this.props.getPropAsList(HIVE_TABLE_PARTITION_KEYS)) {
        LOG.debug("Setting partition key {} for table {}", key, tableName);
        partitionKeyColumns.add(new HiveRegistrationUnit.Column(key , "string", null));
      }

      table.setPartitionKeys(partitionKeyColumns);
    }

    return table;
  }


  protected Optional<HivePartition> getPartition(Path path, HiveTable table) throws IOException {

    if (this.props.contains(HIVE_PARTITION_REGEX)) {
      Pattern pattern = Pattern.compile(this.props.getProp(HIVE_PARTITION_REGEX));
      List<String> partitionValues = Lists.newArrayList();

      Matcher matcher = pattern.matcher(path.toString());
      if (matcher.matches() && matcher.groupCount() >=2){
        for (int i = 2; i <= matcher.groupCount(); i++) {
          partitionValues.add(matcher.group(i));
        }
      } else {
        return Optional.<HivePartition> absent();
      }

      HivePartition.Builder builder = new HivePartition.Builder();
      builder.withDbName(table.getDbName());
      builder.withColumns(table.getColumns());
      builder.withTableName(table.getTableName());
      builder.withPartitionValues(partitionValues);
      builder.withProps(table.getProps());
      builder.withStorageProps(table.getStorageProps());
      builder.withSerdeProps(table.getSerDeProps());
      builder.withSerdeManaager(table.getSerDeManager().orNull());

      HivePartition partition = builder.build();
      partition.setSerDeProps(path);

      return Optional.of(partition);
    }else {
      return Optional.<HivePartition> absent();
    }
  }

  protected Path getTableLocation(Path path) {
    Optional<String> locationRegex = Optional.fromNullable(this.props.getProp(HIVE_PARTITION_REGEX));
    if (locationRegex.isPresent()) {
      Pattern pattern = Pattern.compile(locationRegex.get());
      Matcher matcher = pattern.matcher(path.toString());
      if (matcher.matches()) {
        String location = matcher.group(1);
        return new Path(location);
      }else {
        return path;
      }
    } else {
      return path;
    }
  }

}
