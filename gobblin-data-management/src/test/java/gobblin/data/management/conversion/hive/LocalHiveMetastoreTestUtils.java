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
package gobblin.data.management.conversion.hive;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

import gobblin.hive.HiveMetastoreClientPool;
import gobblin.hive.avro.HiveAvroSerDeManager;


/**
 * Provides a singleton instance of local Hive metastore client and helper methods
 * to run test cases
 */
@Slf4j
public class LocalHiveMetastoreTestUtils {
  private static LocalHiveMetastoreTestUtils instance;
  private IMetaStoreClient localMetastoreClient;

  private LocalHiveMetastoreTestUtils() throws IOException {
    this.localMetastoreClient =
        HiveMetastoreClientPool.get(new Properties(), Optional.<String>absent()).getClient().get();
  }

  static {
    try {
      // Not most optimal singleton initialization, but sufficient for test
      instance = new LocalHiveMetastoreTestUtils();
    } catch (IOException e) {
      throw new RuntimeException("Exception occurred in initializing ConversionHiveUtils", e);
    }
  }

  public static LocalHiveMetastoreTestUtils getInstance() {
    return instance;
  }

  public IMetaStoreClient getLocalMetastoreClient() {
    return localMetastoreClient;
  }

  public void dropDatabaseIfExists(String dbName) throws MetaException, TException {
    try {
      this.getLocalMetastoreClient().getDatabase(dbName);
      this.getLocalMetastoreClient().dropDatabase(dbName, false, true, true);
    } catch (NoSuchObjectException e) {
      // No need to drop
    }
  }

  public Table createTestTable(String dbName, String tableName, String tableSdLoc, Optional<String> partitionFieldName)
      throws Exception {
    return createTestTable(dbName, tableName, tableSdLoc, partitionFieldName, false);
  }

  public Table createTestTable(String dbName, String tableName, String tableSdLoc,
      Optional<String> partitionFieldName, boolean ignoreDbCreation) throws Exception {
    if (!ignoreDbCreation) {
      createTestDb(dbName);
    }

    Table tbl = org.apache.hadoop.hive.ql.metadata.Table.getEmptyTable(dbName, tableName);
    tbl.getSd().setLocation(tableSdLoc);
    tbl.getSd().getSerdeInfo().setParameters(ImmutableMap.of(HiveAvroSerDeManager.SCHEMA_URL, "/tmp/dummy"));

    if (partitionFieldName.isPresent()) {
      tbl.addToPartitionKeys(new FieldSchema(partitionFieldName.get(), "string", "some comment"));
    }

    this.localMetastoreClient.createTable(tbl);

    return tbl;
  }

  public Table createTestTable(String dbName, String tableName, List<String> partitionFieldNames) throws Exception {
    return createTestTable(dbName, tableName, "/tmp/" + tableName, partitionFieldNames, true);
  }

  public Table createTestTable(String dbName, String tableName, String tableSdLoc,
      List<String> partitionFieldNames, boolean ignoreDbCreation)
      throws Exception {

    if (!ignoreDbCreation) {
      createTestDb(dbName);
    }

    Table tbl = org.apache.hadoop.hive.ql.metadata.Table.getEmptyTable(dbName, tableName);
    tbl.getSd().setLocation(tableSdLoc);
    tbl.getSd().getSerdeInfo().setParameters(ImmutableMap.of(HiveAvroSerDeManager.SCHEMA_URL, "/tmp/dummy"));

    for (String partitionFieldName : partitionFieldNames) {
      tbl.addToPartitionKeys(new FieldSchema(partitionFieldName, "string", "some comment"));
    }

    this.localMetastoreClient.createTable(tbl);

    return tbl;
  }

  public void createTestDb(String dbName) throws Exception {
    Database db = new Database(dbName, "Some description", "/tmp/" + dbName, new HashMap<String, String>());
    try {
      this.localMetastoreClient.createDatabase(db);
    } catch (AlreadyExistsException e) {
      log.warn(dbName + " already exits");
    }
  }

  public Partition addTestPartition(Table tbl, List<String> values, int createTime) throws Exception {
    StorageDescriptor partitionSd = new StorageDescriptor();
    if (StringUtils.isNotBlank(tbl.getSd().getLocation())) {
      partitionSd.setLocation(tbl.getSd().getLocation() + values);
    } else {
      partitionSd.setLocation("/tmp/" + tbl.getTableName() + "/part1");
    }

    partitionSd.setSerdeInfo(
        new SerDeInfo("name", "serializationLib", ImmutableMap.of(HiveAvroSerDeManager.SCHEMA_URL, "/tmp/dummy")));
    partitionSd.setCols(tbl.getPartitionKeys());
    Partition partition =
        new Partition(values, tbl.getDbName(), tbl.getTableName(), 1, 1, partitionSd, new HashMap<String, String>());
    partition.setCreateTime(createTime);
    return this.getLocalMetastoreClient().add_partition(partition);

  }

  public org.apache.hadoop.hive.ql.metadata.Partition createDummyPartition(long createTime) {
    org.apache.hadoop.hive.ql.metadata.Partition partition = new org.apache.hadoop.hive.ql.metadata.Partition();
    Partition tPartition = new Partition();
    tPartition.setCreateTime((int) TimeUnit.SECONDS.convert(createTime, TimeUnit.MILLISECONDS));
    partition.setTPartition(tPartition);

    return partition;
  }
}
