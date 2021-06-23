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
package org.apache.gobblin.data.management.conversion.hive.avro;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.AvroUtils;


public class AvroSchemaManagerTest {
  @Test
  public void testGetSchemaFromUrlUsingHiveSchema() throws IOException, HiveException {
    FileSystem fs = FileSystem.getLocal(new Configuration());

    String jobId = "123";
    State state = new State();
    state.setProp(ConfigurationKeys.JOB_ID_KEY, jobId);

    AvroSchemaManager asm = new AvroSchemaManager(fs, state);
    Partition partition = getTestPartition(new Table("testDb", "testTable"));
    Path schemaPath = asm.getSchemaUrl(partition);

    Schema actualSchema = AvroUtils.parseSchemaFromFile(schemaPath, fs);
    String expectedSchema = new String(Files.readAllBytes(
        Paths.get(getClass().getClassLoader().getResource("avroSchemaManagerTest/expectedSchema.avsc").getFile())));
    Assert.assertEquals(actualSchema.toString(), expectedSchema);
  }

  private Partition getTestPartition(Table table) throws HiveException {
    Partition partition = new Partition(table, ImmutableMap.of("partition_key", "1"), null);
    StorageDescriptor sd = new StorageDescriptor();
    sd.setSerdeInfo(new SerDeInfo("avro", AvroSerDe.class.getName(), null));
    sd.setCols(Lists.newArrayList(new FieldSchema("foo", "int", null)));
    partition.getTPartition().setSd(sd);
    return partition;
  }
}
