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
package gobblin.data.management.conversion.hive.dataset;

import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.data.management.conversion.hive.dataset.ConvertibleHiveDataset.ConversionConfig;
import gobblin.hive.HiveMetastoreClientPool;
import gobblin.util.ConfigUtils;

@Test(groups = { "gobblin.data.management.conversion" })
public class ConvertibleHiveDatasetTest {

  @Test
  public void testFlattenedOrcConfig() throws Exception {
    String testConfFilePath = "convertibleHiveDatasetTest/flattenedOrc.conf";
    Config config = ConfigFactory.parseResources(testConfFilePath).getConfig("hive.conversion.avro");

    ConvertibleHiveDataset cd = createTestConvertibleDataset(config);

    Assert.assertEquals(cd.getDestFormats(), ImmutableSet.of("flattenedOrc"));
    Assert.assertTrue(cd.getConversionConfigForFormat("flattenedOrc").isPresent());

    validateFlattenedConfig(cd.getConversionConfigForFormat("flattenedOrc").get());

  }

  @Test
  public void testFlattenedAndNestedOrcConfig() throws Exception {
    String testConfFilePath = "convertibleHiveDatasetTest/flattenedAndNestedOrc.conf";
    Config config = ConfigFactory.parseResources(testConfFilePath).getConfig("hive.conversion.avro");

    ConvertibleHiveDataset cd = createTestConvertibleDataset(config);

    Assert.assertEquals(cd.getDestFormats(), ImmutableSet.of("flattenedOrc", "nestedOrc"));
    Assert.assertTrue(cd.getConversionConfigForFormat("flattenedOrc").isPresent());
    Assert.assertTrue(cd.getConversionConfigForFormat("nestedOrc").isPresent());

    validateFlattenedConfig(cd.getConversionConfigForFormat("flattenedOrc").get());

    validateNestedOrc(cd.getConversionConfigForFormat("nestedOrc").get());

  }

  @Test
  public void testFlattenedAndNestedOrcProps() throws Exception {
    String testConfFilePath = "convertibleHiveDatasetTest/flattenedAndNestedOrc.properties";

    Properties jobProps = new Properties();
    try (final InputStream stream = ConvertibleHiveDatasetTest.class.getClassLoader().getResourceAsStream(testConfFilePath)) {
      jobProps.load(stream);
    }

    Config config = ConfigUtils.propertiesToConfig(jobProps).getConfig("hive.conversion.avro");

    ConvertibleHiveDataset cd = createTestConvertibleDataset(config);

    Assert.assertEquals(cd.getDestFormats(), ImmutableSet.of("flattenedOrc", "nestedOrc"));
    Assert.assertTrue(cd.getConversionConfigForFormat("flattenedOrc").isPresent());
    Assert.assertTrue(cd.getConversionConfigForFormat("nestedOrc").isPresent());

    validateFlattenedConfig(cd.getConversionConfigForFormat("flattenedOrc").get());

    validateNestedOrc(cd.getConversionConfigForFormat("nestedOrc").get());

  }

  @Test
  public void testInvalidFormat() {

    Config config = ConfigFactory.parseMap(ImmutableMap.<String, String>of("destinationFormats", "flattenedOrc,nestedOrc"));
    ConvertibleHiveDataset cd = createTestConvertibleDataset(config);

    Assert.assertFalse(cd.getConversionConfigForFormat("invalidFormat").isPresent());
  }

  @Test
  public void testDisableFormat() {

    Config config = ConfigFactory.parseMap(ImmutableMap.<String, String> builder()
        .put("destinationFormats", "flattenedOrc")
        .put("flattenedOrc.destination.tableName","d")
        .put("flattenedOrc.destination.dbName","d")
        .put("flattenedOrc.destination.dataPath","d")
        .put("nestedOrc.destination.tableName","d")
        .put("nestedOrc.destination.dbName","d")
        .put("nestedOrc.destination.dataPath","d")
        .build());

    ConvertibleHiveDataset cd = createTestConvertibleDataset(config);

    Assert.assertTrue(cd.getConversionConfigForFormat("flattenedOrc").isPresent());
    Assert.assertFalse(cd.getConversionConfigForFormat("nestedOrc").isPresent());

  }

  private void validateFlattenedConfig(ConversionConfig conversionConfig) {

    Assert.assertEquals(conversionConfig.getDestinationDbName(), "db1_flattenedOrcDb");
    Assert.assertEquals(conversionConfig.getDestinationTableName(), "tb1_flattenedOrc");
    Assert.assertEquals(conversionConfig.getDestinationDataPath(), "/tmp/data_flattenedOrc/db1/tb1");

    Assert.assertEquals(conversionConfig.getClusterBy(), ImmutableList.of("c1", "c2"));
    Assert.assertEquals(conversionConfig.getNumBuckets().get(), Integer.valueOf(4));

    Properties hiveProps = new Properties();
    hiveProps.setProperty("mapred.map.tasks", "10,12");
    hiveProps.setProperty("hive.merge.mapfiles", "false");
    Assert.assertEquals(conversionConfig.getHiveRuntimeProperties(), hiveProps);
  }

  private void validateNestedOrc(ConversionConfig conversionConfig) {

    Assert.assertEquals(conversionConfig.getDestinationDbName(), "db1_nestedOrcDb");
    Assert.assertEquals(conversionConfig.getDestinationTableName(), "tb1_nestedOrc");
    Assert.assertEquals(conversionConfig.getDestinationViewName().get(), "tb1_view");
    Assert.assertEquals(conversionConfig.getDestinationDataPath(), "/tmp/data_nestedOrc/db1/tb1");

    Assert.assertEquals(conversionConfig.isUpdateViewAlwaysEnabled(), false);
    Assert.assertEquals(conversionConfig.getClusterBy(), ImmutableList.of("c3", "c4"));
    Assert.assertEquals(conversionConfig.getNumBuckets().get(), Integer.valueOf(5));

    Properties hiveProps = new Properties();
    hiveProps = new Properties();
    hiveProps.setProperty("mapred.map.tasks", "12");
    Assert.assertEquals(conversionConfig.getHiveRuntimeProperties(), hiveProps);
  }

  public static ConvertibleHiveDataset createTestConvertibleDataset(Config config) {
    Table table = getTestTable("db1", "tb1");
    ConvertibleHiveDataset cd =
        new ConvertibleHiveDataset(Mockito.mock(FileSystem.class), Mockito.mock(HiveMetastoreClientPool.class), new org.apache.hadoop.hive.ql.metadata.Table(
            table), new Properties(), config);
    return cd;
  }

  public static Table getTestTable(String dbName, String tableName) {
    Table table = new Table();
    table.setDbName(dbName);
    table.setTableName(tableName);
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("/tmp/test");
    table.setSd(sd);
    return table;
  }
}
