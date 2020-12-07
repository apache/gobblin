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
package org.apache.gobblin.data.management.conversion.hive.dataset;

import com.google.common.base.Optional;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.gobblin_scopes.JobScopeInstance;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.conversion.hive.publisher.HiveConvertPublisher;
import org.apache.gobblin.data.management.conversion.hive.source.HiveAvroToOrcSource;
import org.apache.gobblin.data.management.conversion.hive.source.HiveWorkUnit;
import org.apache.gobblin.data.management.conversion.hive.utils.LineageUtils;
import org.apache.gobblin.dataset.DatasetDescriptor;
import org.apache.gobblin.dataset.Descriptor;
import org.apache.gobblin.dataset.HiveToHdfsDatasetResolver;
import org.apache.gobblin.dataset.HiveToHdfsDatasetResolverFactory;
import org.apache.gobblin.metrics.event.lineage.LineageEventBuilder;
import org.apache.gobblin.metrics.event.lineage.LineageInfo;
import org.apache.gobblin.runtime.TaskState;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.data.management.conversion.hive.dataset.ConvertibleHiveDataset.ConversionConfig;
import org.apache.gobblin.hive.HiveMetastoreClientPool;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.io.GsonInterfaceAdapter;

import static org.mockito.Mockito.when;


@Test(groups = { "gobblin.data.management.conversion" })
public class ConvertibleHiveDatasetTest {

  /** Lineage info ser/de */
  private static final Type DESCRIPTOR_LIST_TYPE = new TypeToken<ArrayList<Descriptor>>(){}.getType();
  private static final Gson GSON =
      new GsonBuilder().registerTypeAdapterFactory(new GsonInterfaceAdapter(Descriptor.class)).create();


  /**
   * Test if lineage information is properly set in the workunit for convertible hive datasets
   */
  @Test
  public void testLineageInfo() throws Exception {
    String testConfFilePath = "convertibleHiveDatasetTest/flattenedAndNestedOrc.conf";
    Config config = ConfigFactory.parseResources(testConfFilePath).getConfig("hive.conversion.avro");
    // Set datasetResolverFactory to convert Hive Lineage event to Hdfs Lineage event
    ConvertibleHiveDataset testConvertibleDataset = createTestConvertibleDataset(config);
    HiveWorkUnit workUnit = new HiveWorkUnit(testConvertibleDataset);
    workUnit.setProp("gobblin.broker.lineageInfo.datasetResolverFactory",
        HiveToHdfsDatasetResolverFactory.class.getName());
    workUnit.setProp(ConfigurationKeys.JOB_ID_KEY, "123456");

    Optional<LineageInfo> lineageInfo = LineageInfo.getLineageInfo(getSharedJobBroker(workUnit.getProperties()));
    HiveAvroToOrcSource src = new HiveAvroToOrcSource();
    Assert.assertTrue(LineageUtils.shouldSetLineageInfo(workUnit));
    if (LineageUtils.shouldSetLineageInfo(workUnit)) {
      src.setSourceLineageInfo(workUnit,
          lineageInfo);
    }
    // TaskState is passed to the publisher, hence test should mimic the same behavior
    TaskState taskState = new TaskState(new WorkUnitState(workUnit));
    if (LineageUtils.shouldSetLineageInfo(taskState)) {
      HiveConvertPublisher.setDestLineageInfo(taskState, lineageInfo);
    }
    Properties props = taskState.getProperties();

    // Assert that there are two eventBuilders for nestedOrc and flattenedOrc
    Collection<LineageEventBuilder> lineageEventBuilders = LineageInfo.load(Collections.singleton(taskState));
    Assert.assertEquals(lineageEventBuilders.size(), 2);

    // Asset that lineage name is correct
    Assert.assertEquals(props.getProperty("gobblin.event.lineage.name"), "/tmp/test");

    // Assert that source is correct for lineage event
    Assert.assertTrue(props.containsKey("gobblin.event.lineage.source"));
    DatasetDescriptor sourceDD =
        GSON.fromJson(props.getProperty("gobblin.event.lineage.source"), DatasetDescriptor.class);
    Assert.assertEquals(sourceDD.getPlatform(), "file");
    Assert.assertEquals(sourceDD.getName(), "/tmp/test");
    Assert.assertEquals(sourceDD.getMetadata().get(HiveToHdfsDatasetResolver.HIVE_TABLE), "db1.tb1");

    // Assert that first dest is correct for lineage event
    Assert.assertTrue(props.containsKey("gobblin.event.lineage.branch.1.destination"));
    DatasetDescriptor destDD1 =
        (DatasetDescriptor) firstDescriptor(props, "gobblin.event.lineage.branch.1.destination");
    Assert.assertEquals(destDD1.getPlatform(), "file");
    Assert.assertEquals(destDD1.getName(), "/tmp/data_nestedOrc/db1/tb1/final");
    Assert.assertEquals(destDD1.getMetadata().get(HiveToHdfsDatasetResolver.HIVE_TABLE),
        "db1_nestedOrcDb.tb1_nestedOrc");

    // Assert that second dest is correct for lineage event
    Assert.assertTrue(props.containsKey("gobblin.event.lineage.branch.2.destination"));
    DatasetDescriptor destDD2 =
        (DatasetDescriptor) firstDescriptor(props, "gobblin.event.lineage.branch.2.destination");
    Assert.assertEquals(destDD2.getPlatform(), "file");
    Assert.assertEquals(destDD2.getName(), "/tmp/data_flattenedOrc/db1/tb1/final");
    Assert.assertEquals(destDD2.getMetadata().get(HiveToHdfsDatasetResolver.HIVE_TABLE),
        "db1_flattenedOrcDb.tb1_flattenedOrc");
  }

  private Descriptor firstDescriptor(Properties prop, String destinationKey) {
    List<Descriptor> descriptors = GSON.fromJson(prop.getProperty(destinationKey), DESCRIPTOR_LIST_TYPE);
    return descriptors.get(0);
  }

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
  public void testInvalidFormat()
      throws Exception {

    Config config = ConfigFactory.parseMap(ImmutableMap.<String, String>of("destinationFormats", "flattenedOrc,nestedOrc"));
    ConvertibleHiveDataset cd = createTestConvertibleDataset(config);

    Assert.assertFalse(cd.getConversionConfigForFormat("invalidFormat").isPresent());
  }

  @Test
  public void testDisableFormat()
      throws Exception {

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

  public static ConvertibleHiveDataset createTestConvertibleDataset(Config config)
      throws URISyntaxException {
    Table table = getTestTable("db1", "tb1");
    FileSystem mockFs = Mockito.mock(FileSystem.class);
    when(mockFs.getUri()).thenReturn(new URI("test"));
    ConvertibleHiveDataset cd =
        new ConvertibleHiveDataset(mockFs, Mockito.mock(HiveMetastoreClientPool.class), new org.apache.hadoop.hive.ql.metadata.Table(
            table), new Properties(), config);
    return cd;
  }

  public static Table getTestTable(String dbName, String tableName) {
    Table table = new Table();
    table.setDbName(dbName);
    table.setTableName(tableName);
    table.setTableType(TableType.EXTERNAL_TABLE.name());
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("/tmp/test");
    table.setSd(sd);
    return table;
  }

  public static SharedResourcesBroker<GobblinScopeTypes> getSharedJobBroker(Properties props) {
    SharedResourcesBroker<GobblinScopeTypes> instanceBroker = SharedResourcesBrokerFactory
        .createDefaultTopLevelBroker(ConfigFactory.parseProperties(props), GobblinScopeTypes.GLOBAL.defaultScopeInstance());
    SharedResourcesBroker<GobblinScopeTypes> jobBroker = instanceBroker
        .newSubscopedBuilder(new JobScopeInstance("ConvertibleHiveDatasetLineageEventTest", String.valueOf(System.currentTimeMillis())))
        .build();
    return jobBroker;
  }
}
