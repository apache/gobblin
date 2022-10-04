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

package org.apache.gobblin.data.management.copy;
import com.google.common.base.Optional;
import com.google.common.collect.SetMultimap;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.data.management.copy.watermark.CopyableFileWatermarkGenerator;
import org.apache.gobblin.metrics.event.lineage.LineageInfo;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.util.request_allocation.RequestAllocatorConfig;
import org.apache.hadoop.fs.FileSystem;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.data.management.dataset.DatasetUtils;
import org.apache.gobblin.data.management.partition.CopyableDatasetRequestor;
import org.apache.gobblin.data.management.partition.FileSet;
import org.apache.gobblin.dataset.DatasetsFinder;
import org.apache.gobblin.dataset.IterableDatasetFinder;
import org.apache.gobblin.dataset.IterableDatasetFinderImpl;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.JobLauncherUtils;
import org.apache.gobblin.util.request_allocation.PriorityIterableBasedRequestAllocator;


@Slf4j
public class CopySourceTest {

  @Test
  public void testCopySource()
      throws Exception {

    SourceState state = new SourceState();

    state.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, "file:///");
    state.setProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, "file:///");
    state.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/target/dir");
    state.setProp(DatasetUtils.DATASET_PROFILE_CLASS_KEY, TestCopyableDatasetFinder.class.getName());

    CopySource source = new CopySource();

    List<WorkUnit> workunits = source.getWorkunits(state);
    workunits = JobLauncherUtils.flattenWorkUnits(workunits);

    Assert.assertEquals(workunits.size(), TestCopyableDataset.FILE_COUNT);

    Extract extract = workunits.get(0).getExtract();

    for (WorkUnit workUnit : workunits) {
      CopyableFile file = (CopyableFile) CopySource.deserializeCopyEntity(workUnit);
      Assert.assertTrue(file.getOrigin().getPath().toString().startsWith(TestCopyableDataset.ORIGIN_PREFIX));
      Assert.assertEquals(file.getDestinationOwnerAndPermission(), TestCopyableDataset.OWNER_AND_PERMISSION);
      Assert.assertEquals(workUnit.getProp(ServiceConfigKeys.WORK_UNIT_SIZE), String.valueOf(TestCopyableDataset.FILE_LENGTH));
      Assert.assertEquals(workUnit.getExtract(), extract);
    }
  }

  @Test
  public void testPartitionableDataset()
      throws Exception {

    SourceState state = new SourceState();

    state.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, "file:///");
    state.setProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, "file:///");
    state.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/target/dir");
    state.setProp(DatasetUtils.DATASET_PROFILE_CLASS_KEY,
        TestCopyablePartitionableDatasedFinder.class.getCanonicalName());

    CopySource source = new CopySource();

    List<WorkUnit> workunits = source.getWorkunits(state);
    workunits = JobLauncherUtils.flattenWorkUnits(workunits);

    Assert.assertEquals(workunits.size(), TestCopyableDataset.FILE_COUNT);

    Extract extractAbove = null;
    Extract extractBelow = null;

    for (WorkUnit workUnit : workunits) {
      CopyableFile copyableFile = (CopyableFile) CopySource.deserializeCopyEntity(workUnit);
      Assert.assertTrue(copyableFile.getOrigin().getPath().toString().startsWith(TestCopyableDataset.ORIGIN_PREFIX));
      Assert.assertEquals(copyableFile.getDestinationOwnerAndPermission(), TestCopyableDataset.OWNER_AND_PERMISSION);

      if (Integer.parseInt(copyableFile.getOrigin().getPath().getName()) < TestCopyablePartitionableDataset.THRESHOLD) {
        // should be in extractBelow
        if (extractBelow == null) {
          extractBelow = workUnit.getExtract();
        }
        Assert.assertEquals(workUnit.getExtract(), extractBelow);
      } else {
        // should be in extractAbove
        if (extractAbove == null) {
          extractAbove = workUnit.getExtract();
        }
        Assert.assertEquals(workUnit.getExtract(), extractAbove);
      }
    }

    Assert.assertNotNull(extractAbove);
    Assert.assertNotNull(extractBelow);
  }

  @Test
  public void testSubmitUnfulfilledRequestEvents()
      throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    SourceState state = new SourceState();

    state.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, "file:///");
    state.setProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, "file:///");
    state.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/target/dir");
    state.setProp(DatasetUtils.DATASET_PROFILE_CLASS_KEY,
        TestCopyablePartitionableDatasedFinder.class.getCanonicalName());
    state.setProp(CopySource.MAX_CONCURRENT_LISTING_SERVICES, 2);
    state.setProp(CopyConfiguration.MAX_COPY_PREFIX + ".size", "50");
    state.setProp(CopyConfiguration.MAX_COPY_PREFIX + ".copyEntities", 2);
    state.setProp(CopyConfiguration.STORE_REJECTED_REQUESTS_KEY,
        RequestAllocatorConfig.StoreRejectedRequestsConfig.ALL.name().toLowerCase());
    state.setProp(ConfigurationKeys.METRICS_CUSTOM_BUILDERS, "org.apache.gobblin.metrics.ConsoleEventReporterFactory");

    CopySource source = new CopySource();

    final FileSystem sourceFs = HadoopUtils.getSourceFileSystem(state);
    final FileSystem targetFs = HadoopUtils.getWriterFileSystem(state, 1, 0);

    int maxThreads = state
        .getPropAsInt(CopySource.MAX_CONCURRENT_LISTING_SERVICES, CopySource.DEFAULT_MAX_CONCURRENT_LISTING_SERVICES);

    final CopyConfiguration copyConfiguration = CopyConfiguration.builder(targetFs, state.getProperties()).build();

    MetricContext metricContext = Instrumented.getMetricContext(state, CopySource.class);
    EventSubmitter eventSubmitter = new EventSubmitter.Builder(metricContext, CopyConfiguration.COPY_PREFIX).build();
    DatasetsFinder<CopyableDatasetBase> datasetFinder = DatasetUtils
        .instantiateDatasetFinder(state.getProperties(), sourceFs, CopySource.DEFAULT_DATASET_PROFILE_CLASS_KEY,
            eventSubmitter, state);

    IterableDatasetFinder<CopyableDatasetBase> iterableDatasetFinder =
        datasetFinder instanceof IterableDatasetFinder ? (IterableDatasetFinder<CopyableDatasetBase>) datasetFinder
            : new IterableDatasetFinderImpl<>(datasetFinder);

    Iterator<CopyableDatasetRequestor> requestorIteratorWithNulls = Iterators
        .transform(iterableDatasetFinder.getDatasetsIterator(),
            new CopyableDatasetRequestor.Factory(targetFs, copyConfiguration, log));
    Iterator<CopyableDatasetRequestor> requestorIterator =
        Iterators.filter(requestorIteratorWithNulls, Predicates.<CopyableDatasetRequestor>notNull());

    Method m = CopySource.class.getDeclaredMethod("createRequestAllocator", CopyConfiguration.class, int.class);
    m.setAccessible(true);
    PriorityIterableBasedRequestAllocator<FileSet<CopyEntity>> allocator =
        (PriorityIterableBasedRequestAllocator<FileSet<CopyEntity>>) m.invoke(source, copyConfiguration, maxThreads);
    Iterator<FileSet<CopyEntity>> prioritizedFileSets =
        allocator.allocateRequests(requestorIterator, copyConfiguration.getMaxToCopy());
    List<FileSet<CopyEntity>> fileSetList = allocator.getRequestsExceedingAvailableResourcePool();
    Assert.assertEquals(fileSetList.size(), 2);

    FileSet<CopyEntity> fileSet = fileSetList.get(0);
    Assert.assertEquals(fileSet.getDataset().getUrn(), "/test");
    Assert.assertEquals(fileSet.getTotalEntities(), 5);
    Assert.assertEquals(fileSet.getTotalSizeInBytes(), 75);

    fileSet = fileSetList.get(1);
    Assert.assertEquals(fileSet.getDataset().getUrn(), "/test");
    Assert.assertEquals(fileSet.getTotalEntities(), 5);
    Assert.assertEquals(fileSet.getTotalSizeInBytes(), 75);
  }

  @Test(expectedExceptions = IOException.class)
  public void testFailIfAllAllocationRequestsRejected()
      throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    SourceState state = new SourceState();

    state.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, "file:///");
    state.setProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, "file:///");
    state.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/target/dir");
    state.setProp(DatasetUtils.DATASET_PROFILE_CLASS_KEY,
        TestCopyablePartitionableDatasedFinder.class.getCanonicalName());
    state.setProp(CopySource.MAX_CONCURRENT_LISTING_SERVICES, 2);
    state.setProp(CopyConfiguration.MAX_COPY_PREFIX + ".size", "50");
    state.setProp(CopyConfiguration.MAX_COPY_PREFIX + ".copyEntities", "2");
    state.setProp(CopyConfiguration.STORE_REJECTED_REQUESTS_KEY,
        RequestAllocatorConfig.StoreRejectedRequestsConfig.ALL.name().toLowerCase());
    state.setProp(ConfigurationKeys.METRICS_CUSTOM_BUILDERS, "org.apache.gobblin.metrics.ConsoleEventReporterFactory");

    CopySource source = new CopySource();

    final FileSystem sourceFs = HadoopUtils.getSourceFileSystem(state);
    final FileSystem targetFs = HadoopUtils.getWriterFileSystem(state, 1, 0);

    int maxThreads = state
        .getPropAsInt(CopySource.MAX_CONCURRENT_LISTING_SERVICES, CopySource.DEFAULT_MAX_CONCURRENT_LISTING_SERVICES);

    final CopyConfiguration copyConfiguration = CopyConfiguration.builder(targetFs, state.getProperties()).build();

    MetricContext metricContext = Instrumented.getMetricContext(state, CopySource.class);
    EventSubmitter eventSubmitter = new EventSubmitter.Builder(metricContext, CopyConfiguration.COPY_PREFIX).build();
    DatasetsFinder<CopyableDatasetBase> datasetFinder = DatasetUtils
        .instantiateDatasetFinder(state.getProperties(), sourceFs, CopySource.DEFAULT_DATASET_PROFILE_CLASS_KEY,
            eventSubmitter, state);

    IterableDatasetFinder<CopyableDatasetBase> iterableDatasetFinder =
        datasetFinder instanceof IterableDatasetFinder ? (IterableDatasetFinder<CopyableDatasetBase>) datasetFinder
            : new IterableDatasetFinderImpl<>(datasetFinder);

    Iterator<CopyableDatasetRequestor> requesterIteratorWithNulls = Iterators
        .transform(iterableDatasetFinder.getDatasetsIterator(),
            new CopyableDatasetRequestor.Factory(targetFs, copyConfiguration, log));
    Iterator<CopyableDatasetRequestor> requesterIterator =
        Iterators.filter(requesterIteratorWithNulls, Predicates.<CopyableDatasetRequestor>notNull());

    Method m = CopySource.class.getDeclaredMethod("createRequestAllocator", CopyConfiguration.class, int.class);
    m.setAccessible(true);
    PriorityIterableBasedRequestAllocator<FileSet<CopyEntity>> allocator =
        (PriorityIterableBasedRequestAllocator<FileSet<CopyEntity>>) m.invoke(source, copyConfiguration, maxThreads);
    Iterator<FileSet<CopyEntity>> prioritizedFileSets =
        allocator.allocateRequests(requesterIterator, copyConfiguration.getMaxToCopy());
    List<FileSet<CopyEntity>> fileSetList = allocator.getRequestsExceedingAvailableResourcePool();
    Assert.assertEquals(fileSetList.size(), 2);
    source.failJobIfAllRequestsRejected(allocator, prioritizedFileSets);
  }

  @Test
  public void testPassIfNoAllocationsRejected()
      throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    SourceState state = new SourceState();

    state.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, "file:///");
    state.setProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, "file:///");
    state.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/target/dir");
    state.setProp(DatasetUtils.DATASET_PROFILE_CLASS_KEY,
        TestCopyablePartitionableDatasedFinder.class.getCanonicalName());
    state.setProp(CopySource.MAX_CONCURRENT_LISTING_SERVICES, 2);
    state.setProp(CopyConfiguration.MAX_COPY_PREFIX + ".size", "100");
    state.setProp(CopyConfiguration.MAX_COPY_PREFIX + ".copyEntities", "10");
    state.setProp(CopyConfiguration.STORE_REJECTED_REQUESTS_KEY,
        RequestAllocatorConfig.StoreRejectedRequestsConfig.ALL.name().toLowerCase());
    state.setProp(ConfigurationKeys.METRICS_CUSTOM_BUILDERS, "org.apache.gobblin.metrics.ConsoleEventReporterFactory");

    CopySource source = new CopySource();

    final FileSystem sourceFs = HadoopUtils.getSourceFileSystem(state);
    final FileSystem targetFs = HadoopUtils.getWriterFileSystem(state, 1, 0);

    int maxThreads = state
        .getPropAsInt(CopySource.MAX_CONCURRENT_LISTING_SERVICES, CopySource.DEFAULT_MAX_CONCURRENT_LISTING_SERVICES);

    final CopyConfiguration copyConfiguration = CopyConfiguration.builder(targetFs, state.getProperties()).build();

    MetricContext metricContext = Instrumented.getMetricContext(state, CopySource.class);
    EventSubmitter eventSubmitter = new EventSubmitter.Builder(metricContext, CopyConfiguration.COPY_PREFIX).build();
    DatasetsFinder<CopyableDatasetBase> datasetFinder = DatasetUtils
        .instantiateDatasetFinder(state.getProperties(), sourceFs, CopySource.DEFAULT_DATASET_PROFILE_CLASS_KEY,
            eventSubmitter, state);

    IterableDatasetFinder<CopyableDatasetBase> iterableDatasetFinder =
        datasetFinder instanceof IterableDatasetFinder ? (IterableDatasetFinder<CopyableDatasetBase>) datasetFinder
            : new IterableDatasetFinderImpl<>(datasetFinder);

    Iterator<CopyableDatasetRequestor> requesterIteratorWithNulls = Iterators
        .transform(iterableDatasetFinder.getDatasetsIterator(),
            new CopyableDatasetRequestor.Factory(targetFs, copyConfiguration, log));
    Iterator<CopyableDatasetRequestor> requesterIterator =
        Iterators.filter(requesterIteratorWithNulls, Predicates.<CopyableDatasetRequestor>notNull());

    Method m = CopySource.class.getDeclaredMethod("createRequestAllocator", CopyConfiguration.class, int.class);
    m.setAccessible(true);
    PriorityIterableBasedRequestAllocator<FileSet<CopyEntity>> allocator =
        (PriorityIterableBasedRequestAllocator<FileSet<CopyEntity>>) m.invoke(source, copyConfiguration, maxThreads);
    Iterator<FileSet<CopyEntity>> prioritizedFileSets =
        allocator.allocateRequests(requesterIterator, copyConfiguration.getMaxToCopy());
    List<FileSet<CopyEntity>> fileSetList = allocator.getRequestsExceedingAvailableResourcePool();
    Assert.assertEquals(fileSetList.size(), 0);
    source.failJobIfAllRequestsRejected(allocator, prioritizedFileSets);
  }

  @Test
  public void testDefaultHiveDatasetShardTempPaths()
      throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    SourceState state = new SourceState();
    Properties copyProperties = new Properties();
    copyProperties.put(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/target");
    File tempDir = Files.createTempDir();
    String tempDirRoot = tempDir.getPath();
    tempDir.deleteOnExit();

    state.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, "file:///");
    state.setProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, "file:///");
    state.setProp("hive.dataset.whitelist", "testDB.table*"); // using a mock class so the finder will always find 3 tables regardless of this setting
    state.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/target/dir");
    state.setProp(DatasetUtils.DATASET_PROFILE_CLASS_KEY, MockHiveDatasetFinder.class.getName());
    state.setProp(ConfigurationKeys.USE_DATASET_LOCAL_WORK_DIR, "true");
    state.setProp("tempDirRoot", tempDirRoot);
    state.setProp(CopyConfiguration.STORE_REJECTED_REQUESTS_KEY,
        RequestAllocatorConfig.StoreRejectedRequestsConfig.ALL.name().toLowerCase());
    state.setProp(ConfigurationKeys.JOB_NAME_KEY, "jobName");
    state.setProp(ConfigurationKeys.JOB_ID_KEY, "jobId");
    CopySource source = new CopySource();

    List<WorkUnit> workunits = source.getWorkunits(state);
    workunits = JobLauncherUtils.flattenWorkUnits(workunits);
    Assert.assertEquals(workunits.size(), 6); // workunits are created for pre and post publish steps

    // workunits are not guaranteed to be created in any order, remove duplicate paths
    Set<String> datasetPaths = workunits.stream().map(w -> w.getProp(ConfigurationKeys.DATASET_DESTINATION_PATH)).collect(
        Collectors.toSet());

    for (int i = 0; i < 3; i++) {
      Assert.assertEquals(datasetPaths.contains(tempDirRoot + "/targetPath/testDB/table" + i), true);
    }
  }

  @Test (expectedExceptions = RuntimeException.class)
  public void testGetWorkUnitsExecutionFastFailure() {

    SourceState state = new SourceState();

    state.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, "file:///");
    state.setProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, "file:///");
    state.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/target/dir");
    state.setProp(DatasetUtils.DATASET_PROFILE_CLASS_KEY,
        TestCopyablePartitionableDatasedFinder.class.getCanonicalName());
    state.setProp(ConfigurationKeys.COPY_SOURCE_FILESET_WU_GENERATOR_CLASS, AlwaysThrowsMockedFileSetWorkUnitGenerator.class.getName());
    state.setProp(ConfigurationKeys.WORK_UNIT_GENERATOR_FAILURE_IS_FATAL, ConfigurationKeys.DEFAULT_WORK_UNIT_FAST_FAIL_ENABLED);

    CopySource source = new CopySource();
    // throws the runtime exception after encountering a failure generating the work units
    List<WorkUnit> workunits = source.getWorkunits(state);
    Assert.assertNull(workunits);
  }

  class AlwaysThrowsMockedFileSetWorkUnitGenerator extends CopySource.FileSetWorkUnitGenerator {

    public AlwaysThrowsMockedFileSetWorkUnitGenerator(CopyableDatasetBase copyableDataset, FileSet<CopyEntity> fileSet, State state,
        FileSystem targetFs, SetMultimap<FileSet<CopyEntity>, WorkUnit> workUnitList,
        Optional<CopyableFileWatermarkGenerator> watermarkGenerator, long minWorkUnitWeight,
        Optional<LineageInfo> lineageInfo) {
      super(copyableDataset, fileSet, state, targetFs, workUnitList, watermarkGenerator, minWorkUnitWeight,
          lineageInfo);
    }

    @Override
    public Void call(){
      throw new RuntimeException("boom!");
    }
  }
}
