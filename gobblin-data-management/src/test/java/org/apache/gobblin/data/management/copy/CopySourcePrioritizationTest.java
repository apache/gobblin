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

package gobblin.data.management.copy;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.data.management.copy.prioritization.FileSetComparator;
import gobblin.data.management.dataset.DatasetUtils;
import gobblin.data.management.partition.FileSet;
import gobblin.data.management.partition.FileSetResourceEstimator;
import gobblin.dataset.Dataset;
import gobblin.dataset.IterableDatasetFinder;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.JobLauncherUtils;

import lombok.AllArgsConstructor;
import lombok.Getter;


public class CopySourcePrioritizationTest {

  @Test
  public void testNoPrioritization() throws Exception {
    SourceState state = new SourceState();

    state.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, "file:///");
    state.setProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, "file:///");
    state.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/target/dir");
    state.setProp(DatasetUtils.DATASET_PROFILE_CLASS_KEY,
        MyFinder.class.getName());

    CopySource source = new CopySource();

    List<WorkUnit> workunits = source.getWorkunits(state);
    workunits = JobLauncherUtils.flattenWorkUnits(workunits);

    Assert.assertEquals(workunits.size(), MyFinder.DATASETS * MyDataset.FILE_SETS * MyFileSet.FILES);
  }

  @Test
  public void testUnprioritizedFileLimit() throws Exception {
    SourceState state = new SourceState();

    state.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, "file:///");
    state.setProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, "file:///");
    state.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/target/dir");
    state.setProp(DatasetUtils.DATASET_PROFILE_CLASS_KEY,
        MyFinder.class.getName());
    // Disable parallel listing to make work unit selection deterministic
    state.setProp(CopySource.MAX_CONCURRENT_LISTING_SERVICES, 1);
    state.setProp(CopyConfiguration.MAX_COPY_PREFIX + "." + CopyResourcePool.ENTITIES_KEY, 10);
    state.setProp(CopyConfiguration.MAX_COPY_PREFIX + "." + CopyResourcePool.TOLERANCE_KEY, 1);

    CopySource source = new CopySource();

    List<WorkUnit> workunits = source.getWorkunits(state);
    workunits = JobLauncherUtils.flattenWorkUnits(workunits);

    // Check limited to 10 entities
    Assert.assertEquals(workunits.size(), 10);

    List<String> paths = extractPaths(workunits);

    Assert.assertTrue(paths.contains("d0.fs0.f1"));
    Assert.assertTrue(paths.contains("d0.fs0.f2"));
    Assert.assertTrue(paths.contains("d0.fs1.f1"));
    Assert.assertTrue(paths.contains("d0.fs1.f2"));
    Assert.assertTrue(paths.contains("d0.fs2.f1"));
    Assert.assertTrue(paths.contains("d0.fs2.f2"));
    Assert.assertTrue(paths.contains("d0.fs3.f1"));
    Assert.assertTrue(paths.contains("d0.fs3.f2"));
    Assert.assertTrue(paths.contains("d1.fs0.f1"));
    Assert.assertTrue(paths.contains("d1.fs0.f2"));

  }

  // This test uses a prioritizer that preferentially copies the lower file sets of each dataset
  @Test
  public void testPrioritization() throws Exception {
    SourceState state = new SourceState();

    state.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, "file:///");
    state.setProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, "file:///");
    state.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/target/dir");
    state.setProp(DatasetUtils.DATASET_PROFILE_CLASS_KEY,
        MyFinder.class.getName());
    state.setProp(CopyConfiguration.PRIORITIZER_ALIAS_KEY, MyPrioritizer.class.getName());
    state.setProp(CopyConfiguration.MAX_COPY_PREFIX + "." + CopyResourcePool.ENTITIES_KEY, 8);
    state.setProp(CopyConfiguration.MAX_COPY_PREFIX + "." + CopyResourcePool.TOLERANCE_KEY, 1);

    CopySource source = new CopySource();

    List<WorkUnit> workunits = source.getWorkunits(state);
    workunits = JobLauncherUtils.flattenWorkUnits(workunits);

    Assert.assertEquals(workunits.size(), 8);

    List<String> paths = extractPaths(workunits);

    Assert.assertTrue(paths.contains("d0.fs0.f1"));
    Assert.assertTrue(paths.contains("d0.fs0.f2"));
    Assert.assertTrue(paths.contains("d0.fs1.f1"));
    Assert.assertTrue(paths.contains("d0.fs1.f2"));
    Assert.assertTrue(paths.contains("d1.fs0.f1"));
    Assert.assertTrue(paths.contains("d1.fs0.f2"));
    Assert.assertTrue(paths.contains("d1.fs1.f1"));
    Assert.assertTrue(paths.contains("d1.fs1.f2"));
  }

  private List<String> extractPaths(List<WorkUnit> workUnits) {
    List<String> paths = Lists.newArrayList();
    for (WorkUnit wu : workUnits) {
      CopyableFile cf = (CopyableFile) CopySource.deserializeCopyEntity(wu);
      paths.add(cf.getOrigin().getPath().toString());
    }
    return paths;
  }

  public static class MyFinder implements IterableDatasetFinder<IterableCopyableDataset> {
    public static final int DATASETS = 2;

    @Override
    public List<IterableCopyableDataset> findDatasets()
        throws IOException {
      return null;
    }

    @Override
    public Path commonDatasetRoot() {
      return new Path("/");
    }

    @Override
    public Iterator<IterableCopyableDataset> getDatasetsIterator()
        throws IOException {
      List<IterableCopyableDataset> datasets = Lists.newArrayList();
      for (int i = 0; i < DATASETS; i++) {
        datasets.add(new MyDataset("d" + i));
      }
      return datasets.iterator();
    }
  }

  @AllArgsConstructor
  public static class MyDataset implements IterableCopyableDataset {

    public static final int FILE_SETS = 4;

    private final String name;

    @Override
    public String datasetURN() {
      return this.name;
    }

    @Override
    public Iterator<FileSet<CopyEntity>> getFileSetIterator(FileSystem targetFs, CopyConfiguration configuration)
        throws IOException {
      List<FileSet<CopyEntity>> fileSets = Lists.newArrayList();
      for (int i = 0; i < FILE_SETS; i++) {
        fileSets.add(new MyFileSet(this.name + ".fs" + Integer.toString(i), this, i));
      }
      return fileSets.iterator();
    }
  }

  public static class MyFileSet extends FileSet<CopyEntity> {

    public static final int FILES = 2;

    @Getter
    private final int filesetNumberInDataset;

    public MyFileSet(String name, Dataset dataset, int filesetNumberInDataset) {
      super(name, dataset);
      this.filesetNumberInDataset = filesetNumberInDataset;
    }

    @Override
    protected Collection<CopyEntity> generateCopyEntities()
        throws IOException {
      CopyableFile cf1 = createCopyableFile(getName() + ".f1", Integer.toString(this.filesetNumberInDataset));
      CopyableFile cf2 = createCopyableFile(getName() + ".f2", Integer.toString(this.filesetNumberInDataset));
      return Lists.<CopyEntity>newArrayList(cf1, cf2);
    }
  }

  private static CopyableFile createCopyableFile(String path, String fileSet) {
    return new CopyableFile(new FileStatus(0, false, 0, 0, 0, new Path(path)), new Path(path),
        new OwnerAndPermission("owner", "group", FsPermission.getDefault()), null, null,
        PreserveAttributes.fromMnemonicString(""), fileSet, 0, 0, Maps.<String, String>newHashMap());
  }

  public static class MyPrioritizer implements FileSetComparator {
    @Override
    public int compare(FileSet<CopyEntity> o1, FileSet<CopyEntity> o2) {
      return Integer.compare(((MyFileSet) o1).getFilesetNumberInDataset(), ((MyFileSet) o2) .getFilesetNumberInDataset());
    }
  }

}
