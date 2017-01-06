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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import gobblin.data.management.copy.CopyConfiguration;
import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.policy.VersionSelectionPolicy;
import gobblin.data.management.version.DatasetVersion;
import gobblin.data.management.version.FileSystemDatasetVersion;
import gobblin.data.management.version.TimestampedDatasetVersion;
import gobblin.data.management.version.finder.VersionFinder;
import gobblin.dataset.Dataset;


/**
 * Test for {@link TimestampBasedCopyableDataset}.
 */
@Test(groups = {"gobblin.data.management.copy"})
public class TimestampBasedCopyableDatasetTest {

  private FileSystem localFs;

  @BeforeTest
  public void before()
      throws IOException {
    this.localFs = FileSystem.getLocal(new Configuration());
  }

  /**
   * Test the {@link TimestampBasedCopyableDataset} constructor with different config options.
   */
  @Test
  public void testConfigOptions() {
    Properties props = new Properties();
    props.put(TimestampBasedCopyableDataset.COPY_POLICY, TimeBasedCopyPolicyForTest.class.getName());
    props.put(TimestampBasedCopyableDataset.DATASET_VERSION_FINDER,
        TimestampedDatasetVersionFinderForTest.class.getName());
    TimestampBasedCopyableDataset copyabledataset1 =
        new TimestampBasedCopyableDataset(localFs, props, new Path("dummy"));
    Assert.assertEquals(copyabledataset1.getDatasetVersionFinder().getClass().getName(),
        TimestampedDatasetVersionFinderForTest.class.getName());
    Assert.assertEquals(copyabledataset1.getVersionSelectionPolicy().getClass().getName(),
        TimeBasedCopyPolicyForTest.class.getName());

    // Change the version finder
    props.put(TimestampBasedCopyableDataset.DATASET_VERSION_FINDER, VersionFinderDoNothingForTest.class.getName());
    TimestampBasedCopyableDataset copyabledataset2 =
        new TimestampBasedCopyableDataset(localFs, props, new Path("dummy"));
    Assert.assertEquals(copyabledataset2.getDatasetVersionFinder().getClass().getName(),
        VersionFinderDoNothingForTest.class.getName());
    Assert.assertEquals(copyabledataset2.getVersionSelectionPolicy().getClass().getName(),
        TimeBasedCopyPolicyForTest.class.getName());
  }

  /**
   * Test {@link TimestampBasedCopyableDataset.CopyableFileGenerator}'s logic to determine copyable files.
   */
  @Test
  public void testIsCopyableFile()
      throws IOException, InterruptedException {
    Path testRoot = new Path("testCopyableFileGenerator");
    Path srcRoot = new Path(testRoot, "datasetRoot");
    String versionDir = "dummyversion";
    Path versionPath = new Path(srcRoot, versionDir);
    Path targetDir = new Path(testRoot, "target");
    if (this.localFs.exists(testRoot)) {
      this.localFs.delete(testRoot, true);
    }
    this.localFs.mkdirs(versionPath);

    Path srcfile = new Path(versionPath, "file1");
    this.localFs.create(srcfile);
    this.localFs.mkdirs(targetDir);
    Properties props = new Properties();
    props.put(TimestampBasedCopyableDataset.COPY_POLICY, TimeBasedCopyPolicyForTest.class.getName());
    props.put(TimestampBasedCopyableDataset.DATASET_VERSION_FINDER,
        TimestampedDatasetVersionFinderForTest.class.getName());
    Path datasetRootPath = this.localFs.getFileStatus(srcRoot).getPath();
    TimestampBasedCopyableDataset copyabledataset = new TimestampBasedCopyableDataset(localFs, props, datasetRootPath);

    TimestampedDatasetVersion srcVersion = new TimestampedDatasetVersion(new DateTime(), versionPath);

    class SimpleCopyableFileGenerator extends TimestampBasedCopyableDataset.CopyableFileGenerator {
      public SimpleCopyableFileGenerator(TimestampBasedCopyableDataset copyableDataset, FileSystem srcFs,
          FileSystem targetFs, CopyConfiguration configuration, TimestampedDatasetVersion copyableVersion,
          ConcurrentLinkedQueue<CopyableFile> copyableFileList) {
        super(srcFs, targetFs, configuration, copyableDataset.datasetRoot(), configuration.getPublishDir(),
            copyableVersion.getDateTime(), copyableVersion.getPaths(), copyableFileList,
            copyableDataset.copyableFileFilter());
      }

      @Override
      protected CopyableFile generateCopyableFile(FileStatus singleFile, Path targetPath, long timestampFromPath,
          Path locationToCopy)
          throws IOException {
        CopyableFile mockCopyableFile = mock(CopyableFile.class);
        when(mockCopyableFile.getFileSet()).thenReturn(singleFile.getPath().toString());
        return mockCopyableFile;
      }
    }

    // When srcFile exists on src but not on target, srcFile should be included in the copyableFileList.
    CopyConfiguration configuration1 = mock(CopyConfiguration.class);
    when(configuration1.getPublishDir()).thenReturn(localFs.getFileStatus(targetDir).getPath());
    ConcurrentLinkedQueue<CopyableFile> copyableFileList1 = new ConcurrentLinkedQueue<>();
    TimestampBasedCopyableDataset.CopyableFileGenerator copyFileGenerator1 =
        new SimpleCopyableFileGenerator(copyabledataset, localFs, localFs, configuration1, srcVersion,
            copyableFileList1);
    copyFileGenerator1.run();
    Assert.assertEquals(copyableFileList1.size(), 1);
    Assert.assertEquals(copyableFileList1.poll().getFileSet(), localFs.getFileStatus(srcfile).getPath().toString());

    // When files exist on both locations but with different timestamp, the result should only include newer src files.
    String noNeedToCopyFile = "file2";
    Path oldSrcFile = new Path(versionPath, noNeedToCopyFile);
    this.localFs.create(oldSrcFile);
    Thread.sleep(100);
    Path newTargetfile = new Path(targetDir, new Path(versionDir, noNeedToCopyFile));
    this.localFs.create(newTargetfile);
    copyFileGenerator1.run();
    Assert.assertEquals(copyableFileList1.size(), 1);
    Assert.assertEquals(copyableFileList1.poll().getFileSet(), localFs.getFileStatus(srcfile).getPath().toString());

    // When srcFile exists on both locations and have the same modified timestamp, it should not be included in copyableFileList.
    CopyConfiguration configuration2 = mock(CopyConfiguration.class);
    when(configuration2.getPublishDir()).thenReturn(localFs.getFileStatus(datasetRootPath).getPath());
    ConcurrentLinkedQueue<CopyableFile> copyableFileList2 = new ConcurrentLinkedQueue<>();
    TimestampBasedCopyableDataset.CopyableFileGenerator copyFileGenerator2 =
        new SimpleCopyableFileGenerator(copyabledataset, localFs, localFs, configuration2, srcVersion,
            copyableFileList2);
    copyFileGenerator2.run();
    Assert.assertEquals(copyableFileList2.size(), 0);
    this.localFs.delete(testRoot, true);
  }

  /**
   * Test {@link TimestampBasedCopyableDataset.CopyableFileGenerator} when src location is empty and also when it is null.
   */
  @Test(expectedExceptions = RuntimeException.class)
  public void testCopyableFileGenerator() {
    Properties props = new Properties();
    props.put(TimestampBasedCopyableDataset.COPY_POLICY, TimeBasedCopyPolicyForTest.class.getName());
    props.put(TimestampBasedCopyableDataset.DATASET_VERSION_FINDER,
        TimestampedDatasetVersionFinderForTest.class.getName());
    TimestampBasedCopyableDataset copyabledataset =
        new TimestampBasedCopyableDataset(localFs, props, new Path("dummy"));
    CopyConfiguration configuration = mock(CopyConfiguration.class);
    when(configuration.getPublishDir()).thenReturn(new Path("publishDir"));
    ConcurrentLinkedQueue<CopyableFile> copyableFileList = new ConcurrentLinkedQueue<>();
    // The src path is empty.
    TimestampedDatasetVersion emptyVersion = new TimestampedDatasetVersion(new DateTime(), new Path("dummy2"));
    TimestampBasedCopyableDataset.CopyableFileGenerator emptyGenerator =
        copyabledataset.getCopyableFileGenetator(localFs, configuration, emptyVersion, copyableFileList);
    emptyGenerator.run();
    Assert.assertEquals(copyableFileList.size(), 0);

    // The src path is null.
    TimestampedDatasetVersion versionHasNullPath = new TimestampedDatasetVersion(new DateTime(), null);
    TimestampBasedCopyableDataset.CopyableFileGenerator exceptionGenerator =
        copyabledataset.getCopyableFileGenetator(localFs, configuration, versionHasNullPath, copyableFileList);
    exceptionGenerator.run();
  }

  /**
   * Test the parallel execution to get copyable files in {@link TimestampBasedCopyableDataset#getCopyableFiles(FileSystem, CopyConfiguration)}.
   */
  @Test
  public void testGetCopyableFiles()
      throws IOException {
    Properties props = new Properties();
    props.put(TimestampBasedCopyableDataset.COPY_POLICY, TimeBasedCopyPolicyForTest.class.getName());
    props.put(TimestampBasedCopyableDataset.DATASET_VERSION_FINDER,
        TimestampedDatasetVersionFinderForTest.class.getName());
    TimestampBasedCopyableDataset copyabledataset =
        new TimestampBasedCopyableDatasetForTest(localFs, props, new Path("/data/tracking/PVE"));
    Collection<CopyableFile> copyableFiles = copyabledataset.getCopyableFiles(localFs, null);
    /**
     * {@link TimestampedDatasetVersionFinderForTest} will return three versions, and each version will contain two files.
     * So the total number of copyableFiles should be 6, and all should follow the pattern: dummy\/[\\d]\*\/file[12].
     */
    Assert.assertEquals(copyableFiles.size(), 6);
    Pattern pattern = Pattern.compile("dummy/[\\d]*/file[12]");
    Set<String> resultFilesets = Sets.newHashSet();
    for (CopyableFile copyableFile : copyableFiles) {
      String copyableFileset = copyableFile.getFileSet();
      Assert.assertTrue(pattern.matcher(copyableFileset).matches());
      resultFilesets.add(copyableFileset);
    }
    Assert.assertEquals(resultFilesets.size(), 6);
  }

  public static class TimestampBasedCopyableDatasetForTest extends TimestampBasedCopyableDataset {

    public TimestampBasedCopyableDatasetForTest(FileSystem fs, Properties props, Path datasetRoot)
        throws IOException {
      super(fs, props, datasetRoot);
    }

    @Override
    protected CopyableFileGenerator getCopyableFileGenetator(FileSystem targetFs, CopyConfiguration configuration,
        TimestampedDatasetVersion copyableVersion, ConcurrentLinkedQueue<CopyableFile> copyableFileList) {
      return new CopyableFileGeneratorForTest(copyableFileList, copyableVersion);
    }

    private class CopyableFileGeneratorForTest extends TimestampBasedCopyableDataset.CopyableFileGenerator {

      public CopyableFileGeneratorForTest(ConcurrentLinkedQueue<CopyableFile> copyableFileList,
          TimestampedDatasetVersion copyableVersion) {
        super(null, null, null, null, null, null, null, null, null);
        this.copyableFileList = copyableFileList;
        this.copyableVersion = copyableVersion;
      }

      ConcurrentLinkedQueue<CopyableFile> copyableFileList;
      TimestampedDatasetVersion copyableVersion;

      @Override
      public void run() {
        CopyableFile mockCopyableFile1 = mock(CopyableFile.class);
        String file1 = new Path(copyableVersion.getPaths().iterator().next(), "file1").toString();
        when(mockCopyableFile1.getFileSet()).thenReturn(file1);

        CopyableFile mockCopyableFile2 = mock(CopyableFile.class);
        String file2 = new Path(copyableVersion.getPaths().iterator().next(), "file2").toString();
        when(mockCopyableFile2.getFileSet()).thenReturn(file2);

        try {
          Thread.sleep(new Random().nextInt(3000));
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        copyableFileList.add(mockCopyableFile1);
        copyableFileList.add(mockCopyableFile2);
      }
    }
  }

  public static class TimeBasedCopyPolicyForTest implements VersionSelectionPolicy<TimestampedDatasetVersion> {
    public TimeBasedCopyPolicyForTest(Properties props) {
      //do nothing
    }

    @Override
    public Class<? extends FileSystemDatasetVersion> versionClass() {
      return null;
    }

    @Override
    public Collection<TimestampedDatasetVersion> listSelectedVersions(List<TimestampedDatasetVersion> allVersions) {
      return allVersions;
    }
  }

  public static class TimestampedDatasetVersionFinderForTest implements VersionFinder<TimestampedDatasetVersion> {
    DateTime start;
    int range;

    public TimestampedDatasetVersionFinderForTest(FileSystem fs, Properties props) {
      start = new DateTime(2006, 1, 1, 0, 0);
      range = 3650;
    }

    @Override
    public Class<? extends DatasetVersion> versionClass() {
      return null;
    }

    @Override
    public Collection<TimestampedDatasetVersion> findDatasetVersions(Dataset dataset)
        throws IOException {
      Random ran = new Random();
      Path dummyPath = new Path("dummy");
      DateTime dt1 = start.plusDays(ran.nextInt(range));
      Path path1 = new Path(dummyPath, Long.toString(dt1.getMillis()));
      TimestampedDatasetVersion version1 = new TimestampedDatasetVersion(dt1, path1);

      DateTime dt2 = dt1.plusDays(ran.nextInt(range));
      Path path2 = new Path(dummyPath, Long.toString(dt2.getMillis()));
      TimestampedDatasetVersion version2 = new TimestampedDatasetVersion(dt2, path2);

      DateTime dt3 = dt2.plusDays(ran.nextInt(range));
      Path path3 = new Path(dummyPath, Long.toString(dt3.getMillis()));
      TimestampedDatasetVersion version3 = new TimestampedDatasetVersion(dt3, path3);

      return Lists.newArrayList(version1, version2, version3);
    }
  }

  public static class VersionFinderDoNothingForTest implements VersionFinder<TimestampedDatasetVersion> {

    public VersionFinderDoNothingForTest(FileSystem fs, Properties props) {
    }

    @Override
    public Class<? extends DatasetVersion> versionClass() {
      return null;
    }

    @Override
    public Collection<TimestampedDatasetVersion> findDatasetVersions(Dataset dataset)
        throws IOException {
      return Lists.newArrayList();
    }
  }
}
