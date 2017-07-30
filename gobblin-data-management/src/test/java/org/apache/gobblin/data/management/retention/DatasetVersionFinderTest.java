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

package gobblin.data.management.retention;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import gobblin.data.management.retention.dataset.CleanableDataset;
import gobblin.data.management.retention.version.DatasetVersion;
import gobblin.data.management.retention.version.StringDatasetVersion;
import gobblin.data.management.retention.version.finder.DatasetVersionFinder;
import gobblin.dataset.FileSystemDataset;


public class DatasetVersionFinderTest {

  @Test
  public void test() throws IOException {
    FileSystem fs = mock(FileSystem.class);

    String datasetPathStr = "/path/to/dataset";
    String dataset1 = "datasetVersion1";
    String dataset2 = "datasetVersion2";
    Path datasetPath = new Path(datasetPathStr);
    Path globbedPath = new Path(datasetPathStr + "/*");
    Path datasetVersion1 = new Path(datasetPathStr + "/" + dataset1);
    Path datasetVersion2 = new Path(datasetPathStr + "/" + dataset2);

    when(fs.globStatus(globbedPath)).
        thenReturn(new FileStatus[]{new FileStatus(0, true, 0, 0, 0, datasetVersion1),
            new FileStatus(0, true, 0, 0, 0, datasetVersion2)});

    DatasetVersionFinder<StringDatasetVersion> versionFinder = new MockDatasetVersionFinder(fs, new Properties());

    List<StringDatasetVersion> datasetVersions =
        Lists.newArrayList(versionFinder.findDatasetVersions(new MockDataset(datasetPath)));
    Assert.assertEquals(datasetVersions.size(), 2);
    Assert.assertEquals(datasetVersions.get(0).getVersion(), dataset1);
    Assert.assertEquals(datasetVersions.get(0).getPathsToDelete().iterator().next(), datasetVersion1);
    Assert.assertEquals(datasetVersions.get(1).getVersion(), dataset2);
    Assert.assertEquals(datasetVersions.get(1).getPathsToDelete().iterator().next(), datasetVersion2);
  }


  public static class MockDatasetVersionFinder extends DatasetVersionFinder<StringDatasetVersion> {
    public MockDatasetVersionFinder(FileSystem fs, Properties props) {
      super(fs, props);
    }

    @Override
    public Class<? extends DatasetVersion> versionClass() {
      return StringDatasetVersion.class;
    }

    @Override
    public Path globVersionPattern() {
      return new Path("*");
    }

    @Override
    public StringDatasetVersion getDatasetVersion(Path pathRelativeToDatasetRoot, Path fullPath) {
      return new StringDatasetVersion(fullPath.getName(), fullPath);
    }
  }

  public static class MockDataset implements CleanableDataset, FileSystemDataset {
    private final Path datasetRoot;

    public MockDataset(Path datasetRoot) {
      this.datasetRoot = datasetRoot;
    }

    @Override
    public void clean()
        throws IOException {

    }

    @Override
    public Path datasetRoot() {
      return this.datasetRoot;
    }

    @Override public String datasetURN() {
      return datasetRoot().toString();
    }
  }

}
