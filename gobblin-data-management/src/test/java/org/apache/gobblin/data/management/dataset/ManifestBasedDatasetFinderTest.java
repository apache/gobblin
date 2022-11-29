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

package org.apache.gobblin.data.management.dataset;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.ManifestBasedDataset;
import org.apache.gobblin.data.management.copy.ManifestBasedDatasetFinder;
import org.apache.gobblin.data.management.partition.FileSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Matchers.*;


public class ManifestBasedDatasetFinderTest {
  private FileSystem localFs;

  public ManifestBasedDatasetFinderTest() throws IOException {
    localFs = FileSystem.getLocal(new Configuration());
  }

  @Test
  public void testFindDataset() throws IOException {

    //Get manifest Path
    String manifestPath = getClass().getClassLoader().getResource("manifestBasedDistcpTest/sampleManifest.json").getPath();

    // Test manifestDatasetFinder
    Properties props = new Properties();
    props.setProperty("gobblin.copy.manifestBased.manifest.location", manifestPath);
    ManifestBasedDatasetFinder finder = new ManifestBasedDatasetFinder(localFs, props);
    List<ManifestBasedDataset> datasets = finder.findDatasets();
    Assert.assertEquals(datasets.size(), 1);
  }

  @Test
  public void testFindFiles() throws IOException, URISyntaxException {

    //Get manifest Path
    Path manifestPath = new Path(getClass().getClassLoader().getResource("manifestBasedDistcpTest/sampleManifest.json").getPath());

    // Test manifestDatasetFinder
    Properties props = new Properties();
    props.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/");

    try (FileSystem destFs = Mockito.mock(FileSystem.class); FileSystem sourceFs = Mockito.mock(FileSystem.class)) {
      URI SRC_FS_URI = new URI("source", "the.source.org", "/", null);
      URI DEST_FS_URI = new URI("dest", "the.dest.org", "/", null);
      Mockito.when(sourceFs.getUri()).thenReturn(SRC_FS_URI);
      Mockito.when(destFs.getUri()).thenReturn(DEST_FS_URI);
      Mockito.when(sourceFs.getFileStatus(any(Path.class))).thenReturn(localFs.getFileStatus(manifestPath));
      Mockito.when(sourceFs.exists(any(Path.class))).thenReturn(true);
      Mockito.when(sourceFs.open(manifestPath)).thenReturn(localFs.open(manifestPath));
      Mockito.when(destFs.exists(any(Path.class))).thenReturn(false);
      Mockito.doAnswer(invocation -> {
        Object[] args = invocation.getArguments();
        Path path = (Path)args[0];
        return localFs.makeQualified(path);
      }).when(sourceFs).makeQualified(any(Path.class));
      Iterator<FileSet<CopyEntity>> fileSets =
          new ManifestBasedDataset(sourceFs, manifestPath, props).getFileSetIterator(destFs, CopyConfiguration.builder(destFs, props).build());
      Assert.assertTrue(fileSets.hasNext());
      FileSet<CopyEntity> fileSet = fileSets.next();
      Assert.assertEquals(fileSet.getFiles().size(), 2);
    }
  }
}