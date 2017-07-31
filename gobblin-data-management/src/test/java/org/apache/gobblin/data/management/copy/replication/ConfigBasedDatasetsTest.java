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

package org.apache.gobblin.data.management.copy.replication;

import java.net.URI;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.data.management.copy.PreserveAttributes;
import org.apache.gobblin.data.management.copy.entities.PostPublishStep;
import org.apache.gobblin.data.management.copy.entities.PrePublishStep;
import org.apache.gobblin.source.extractor.ComparableWatermark;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.gobblin.util.FileListUtils;
import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.commit.DeleteFileCommitStep;


/**
 * Unit test for {@link ConfigBasedDatasets}
 * @author mitu
 *
 */
@Test(groups = {"gobblin.data.management.copy.replication"})

public class ConfigBasedDatasetsTest {

  @Test
  public void testGetCopyableFiles() throws Exception {
    String sourceDir = getClass().getClassLoader().getResource("configBasedDatasetTest/src").getFile();
    String destinationDir = getClass().getClassLoader().getResource("configBasedDatasetTest/dest").getFile();
    FileSystem localFs = FileSystem.getLocal(new Configuration());
    URI local = localFs.getUri();
    long sourceWatermark = 100L;

    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/publisher");

    CopyConfiguration copyConfiguration =
        CopyConfiguration.builder(FileSystem.getLocal(new Configuration()), properties).publishDir(new Path(destinationDir))
        .preserve(PreserveAttributes.fromMnemonicString("ugp")).build();

    ReplicationMetaData mockMetaData = Mockito.mock(ReplicationMetaData.class);
    Mockito.when(mockMetaData.toString()).thenReturn("Mock Meta Data");

    ReplicationConfiguration mockRC = Mockito.mock(ReplicationConfiguration.class);
    Mockito.when(mockRC.getCopyMode()).thenReturn(ReplicationCopyMode.PULL);
    Mockito.when(mockRC.getMetaData()).thenReturn(mockMetaData);

    HadoopFsEndPoint copyFrom = Mockito.mock(HadoopFsEndPoint.class);
    Mockito.when(copyFrom.getDatasetPath()).thenReturn(new Path(sourceDir));
    Mockito.when(copyFrom.getFsURI()).thenReturn(local);
    ComparableWatermark sw = new LongWatermark(sourceWatermark);
    Mockito.when(copyFrom.getWatermark()).thenReturn(Optional.of(sw));
    Mockito.when(copyFrom.getFiles()).thenReturn(FileListUtils.listFilesRecursively(localFs, new Path(sourceDir)));

    HadoopFsEndPoint copyTo = Mockito.mock(HadoopFsEndPoint.class);
    Mockito.when(copyTo.getDatasetPath()).thenReturn(new Path(destinationDir));
    Mockito.when(copyTo.getFsURI()).thenReturn(local);
    Optional<ComparableWatermark>tmp = Optional.absent();
    Mockito.when(copyTo.getWatermark()).thenReturn(tmp);
    Mockito.when(copyTo.getFiles()).thenReturn(FileListUtils.listFilesRecursively(localFs, new Path(destinationDir)));

    CopyRoute route = Mockito.mock(CopyRoute.class);
    Mockito.when(route.getCopyFrom()).thenReturn(copyFrom);
    Mockito.when(route.getCopyTo()).thenReturn(copyTo);

    ConfigBasedDataset dataset = new ConfigBasedDataset(mockRC, properties, route);

    Collection<? extends CopyEntity> copyableFiles = dataset.getCopyableFiles(localFs, copyConfiguration);
    Assert.assertEquals(copyableFiles.size(), 6);

    Set<Path> paths = Sets.newHashSet(new Path("dir1/file2"), new Path("dir1/file1"), new Path("dir2/file1"), new Path("dir2/file3"));
    for (CopyEntity copyEntity : copyableFiles) {
      if(copyEntity instanceof CopyableFile) {
        CopyableFile file = (CopyableFile) copyEntity;
        Path originRelativePath =
            PathUtils.relativizePath(PathUtils.getPathWithoutSchemeAndAuthority(file.getOrigin().getPath()),
                PathUtils.getPathWithoutSchemeAndAuthority(new Path(sourceDir)));
        Path targetRelativePath =
            PathUtils.relativizePath(PathUtils.getPathWithoutSchemeAndAuthority(file.getDestination()),
                PathUtils.getPathWithoutSchemeAndAuthority(new Path(destinationDir)));

        Assert.assertTrue(paths.contains(originRelativePath));
        Assert.assertTrue(paths.contains(targetRelativePath));
        Assert.assertEquals(originRelativePath, targetRelativePath);
      }
      else if(copyEntity instanceof PrePublishStep){
        PrePublishStep pre = (PrePublishStep)copyEntity;
        Assert.assertTrue(pre.getStep() instanceof DeleteFileCommitStep);
        // need to delete this file
        Assert.assertTrue(pre.explain().indexOf("configBasedDatasetTest/dest/dir1/file1") > 0);
      }
      else if(copyEntity instanceof PostPublishStep){
        PostPublishStep post = (PostPublishStep)copyEntity;
        Assert.assertTrue(post.getStep() instanceof WatermarkMetadataGenerationCommitStep);
        Assert.assertTrue(post.explain().indexOf("dest/_metadata") > 0 && post.explain().indexOf(""+sourceWatermark)>0);
      }
      else{
        throw new Exception("Wrong type");
      }
    }
  }

}
