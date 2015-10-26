/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.data.management.copy;

import gobblin.configuration.ConfigurationKeys;
import gobblin.data.management.util.PathUtils;

import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;


public class RecursiveCopyableDatasetTest {

  @Test
  public void testGetCopyableFiles() throws Exception {

    Set<Path> paths = Sets.newHashSet(new Path("dir1/file2"), new Path("dir1/file1"), new Path("dir2/file1"));

    String baseDir = getClass().getClassLoader().getResource("copyableDatasetTest/source").getFile();
    String destinationDir = getClass().getClassLoader().getResource("copyableDatasetTest/destination").getFile();

    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, destinationDir);

    RecursiveCopyableDataset dataset = new RecursiveCopyableDataset(FileSystem.getLocal(new Configuration()), new Path(baseDir), properties);

    List<CopyableFile> files = dataset.getCopyableFiles(FileSystem.getLocal(new Configuration()));

    Assert.assertEquals(files.size(), 3);

    for (CopyableFile copyableFile : files) {
      Path originRelativePath =
          PathUtils.relativizePath(PathUtils.getPathWithoutSchemeAndAuthority(copyableFile.getOrigin().getPath()),
              PathUtils.getPathWithoutSchemeAndAuthority(new Path(baseDir)));
      Path targetRelativePath =
          PathUtils.relativizePath(PathUtils.getPathWithoutSchemeAndAuthority(copyableFile.getDestination()),
              PathUtils.getPathWithoutSchemeAndAuthority(new Path(destinationDir)));
      Assert.assertTrue(paths.contains(originRelativePath));
      Assert.assertTrue(paths.contains(targetRelativePath));
      Assert.assertEquals(originRelativePath, targetRelativePath);
      Assert.assertEquals(copyableFile.getAncestorsOwnerAndPermission().size(), originRelativePath.depth() - 1);
    }

  }
}
