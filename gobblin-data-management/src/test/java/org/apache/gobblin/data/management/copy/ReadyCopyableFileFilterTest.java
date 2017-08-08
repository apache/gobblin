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

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

import org.apache.gobblin.util.PathUtils;

@Test(groups = {"gobblin.data.management.copy"})
public class ReadyCopyableFileFilterTest {

  @Test
  public void testFilter() throws Exception {

    CopyableFileFilter readyFilter = new ReadyCopyableFileFilter();

    List<CopyableFile> copyableFiles = Lists.newArrayList();

    copyableFiles.add(CopyableFileUtils.getTestCopyableFile());
    copyableFiles.add(CopyableFileUtils.getTestCopyableFile());
    copyableFiles.add(CopyableFileUtils.getTestCopyableFile());

    FileSystem sourceFs = Mockito.mock(FileSystem.class);

    Mockito.when(sourceFs.exists(PathUtils.addExtension(copyableFiles.get(0).getOrigin().getPath(), ".ready")))
        .thenReturn(false);
    Mockito.when(sourceFs.exists(PathUtils.addExtension(copyableFiles.get(1).getOrigin().getPath(), ".ready")))
        .thenReturn(true);
    Mockito.when(sourceFs.exists(PathUtils.addExtension(copyableFiles.get(2).getOrigin().getPath(), ".ready")))
        .thenReturn(false);

    Collection<CopyableFile> filtered = readyFilter.filter(sourceFs, null, copyableFiles);

    Assert.assertEquals(filtered.size(), 1);
  }

}
