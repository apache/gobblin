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
package org.apache.gobblin.data.management.copy.splitter;

import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.copy.CopySource;
import org.apache.gobblin.data.management.copy.CopyableDatasetMetadata;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.data.management.copy.CopyableFileUtils;
import org.apache.gobblin.data.management.copy.TestCopyableDataset;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.ForkOperatorUtils;
import org.apache.gobblin.util.guid.Guid;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


public class DistcpFileSplitterTest {

  // This test ONLY checks whether manipulates the properties of the work units correctly
  // (i.e. "merging" them within the collection that was passed in).
  // It does NOT check that the merge in the filesystem has been completed successfully.
  // This requires testing on an HDFS setup.
  @Test
  public void testMergeSplitWorkUnits() throws Exception {
    long mockFileLen = 12L;
    long mockBlockSize = 4L;
    long mockMaxSplitSize = 4L;
    long expectedSplitSize = (mockMaxSplitSize / mockBlockSize) * mockBlockSize;
    int expectedSplits = (int) (mockFileLen / expectedSplitSize + 1);

    FileSystem fs = mock(FileSystem.class);

    List<WorkUnitState> splitWorkUnits =
        createMockSplitWorkUnits(fs, mockFileLen, mockBlockSize, mockMaxSplitSize).stream()
            .map(wu -> new WorkUnitState(wu)).collect(Collectors.toList());
    Assert.assertEquals(splitWorkUnits.size(), expectedSplits);

    Collection<WorkUnitState> mergedWorkUnits = DistcpFileSplitter.mergeAllSplitWorkUnits(fs, splitWorkUnits);
    Assert.assertEquals(mergedWorkUnits.size(), 1);
  }

  // This test checks whether a work unit has been successfully set up for a split,
  // but does not check that the split is actually done correctly when input streams are used.
  @Test
  public void testSplitFile() throws Exception {
    long mockFileLen = 12L;
    long mockBlockSize = 4L;
    long mockMaxSplitSize = 4L;
    long expectedSplitSize = (mockMaxSplitSize / mockBlockSize) * mockBlockSize;
    int expectedSplits = (int) (mockFileLen / expectedSplitSize + 1);

    FileSystem fs = mock(FileSystem.class);

    Collection<WorkUnit> splitWorkUnits = createMockSplitWorkUnits(fs, mockFileLen, mockBlockSize, mockMaxSplitSize);
    Assert.assertEquals(splitWorkUnits.size(), expectedSplits);

    Set<Integer> splitNums = new HashSet<>();
    boolean hasLastSplit = false;
    for (WorkUnit wu : splitWorkUnits) {
      Optional<DistcpFileSplitter.Split> split = DistcpFileSplitter.getSplit(wu);
      Assert.assertTrue(split.isPresent());
      Assert.assertEquals(split.get().getTotalSplits(), expectedSplits);
      int splitNum = split.get().getSplitNumber();
      Assert.assertFalse(splitNums.contains(splitNum));
      splitNums.add(splitNum);
      Assert.assertEquals(split.get().getLowPosition(), expectedSplitSize * splitNum);
      if (split.get().isLastSplit()) {
        hasLastSplit = true;
        Assert.assertEquals(split.get().getHighPosition(), mockFileLen);
      } else {
        Assert.assertEquals(split.get().getHighPosition(), expectedSplitSize * (splitNum + 1));
      }
    }
    Assert.assertTrue(hasLastSplit);
  }

  private Collection<WorkUnit> createMockSplitWorkUnits(FileSystem fs, long fileLen, long blockSize, long maxSplitSize)
      throws Exception {
    FileStatus file = mock(FileStatus.class);
    when(file.getLen()).thenReturn(fileLen);
    when(file.getBlockSize()).thenReturn(blockSize);

    URI uri = new URI("hdfs", "dummyhost", "/test", "test");
    Path path = new Path(uri);
    when(fs.getUri()).thenReturn(uri);

    CopyableDatasetMetadata cdm = new CopyableDatasetMetadata(new TestCopyableDataset(path));

    CopyableFile cf = CopyableFileUtils.getTestCopyableFile();
    CopyableFile spy = spy(cf);
    doReturn(file).when(spy).getFileStatus();
    doReturn(blockSize).when(spy).getBlockSize(any(FileSystem.class));
    doReturn(path).when(spy).getDestination();

    WorkUnit wu = WorkUnit.createEmpty();
    wu.setProp(DistcpFileSplitter.MAX_SPLIT_SIZE_KEY, maxSplitSize);
    wu.setProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_OUTPUT_DIR, 1, 0),
        path.toString());
    CopySource.setWorkUnitGuid(wu, Guid.fromStrings(wu.toString()));
    CopySource.serializeCopyEntity(wu, cf);
    CopySource.serializeCopyableDataset(wu, cdm);

    return DistcpFileSplitter.splitFile(spy, wu, fs);
  }

}
