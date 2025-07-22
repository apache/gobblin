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

package org.apache.gobblin.temporal.ddm.work;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.mockito.Mockito;
import org.mockito.MockedStatic;

import org.apache.gobblin.temporal.util.nesting.work.Workload;
import org.apache.gobblin.temporal.workflows.metrics.EventSubmitterContext;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.WorkUnitSizeInfo;


/** Tests for {@link EagerFsDirBackedWorkUnitClaimCheckWorkload} */
public class EagerFsDirBackedWorkUnitClaimCheckWorkloadTest {

  private EagerFsDirBackedWorkUnitClaimCheckWorkload workload;
  private EventSubmitterContext eventSubmitterContext;
  private FileSystem mockFileSystem;

  @BeforeMethod
  public void setUp() throws Exception {
    URI fileSystemUri = new URI("hdfs://localhost:9000");
    String hdfsDir = "/test/dir";
    eventSubmitterContext = Mockito.mock(EventSubmitterContext.class);
    workload = Mockito.spy(new EagerFsDirBackedWorkUnitClaimCheckWorkload(fileSystemUri, hdfsDir, eventSubmitterContext, new Properties()));
    mockFileSystem = Mockito.mock(FileSystem.class);

    MockedStatic<HadoopUtils> mockedHadoopUtils = Mockito.mockStatic(HadoopUtils.class);
    mockedHadoopUtils.when(() -> HadoopUtils.getFileSystem(Mockito.any(), Mockito.any())).thenReturn(mockFileSystem);
  }

  @Test
  public void testExtractWorkUnitSizeInfo() throws Exception {
    // mock `FileStatus` having an encoded WorkUnitSizeInfo as filename
    List<FileStatus> fileStatuses = Arrays.asList(
        createMockFileStatus("/test/dir/multitask_n=3-total=100-median=100.0-mean=100.0-stddev=0.0_0.mwu"),
        createMockFileStatus("/test/dir/multitask_n=2-total=200-median=100.0-mean=100.0-stddev=0.0_1.mwu"),
        createMockFileStatus("/test/dir/task_n=1-total=300-median=100.0-mean=100.0-stddev=0.0_5.wu"),
        createMockFileStatus("/test/dir/multitask_n=4-total=400-median=100.0-mean=100.0-stddev=0.0_30.mwu"),
        createMockFileStatus("/test/dir/task_n=1-total=500-median=100.0-mean=100.0-stddev=0.0_2.wu")
    );

    Mockito.when(mockFileSystem.listStatus(Mockito.any(Path.class), Mockito.any(PathFilter.class)))
        .thenReturn(fileStatuses.toArray(new FileStatus[0]));

    Optional<Workload.WorkSpan<WorkUnitClaimCheck>> span = workload.getSpan(0, 5);
    Assert.assertTrue(span.isPresent());

    Workload.WorkSpan<WorkUnitClaimCheck> workSpan = span.get();
    Assert.assertEquals(workSpan.getNumElems(), fileStatuses.size());

    Iterable<WorkUnitClaimCheck> workSpanIterable = () -> workSpan;
    List<WorkUnitClaimCheck> wuClaimChecks = StreamSupport.stream(workSpanIterable.spliterator(), false)
        .collect(Collectors.toList());

    // note: since ordering is based on filename, it will not be the same as `fileStatuses` above
    for (WorkUnitClaimCheck workUnitClaimCheck : wuClaimChecks) {
      WorkUnitSizeInfo sizeInfo = workUnitClaimCheck.getWorkUnitSizeInfo();
      Assert.assertNotNull(sizeInfo);
      int n = sizeInfo.getNumConstituents();
      long expectedTotalSize = (n == 1 && workUnitClaimCheck.getWorkUnitPath().endsWith("_5.wu")) ? 300 :
          n == 1 ? 500 :
              n == 2 ? 200 :
                  n == 3 ? 100 :
                      n == 4 ? 400 :
                          -1; // unexpected (sentinel)
      Assert.assertEquals(sizeInfo.getTotalSize(), expectedTotalSize);
    }
  }

  private static FileStatus createMockFileStatus(String path) {
    FileStatus fileStatus = Mockito.mock(FileStatus.class);
    Path filePath = new Path(path);
    Mockito.when(fileStatus.getPath()).thenReturn(filePath);
    return fileStatus;
  }
}

