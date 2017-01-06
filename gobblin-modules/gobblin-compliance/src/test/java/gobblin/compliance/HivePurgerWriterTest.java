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
package gobblin.compliance;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.mockito.Mockito;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class HivePurgerWriterTest {
  private ComplianceRecord recordMock = Mockito.mock(ComplianceRecord.class);
  private HivePurgerQueryExecutor hivePurgerQueryExecutorMock = Mockito.mock(HivePurgerQueryExecutor.class);
  private FileSystem fsMock = Mockito.mock(FileSystem.class);
  private HivePurgerWriter hivePurgerExecutionWriter;
  private Partition partitionMock = Mockito.mock(Partition.class);
  private FileStatus statusMock = Mockito.mock(FileStatus.class);

  private final String FINAL_LOCATION = "finalLocation";
  private final String STAGING_LOCATION = "stagingLocation";
  private final String ORIGINAL_LOCATION = "originalLocation";

  @BeforeTest
  public void initialize()
      throws IOException {
    Mockito.doReturn(new ArrayList<>()).when(this.recordMock).getQueries();
    Mockito.doReturn(this.partitionMock).when(this.recordMock).getHivePartition();
    Mockito.doReturn(FINAL_LOCATION).when(this.recordMock).getFinalPartitionLocation();
    Mockito.doReturn(STAGING_LOCATION).when(this.recordMock).getStagingPartitionLocation();
    Mockito.doReturn(ORIGINAL_LOCATION).when(this.partitionMock).getLocation();
    Mockito.doReturn(statusMock).when(this.fsMock).getFileStatus(any(Path.class));
    Mockito.doReturn((long)12345).doReturn((long)123456).when(this.statusMock).getModificationTime();
    this.hivePurgerExecutionWriter = new HivePurgerWriter(this.fsMock, this.hivePurgerQueryExecutorMock);
  }

  @Test
  public void getPurgeQueriesTest()
      throws IOException {
    when(recordMock.getCommit()).thenReturn(false);
    this.hivePurgerExecutionWriter.write(this.recordMock);
    verify(this.recordMock, times(1)).getQueries();
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testCommit()
      throws IOException {
    when(recordMock.getCommit()).thenReturn(true);
    this.hivePurgerExecutionWriter.write(this.recordMock);
  }
}
