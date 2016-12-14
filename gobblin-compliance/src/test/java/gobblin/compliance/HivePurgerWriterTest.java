/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
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
  private HivePurgerPartitionRecord recordMock = Mockito.mock(HivePurgerPartitionRecord.class);
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
    when(recordMock.getPurgeQueries()).thenReturn(new ArrayList<String>());
    when(recordMock.getHiveTablePartition()).thenReturn(partitionMock);
    when(recordMock.getFinalPartitionLocation()).thenReturn(FINAL_LOCATION);
    when(recordMock.getStagingPartitionLocation()).thenReturn(STAGING_LOCATION);
    when(partitionMock.getLocation()).thenReturn(ORIGINAL_LOCATION);
    when(fsMock.getFileStatus(any(Path.class))).thenReturn(statusMock);
    when(statusMock.getModificationTime()).thenReturn((long) 12345).thenReturn((long) 12346);
    this.hivePurgerExecutionWriter = new HivePurgerWriter(this.fsMock, this.hivePurgerQueryExecutorMock);
  }

  @Test
  public void getPurgeQueriesTest()
      throws IOException {
    when(recordMock.shouldCommit()).thenReturn(false);
    this.hivePurgerExecutionWriter.write(this.recordMock);
    verify(this.recordMock, times(1)).getPurgeQueries();
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testCommit()
      throws IOException {
    when(recordMock.shouldCommit()).thenReturn(true);
    this.hivePurgerExecutionWriter.write(this.recordMock);
  }
}
