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
package org.apache.gobblin.temporal.ddm.utils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.source.Source;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.temporal.ddm.util.JobStateUtils;


public class JobStateUtilTest {

  private JobState jobState;
  private FileSystem fileSystem;

  @BeforeMethod
  public void setUp() {
    jobState = Mockito.mock(JobState.class);
    fileSystem = Mockito.mock(FileSystem.class);
  }

  @Test
  public void testOpenFileSystem() throws IOException {

    Mockito.when(jobState.getProp(Mockito.anyString(), Mockito.anyString())).thenReturn("file:///test");
    Mockito.when(jobState.getProperties()).thenReturn(new Properties());

    FileSystem fs = JobStateUtils.openFileSystem(jobState);

    Assert.assertNotNull(fs);
    Mockito.verify(jobState,Mockito.times(1)).getProp(Mockito.anyString(), Mockito.anyString());
  }

  @Test
  public void testCreateSource() throws ReflectiveOperationException {
    Mockito.when(jobState.getProp(Mockito.anyString()))
        .thenReturn("org.apache.gobblin.source.extractor.filebased.TextFileBasedSource");
    Source<?, ?> source = JobStateUtils.createSource(jobState);
    Assert.assertNotNull(source);
  }

  @Test
  public void testOpenTaskStateStoreUncached() throws URISyntaxException {
    Mockito.when(jobState.getProp(Mockito.anyString())).thenReturn("file:///test");
    Mockito.when(jobState.getJobId()).thenReturn("testJobId");
    Mockito.when(jobState.getJobName()).thenReturn("testJobName");
    Mockito.when(fileSystem.makeQualified(Mockito.any()))
        .thenReturn(new Path("file:///test/testJobName/testJobId/output"));
    Mockito.when(fileSystem.getUri()).thenReturn(new URI("file:///test/testJobName/testJobId/output"));

    StateStore<TaskState> stateStore = JobStateUtils.openTaskStateStoreUncached(jobState, fileSystem);

    Assert.assertNotNull(stateStore);
  }

  @Test
  public void testGetFileSystemUri() {
    Mockito.when(jobState.getProp(Mockito.anyString(), Mockito.anyString())).thenReturn("file:///test");
    URI fsUri = JobStateUtils.getFileSystemUri(jobState);
    Assert.assertEquals(URI.create("file:///test"), fsUri);
    Mockito.verify(jobState).getProp(Mockito.anyString(), Mockito.anyString());
  }

  @Test
  public void testGetWorkDirRoot() {
    Mockito.when(jobState.getProp(Mockito.anyString())).thenReturn("/tmp");
    Mockito.when(jobState.getJobName()).thenReturn("testJob");
    Mockito.when(jobState.getJobId()).thenReturn("jobId123");
    Path rootPath = JobStateUtils.getWorkDirRoot(jobState);
    Assert.assertEquals(new Path("/tmp/testJob/jobId123"), rootPath);
    Mockito.verify(jobState, Mockito.times(1)).getProp(Mockito.anyString());
  }

  @Test
  public void testGetWorkUnitsPath() {
    Mockito.when(jobState.getProp(Mockito.anyString())).thenReturn("/tmp");
    Mockito.when(jobState.getJobName()).thenReturn("testJob");
    Mockito.when(jobState.getJobId()).thenReturn("jobId123");
    Path workUnitsPath = JobStateUtils.getWorkUnitsPath(jobState);
    Assert.assertEquals(new Path("/tmp/testJob/jobId123/input"), workUnitsPath);
  }

  @Test
  public void testGetTaskStateStorePath() throws IOException {
    Mockito.when(fileSystem.makeQualified(Mockito.any(Path.class))).thenReturn(new Path("/qualified/path"));
    Mockito.when(jobState.getProp(Mockito.anyString())).thenReturn("/tmp");
    Mockito.when(jobState.getJobName()).thenReturn("testJob");
    Mockito.when(jobState.getJobId()).thenReturn("jobId123");
    Path taskStateStorePath = JobStateUtils.getTaskStateStorePath(jobState, fileSystem);
    Assert.assertEquals(new Path("/qualified/path"), taskStateStorePath);
  }

  @Test
  public void testWriteJobState() throws IOException {
    Path workDirRootPath = new Path("/tmp");
    FSDataOutputStream dos = Mockito.mock(FSDataOutputStream.class);
    Mockito.when(fileSystem.create(Mockito.any(Path.class))).thenReturn(dos);

    JobStateUtils.writeJobState(jobState, workDirRootPath, fileSystem);

    Mockito.verify(fileSystem).create(Mockito.any(Path.class));
    Mockito.verify(jobState).write(Mockito.any(DataOutputStream.class), Mockito.anyBoolean(), Mockito.anyBoolean());
  }

  @Test
  public void testGetSharedResourcesBroker() {
    Mockito.when(jobState.getProperties()).thenReturn(System.getProperties());
    Mockito.when(jobState.getJobName()).thenReturn("testJob");
    Mockito.when(jobState.getJobId()).thenReturn("jobId123");
    Assert.assertNotNull(JobStateUtils.getSharedResourcesBroker(jobState));
  }
}
