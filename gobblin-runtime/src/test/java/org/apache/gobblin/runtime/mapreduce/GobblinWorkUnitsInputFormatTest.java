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

package org.apache.gobblin.runtime.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import static org.testng.Assert.*;


public class GobblinWorkUnitsInputFormatTest {
  @Test
  public void testGetSplits()
      throws Exception {

    URI baseUri = new URI(GobblinWorkUnitsInputFormatTest.class.getSimpleName() + "://testGetSplits");
    Configuration configuration = new Configuration();

    Path workUnitsDir = new Path(new Path(baseUri), "/workUnits");

    FileSystem fs = Mockito.mock(FileSystem.class);
    FileStatus[] statuses = createFileStatuses(20, workUnitsDir);
    Mockito.when(fs.listStatus(workUnitsDir)).thenReturn(statuses);
    Mockito.when(fs.makeQualified(Mockito.any(Path.class))).thenAnswer(new Answer<Path>() {
      @Override
      public Path answer(InvocationOnMock invocation)
          throws Throwable {
        return (Path) invocation.getArguments()[0];
      }
    });

    FileSystemTestUtils.addFileSystemForTest(baseUri, configuration, fs);

    GobblinWorkUnitsInputFormat inputFormat = new GobblinWorkUnitsInputFormat();
    Job job = Job.getInstance(configuration);
    FileInputFormat.addInputPath(job, workUnitsDir);

    List<InputSplit> splits = inputFormat.getSplits(job);

    Assert.assertEquals(splits.size(), 20);
    verifyPaths(splits, statuses);
  }

  @Test
  public void testGetSplitsMaxSize()
      throws Exception {

    URI baseUri = new URI(GobblinWorkUnitsInputFormatTest.class.getSimpleName() + "://testGetSplitsMaxSize");
    Configuration configuration = new Configuration();

    Path workUnitsDir = new Path(new Path(baseUri), "/workUnits");

    FileSystem fs = Mockito.mock(FileSystem.class);
    FileStatus[] statuses = createFileStatuses(20, workUnitsDir);
    Mockito.when(fs.listStatus(workUnitsDir)).thenReturn(statuses);
    Mockito.when(fs.makeQualified(Mockito.any(Path.class))).thenAnswer(new Answer<Path>() {
      @Override
      public Path answer(InvocationOnMock invocation)
          throws Throwable {
        return (Path) invocation.getArguments()[0];
      }
    });

    FileSystemTestUtils.addFileSystemForTest(baseUri, configuration, fs);

    GobblinWorkUnitsInputFormat inputFormat = new GobblinWorkUnitsInputFormat();
    Job job = Job.getInstance(configuration);
    FileInputFormat.addInputPath(job, workUnitsDir);
    GobblinWorkUnitsInputFormat.setMaxMappers(job, 6);

    List<InputSplit> splits = inputFormat.getSplits(job);

    Assert.assertTrue(splits.size() < 6);
    verifyPaths(splits, statuses);
  }

  @Test
  public void testSplit() throws Exception {
    List<String> paths = Lists.newArrayList("/path1", "/path2");
    GobblinWorkUnitsInputFormat.GobblinSplit split = new GobblinWorkUnitsInputFormat.GobblinSplit(paths);

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    split.write(new DataOutputStream(os));

    ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
    GobblinWorkUnitsInputFormat.GobblinSplit deserSplit = new GobblinWorkUnitsInputFormat.GobblinSplit();
    deserSplit.readFields(new DataInputStream(is));

    Assert.assertEquals(split, deserSplit);
  }

  @Test
  public void testRecordReader()
      throws Exception {

    List<String> paths = Lists.newArrayList("/path1", "/path2");
    GobblinWorkUnitsInputFormat.GobblinSplit split = new GobblinWorkUnitsInputFormat.GobblinSplit(paths);

    GobblinWorkUnitsInputFormat inputFormat = new GobblinWorkUnitsInputFormat();
    RecordReader<LongWritable, Text> recordReader =
        inputFormat.createRecordReader(split, new TaskAttemptContextImpl(new Configuration(), new TaskAttemptID("a", 1,
        TaskType.MAP, 1, 1)));

    recordReader.nextKeyValue();
    Assert.assertEquals(recordReader.getCurrentKey().get(), 0);
    Assert.assertEquals(recordReader.getCurrentValue().toString(), "/path1");

    recordReader.nextKeyValue();
    Assert.assertEquals(recordReader.getCurrentKey().get(), 1);
    Assert.assertEquals(recordReader.getCurrentValue().toString(), "/path2");

    Assert.assertFalse(recordReader.nextKeyValue());

  }

  private void verifyPaths(List<InputSplit> splits, FileStatus[] statuses) {
    Set<String> splitPaths = Sets.newHashSet();
    for (InputSplit split : splits) {
      splitPaths.addAll(((GobblinWorkUnitsInputFormat.GobblinSplit) split).getPaths());
    }

    Set<String> statusPaths = Sets.newHashSet();
    for (FileStatus status : statuses) {
      statusPaths.add(status.getPath().toString());
    }

    Assert.assertEquals(splitPaths, statusPaths);
  }

  private FileStatus[] createFileStatuses(int howMany, Path basePath) {
    FileStatus[] statuses = new FileStatus[howMany];
    for (int i = 0; i < howMany; i++) {
      statuses[i] = new FileStatus(0, false, 0, 0, 0, new Path(basePath, "file" + i));
    }
    return statuses;
  }
}
