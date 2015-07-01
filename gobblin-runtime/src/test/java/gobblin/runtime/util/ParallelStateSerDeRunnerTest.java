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

package gobblin.runtime.util;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;

import gobblin.runtime.TaskState;
import gobblin.source.workunit.WorkUnit;


/**
 * Unit tests for {@link ParallelStateSerDeRunner}.
 *
 * @author ynli
 */
@Test(groups = {"gobblin.runtime.util"})
public class ParallelStateSerDeRunnerTest {

  private FileSystem fs;
  private Path outputPath;

  @BeforeClass
  public void setUp() throws IOException {
    this.fs = FileSystem.getLocal(new Configuration());
    this.outputPath = new Path(ParallelStateSerDeRunnerTest.class.getSimpleName());
  }

  @Test
  public void testSerializeToFile() throws IOException {
    Closer closer = Closer.create();
    try {
      ParallelStateSerDeRunner stateSerDeRunner = closer.register(new ParallelStateSerDeRunner(2, this.fs));

      WorkUnit workUnit1 = new WorkUnit();
      workUnit1.setProp("foo", "bar");
      workUnit1.setProp("a", 10);
      stateSerDeRunner.serializeToFile(workUnit1, new Path(this.outputPath, "wu1"));

      WorkUnit workUnit2 = new WorkUnit();
      workUnit2.setProp("foo", "baz");
      workUnit2.setProp("b", 20);
      stateSerDeRunner.serializeToFile(workUnit2, new Path(this.outputPath, "wu2"));

      stateSerDeRunner.awaitDone();
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  @Test(dependsOnMethods = "testSerializeToFile")
  public void testDeserializeFromFile() throws IOException {
    Closer closer = Closer.create();
    try {
      ParallelStateSerDeRunner stateSerDeRunner = closer.register(new ParallelStateSerDeRunner(2, this.fs));

      WorkUnit workUnit1 = new WorkUnit();
      stateSerDeRunner.deserializeFromFile(workUnit1, new Path(this.outputPath, "wu1"));

      WorkUnit workUnit2 = new WorkUnit();
      stateSerDeRunner.deserializeFromFile(workUnit2, new Path(this.outputPath, "wu2"));

      stateSerDeRunner.awaitDone();

      Assert.assertEquals(workUnit1.getPropertyNames().size(), 2);
      Assert.assertEquals(workUnit1.getProp("foo"), "bar");
      Assert.assertEquals(workUnit1.getPropAsInt("a"), 10);

      Assert.assertEquals(workUnit2.getPropertyNames().size(), 2);
      Assert.assertEquals(workUnit2.getProp("foo"), "baz");
      Assert.assertEquals(workUnit2.getPropAsInt("b"), 20);
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  @Test
  public void testSerializeToSequenceFile() throws IOException {
    Closer closer = Closer.create();
    try {
      SequenceFile.Writer writer1 = closer.register(SequenceFile.createWriter(this.fs, new Configuration(),
          new Path(this.outputPath, "seq1"), Text.class, TaskState.class));

      Text key = new Text();
      TaskState taskState = new TaskState();

      taskState.setJobId("Job0");
      taskState.setTaskId("Task0");
      taskState.setStartTime(0l);
      taskState.setEndTime(1000l);
      writer1.append(key, taskState);

      taskState.setTaskId("Task1");
      taskState.setEndTime(2000l);
      writer1.append(key, taskState);

      SequenceFile.Writer writer2 = closer.register(SequenceFile.createWriter(this.fs, new Configuration(),
          new Path(this.outputPath, "seq2"), Text.class, TaskState.class));

      taskState.setTaskId("Task2");
      taskState.setEndTime(3000l);
      writer2.append(key, taskState);

      taskState.setTaskId("Task3");
      taskState.setEndTime(1500l);
      writer2.append(key, taskState);
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  @Test(dependsOnMethods = "testSerializeToSequenceFile")
  public void testDeserializeFromSequenceFile() throws IOException {
    List<TaskState> taskStates = Lists.newArrayList();
    Closer closer = Closer.create();
    try {
      ParallelStateSerDeRunner stateSerDeRunner = closer.register(new ParallelStateSerDeRunner(2, this.fs));
      stateSerDeRunner.deserializeFromSequenceFile(Text.class, TaskState.class, new Path(this.outputPath, "seq1"),
          taskStates);
      stateSerDeRunner.deserializeFromSequenceFile(Text.class, TaskState.class, new Path(this.outputPath, "seq2"),
          taskStates);
      stateSerDeRunner.awaitDone();

      Assert.assertEquals(taskStates.size(), 4);

      Map<String, TaskState> taskStateMap = Maps.newHashMap();
      for (TaskState taskState : taskStates) {
        taskStateMap.put(taskState.getTaskId(), taskState);
      }

      Assert.assertEquals(taskStateMap.size(), 4);

      TaskState taskState0 = taskStateMap.get("Task0");
      Assert.assertEquals(taskState0.getStartTime(), 0l);
      Assert.assertEquals(taskState0.getEndTime(), 1000l);

      TaskState taskState1 = taskStateMap.get("Task1");
      Assert.assertEquals(taskState1.getStartTime(), 0l);
      Assert.assertEquals(taskState1.getEndTime(), 2000l);

      TaskState taskState2 = taskStateMap.get("Task2");
      Assert.assertEquals(taskState2.getStartTime(), 0l);
      Assert.assertEquals(taskState2.getEndTime(), 3000l);

      TaskState taskState3 = taskStateMap.get("Task3");
      Assert.assertEquals(taskState3.getStartTime(), 0l);
      Assert.assertEquals(taskState3.getEndTime(), 1500l);
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  @AfterClass
  public void tearDown() throws IOException {
    if (this.fs != null && this.outputPath != null) {
      this.fs.delete(this.outputPath, true);
    }
  }
}
