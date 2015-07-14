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

package gobblin.util;

import java.io.IOException;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Queues;
import com.google.common.io.Closer;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Watermark;
import gobblin.source.extractor.WatermarkSerializerHelper;
import gobblin.source.workunit.WorkUnit;


/**
 * Unit tests for {@link ParallelRunner}.
 *
 * @author ynli
 */
@Test(groups = { "gobblin.util" })
public class ParallelRunnerTest {

  private FileSystem fs;
  private Path outputPath;

  @BeforeClass
  public void setUp() throws IOException {
    this.fs = FileSystem.getLocal(new Configuration());
    this.outputPath = new Path(ParallelRunnerTest.class.getSimpleName());
  }

  @Test
  public void testSerializeToFile() throws IOException {
    Closer closer = Closer.create();
    try {
      ParallelRunner parallelRunner = closer.register(new ParallelRunner(2, this.fs));

      WorkUnit workUnit1 = new WorkUnit();
      workUnit1.setProp("foo", "bar");
      workUnit1.setProp("a", 10);
      parallelRunner.serializeToFile(workUnit1, new Path(this.outputPath, "wu1"));

      WorkUnit workUnit2 = new WorkUnit();
      workUnit2.setProp("foo", "baz");
      workUnit2.setProp("b", 20);
      parallelRunner.serializeToFile(workUnit2, new Path(this.outputPath, "wu2"));

    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  @Test(dependsOnMethods = "testSerializeToFile")
  public void testDeserializeFromFile() throws IOException {
    WorkUnit workUnit1 = new WorkUnit();
    WorkUnit workUnit2 = new WorkUnit();

    Closer closer = Closer.create();
    try {
      ParallelRunner parallelRunner = closer.register(new ParallelRunner(2, this.fs));
      parallelRunner.deserializeFromFile(workUnit1, new Path(this.outputPath, "wu1"));
      parallelRunner.deserializeFromFile(workUnit2, new Path(this.outputPath, "wu2"));
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }

    Assert.assertEquals(workUnit1.getPropertyNames().size(), 2);
    Assert.assertEquals(workUnit1.getProp("foo"), "bar");
    Assert.assertEquals(workUnit1.getPropAsInt("a"), 10);

    Assert.assertEquals(workUnit2.getPropertyNames().size(), 2);
    Assert.assertEquals(workUnit2.getProp("foo"), "baz");
    Assert.assertEquals(workUnit2.getPropAsInt("b"), 20);
  }

  @Test
  public void testSerializeToSequenceFile() throws IOException {
    Closer closer = Closer.create();
    try {
      SequenceFile.Writer writer1 = closer.register(SequenceFile.createWriter(this.fs, new Configuration(),
          new Path(this.outputPath, "seq1"), Text.class, WorkUnitState.class));

      Text key = new Text();
      WorkUnitState workUnitState = new WorkUnitState();
      TestWatermark watermark = new TestWatermark();

      watermark.setLongWatermark(10L);
      workUnitState.setActualHighWatermark(watermark);
      writer1.append(key, workUnitState);

      SequenceFile.Writer writer2 = closer.register(SequenceFile.createWriter(this.fs, new Configuration(),
          new Path(this.outputPath, "seq2"), Text.class, WorkUnitState.class));

      watermark.setLongWatermark(100L);
      workUnitState.setActualHighWatermark(watermark);
      writer2.append(key, workUnitState);
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  @Test(dependsOnMethods = "testSerializeToSequenceFile")
  public void testDeserializeFromSequenceFile() throws IOException {
    Queue<WorkUnitState> workUnitStates = Queues.newConcurrentLinkedQueue();

    Closer closer = Closer.create();
    try {
      ParallelRunner parallelRunner = closer.register(new ParallelRunner(2, this.fs));
      parallelRunner.deserializeFromSequenceFile(Text.class, WorkUnitState.class, new Path(this.outputPath, "seq1"),
          workUnitStates);
      parallelRunner.deserializeFromSequenceFile(Text.class, WorkUnitState.class, new Path(this.outputPath, "seq2"),
          workUnitStates);
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }

    Assert.assertEquals(workUnitStates.size(), 2);

    for (WorkUnitState workUnitState : workUnitStates) {
      TestWatermark watermark = new Gson().fromJson(workUnitState.getActualHighWatermark(), TestWatermark.class);
      Assert.assertTrue(watermark.getLongWatermark() == 10L || watermark.getLongWatermark() == 100L);
    }
  }

  @AfterClass
  public void tearDown() throws IOException {
    if (this.fs != null && this.outputPath != null) {
      this.fs.delete(this.outputPath, true);
    }
  }

  public static class TestWatermark implements Watermark {

    private long watermark = -1;

    @Override
    public JsonElement toJson() {
      return WatermarkSerializerHelper.convertWatermarkToJson(this);
    }

    @Override
    public short calculatePercentCompletion(Watermark lowWatermark, Watermark highWatermark) {
      return 0;
    }

    public void setLongWatermark(long watermark) {
      this.watermark = watermark;
    }

    public long getLongWatermark() {
      return this.watermark;
    }
  }
}
