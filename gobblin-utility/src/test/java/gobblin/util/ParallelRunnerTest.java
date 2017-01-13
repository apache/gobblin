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

package gobblin.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Queues;
import com.google.common.io.Closer;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Watermark;
import gobblin.source.extractor.WatermarkSerializerHelper;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.io.SeekableFSInputStream;


/**
 * Unit tests for {@link ParallelRunner}.
 *
 * @author Yinan Li
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
    try (ParallelRunner parallelRunner = new ParallelRunner(2, this.fs)) {
      WorkUnit workUnit1 = WorkUnit.createEmpty();
      workUnit1.setProp("foo", "bar");
      workUnit1.setProp("a", 10);
      parallelRunner.serializeToFile(workUnit1, new Path(this.outputPath, "wu1"));

      WorkUnit workUnit2 = WorkUnit.createEmpty();
      workUnit2.setProp("foo", "baz");
      workUnit2.setProp("b", 20);
      parallelRunner.serializeToFile(workUnit2, new Path(this.outputPath, "wu2"));
    }
  }

  @Test(dependsOnMethods = "testSerializeToFile")
  public void testDeserializeFromFile() throws IOException {
    WorkUnit workUnit1 = WorkUnit.createEmpty();
    WorkUnit workUnit2 = WorkUnit.createEmpty();

    try (ParallelRunner parallelRunner = new ParallelRunner(2, this.fs)) {
      parallelRunner.deserializeFromFile(workUnit1, new Path(this.outputPath, "wu1"));
      parallelRunner.deserializeFromFile(workUnit2, new Path(this.outputPath, "wu2"));
    }

    Assert.assertEquals(workUnit1.getPropertyNames().size(), 2);
    Assert.assertEquals(workUnit1.getProp("foo"), "bar");
    Assert.assertEquals(workUnit1.getPropAsInt("a"), 10);

    Assert.assertEquals(workUnit2.getPropertyNames().size(), 2);
    Assert.assertEquals(workUnit2.getProp("foo"), "baz");
    Assert.assertEquals(workUnit2.getPropAsInt("b"), 20);
  }

  @Test
  @SuppressWarnings("deprecation")
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

    Path seqPath1 = new Path(this.outputPath, "seq1");
    Path seqPath2 = new Path(this.outputPath, "seq2");

    try (ParallelRunner parallelRunner = new ParallelRunner(2, this.fs)) {
      parallelRunner.deserializeFromSequenceFile(Text.class, WorkUnitState.class, seqPath1, workUnitStates, true);
      parallelRunner.deserializeFromSequenceFile(Text.class, WorkUnitState.class, seqPath2, workUnitStates, true);
    }

    Assert.assertFalse(this.fs.exists(seqPath1));
    Assert.assertFalse(this.fs.exists(seqPath2));

    Assert.assertEquals(workUnitStates.size(), 2);

    for (WorkUnitState workUnitState : workUnitStates) {
      TestWatermark watermark = new Gson().fromJson(workUnitState.getActualHighWatermark(), TestWatermark.class);
      Assert.assertTrue(watermark.getLongWatermark() == 10L || watermark.getLongWatermark() == 100L);
    }
  }

  @Test
  public void testMovePath() throws IOException, URISyntaxException {
    String expected = "test";
    ByteArrayOutputStream actual = new ByteArrayOutputStream();

    Path src = new Path("/src/file.txt");
    Path dst = new Path("/dst/file.txt");
    FileSystem fs1 = Mockito.mock(FileSystem.class);
    Mockito.when(fs1.exists(src)).thenReturn(true);
    Mockito.when(fs1.isFile(src)).thenReturn(true);
    Mockito.when(fs1.getUri()).thenReturn(new URI("fs1:////"));
    Mockito.when(fs1.getFileStatus(src)).thenReturn(new FileStatus(1, false, 1, 1, 1, src));
    Mockito.when(fs1.open(src))
            .thenReturn(new FSDataInputStream(new SeekableFSInputStream(new ByteArrayInputStream(expected.getBytes()))));
    Mockito.when(fs1.delete(src, true)).thenReturn(true);

    FileSystem fs2 = Mockito.mock(FileSystem.class);
    Mockito.when(fs2.exists(dst)).thenReturn(false);
    Mockito.when(fs2.getUri()).thenReturn(new URI("fs2:////"));
    Mockito.when(fs2.getConf()).thenReturn(new Configuration());
    Mockito.when(fs2.create(dst, false)).thenReturn(new FSDataOutputStream(actual, null));

    try (ParallelRunner parallelRunner = new ParallelRunner(1, fs1)) {
      parallelRunner.movePath(src, fs2, dst, Optional.<String>absent());
    }

    Assert.assertEquals(actual.toString(), expected);
  }

  @Test(groups = { "ignore" })
  public void testWaitsForFuturesWhenClosing() throws IOException, InterruptedException {
    final AtomicBoolean flag = new AtomicBoolean();
    final CountDownLatch latch1 = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(1);
    Path src1 = new Path("/src/file1.txt");
    Path src2 = new Path("/src/file2.txt");
    FileSystem fs = Mockito.mock(FileSystem.class);
    Mockito.when(fs.exists(src1)).thenReturn(true);
    Mockito.when(fs.delete(src1, true)).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        latch1.countDown();
        return false;
      }
    });
    Mockito.when(fs.exists(src2)).thenReturn(true);
    Mockito.when(fs.delete(src2, true)).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        latch1.await();
        long end = System.currentTimeMillis() + 70000;
        while (System.currentTimeMillis() < end) {
          try {
            Thread.sleep(Math.max(1, end - System.currentTimeMillis()));
          } catch (Exception ignored) {
          }
        }
        flag.set(true);
        latch2.countDown();
        return true;
      }
    });

    boolean caughtException = false;
    ParallelRunner parallelRunner = new ParallelRunner(2, fs);
    try {
      parallelRunner.deletePath(src1, true);
      parallelRunner.deletePath(src2, true);
      System.out.println(System.currentTimeMillis() + ": START - ParallelRunner.close()");
      parallelRunner.close();
    } catch (IOException e) {
      caughtException = true;
    }
    Assert.assertTrue(caughtException);
    System.out.println(System.currentTimeMillis() + ": END   - ParallelRunner.close()");
    System.out.println(System.currentTimeMillis() + ": Waiting for unkillable task to finish...");
    latch2.await();
    System.out.println(System.currentTimeMillis() + ": Unkillable task completed.");
    Assert.assertTrue(flag.get());
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
