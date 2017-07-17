/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package gobblin.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;

import gobblin.source.extractor.CheckpointableWatermark;
import gobblin.source.extractor.DefaultCheckpointableWatermark;
import gobblin.stream.RecordEnvelope;
import gobblin.source.extractor.extract.LongWatermark;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;


@Test
public class MultiWriterWatermarkManagerTest {


  @Test
  public void testConstructor() {

    WatermarkStorage watermarkStorage = null;
    try {
      MultiWriterWatermarkManager watermarkManager = new MultiWriterWatermarkManager(watermarkStorage, 0, Optional.<Logger>absent());
      Assert.fail("Should have thrown an exception");
    } catch (Exception e) {

    }
  }

  /**
   * Test that when no sources are registered, no side effects are observed
   */
  @Test
  public void testNoRegisteredSource()
      throws IOException, InterruptedException {

    WatermarkStorage mockWatermarkStorage = mock(WatermarkStorage.class);
    MultiWriterWatermarkManager watermarkManager = new MultiWriterWatermarkManager(mockWatermarkStorage, 1000, Optional.<Logger>absent());
    try {
      watermarkManager.start();
    } catch (Exception e) {
      Assert.fail("Should not throw exception", e);
    }

    Thread.sleep(2000);

    watermarkManager.close();
    verify(mockWatermarkStorage, times(0)).commitWatermarks(any(Iterable.class));

    MultiWriterWatermarkManager.CommitStatus watermarkMgrStatus = watermarkManager.getCommitStatus();
    Assert.assertTrue(watermarkMgrStatus.getLastCommittedWatermarks().isEmpty(),
        "Last committed watermarks should be empty");
    Assert.assertEquals(watermarkMgrStatus.getLastWatermarkCommitSuccessTimestampMillis(), 0 ,
        "Last committed watermark timestamp should be 0");

  }

  /**
   * Test that when we have commits failing to watermark storage, the manager continues to try
   * at every interval and keeps track of the exception it is seeing.
   */
  @Test
  public void testFailingWatermarkStorage()
      throws IOException, InterruptedException {

    WatermarkStorage reallyBadWatermarkStorage = mock(WatermarkStorage.class);
    IOException exceptionToThrow = new IOException("Failed to write coz the programmer told me to");

    doThrow(exceptionToThrow).when(reallyBadWatermarkStorage).commitWatermarks(any(Iterable.class));


    long commitInterval = 1000;

    MultiWriterWatermarkManager
        watermarkManager = new MultiWriterWatermarkManager(reallyBadWatermarkStorage, commitInterval, Optional.<Logger>absent());

    WatermarkAwareWriter mockWriter = mock(WatermarkAwareWriter.class);
    CheckpointableWatermark watermark = new DefaultCheckpointableWatermark("default", new LongWatermark(0));
    when(mockWriter.getCommittableWatermark()).thenReturn(Collections.singletonMap("default", watermark));
    watermarkManager.registerWriter(mockWriter);
    try {
      watermarkManager.start();
    } catch (Exception e) {
      Assert.fail("Should not throw exception", e);
    }

    Thread.sleep(commitInterval * 2 + (commitInterval/2)); // sleep for 2.5 iterations
    watermarkManager.close();
    int expectedCalls = 3; // 2 calls from iterations, 1 additional attempt due to close
    verify(reallyBadWatermarkStorage, atLeast(expectedCalls)).commitWatermarks(any(Iterable.class));
    Assert.assertEquals(watermarkManager.getCommitStatus().getLastCommitException(), exceptionToThrow,
        "Testing tracking of failed exceptions");

  }


  private WatermarkAwareWriter getFlakyWatermarkWriter(final long failEvery) {
    WatermarkAwareWriter mockWatermarkWriter = new WatermarkAwareWriter() {

      private long watermark = 0;

      @Override
      public boolean isWatermarkCapable() {
        return true;
      }

      @Override
      public void writeEnvelope(RecordEnvelope recordEnvelope)
          throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public Map<String, CheckpointableWatermark> getCommittableWatermark() {
        watermark++;
        if (watermark % failEvery == 0) {
          throw new RuntimeException("Failed because you asked me to");
        }
        return Collections.singletonMap("default",
            (CheckpointableWatermark) new DefaultCheckpointableWatermark("default", new LongWatermark(watermark)));
      }

      @Override
      public Map<String, CheckpointableWatermark> getUnacknowledgedWatermark() {
        return null;
      }

      @Override
      public void write(Object record)
          throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public void commit()
          throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public void cleanup()
          throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public long recordsWritten() {
        return 0;
      }

      @Override
      public long bytesWritten()
          throws IOException {
        return 0;
      }

      @Override
      public void close()
          throws IOException {

      }
    };

    return mockWatermarkWriter;

  }

  /**
   * Test that in the presence of flaky Watermark writers, we continue to log retrieval status correctly
   */
  @Test
  public void testRetrievalStatus()
      throws InterruptedException, IOException {

    WatermarkStorage mockWatermarkStorage = mock(WatermarkStorage.class);

    MultiWriterWatermarkManager watermarkManager = new MultiWriterWatermarkManager(mockWatermarkStorage, 1000, Optional.<Logger>absent());

    watermarkManager.registerWriter(getFlakyWatermarkWriter(2));


    try {
      watermarkManager.start();
    } catch (Exception e) {
      Assert.fail("Should not throw exception", e);
    }

    Thread.sleep(2000);

    watermarkManager.close();

    MultiWriterWatermarkManager.RetrievalStatus retrievalStatus = watermarkManager.getRetrievalStatus();
    Assert.assertTrue(retrievalStatus.getLastWatermarkRetrievalAttemptTimestampMillis() > 0);
    Assert.assertTrue(retrievalStatus.getLastWatermarkRetrievalSuccessTimestampMillis() > 0);
    Assert.assertTrue(retrievalStatus.getLastWatermarkRetrievalFailureTimestampMillis() > 0);
    System.out.println(retrievalStatus);

  }

  /**
   * Test that in the presence of intermittent commit successes and failures, we continue to make progress
   */
  @Test
  public void testFlakyWatermarkStorage()
      throws IOException, InterruptedException {

    final int failEvery = 2;

    WatermarkStorage mockWatermarkStorage = new WatermarkStorage() {
      private int watermarkInstance = 0;
      private List<CheckpointableWatermark> checkpointed = new ArrayList<>();
      @Override
      public void commitWatermarks(java.lang.Iterable<CheckpointableWatermark> watermarks)
          throws IOException {
        ++watermarkInstance;
        if (watermarkInstance % failEvery == 0) {
          throw new IOException("Failed to write");
        } else {
          checkpointed.clear();
          for (CheckpointableWatermark watermark: watermarks) {
            checkpointed.add(watermark);
          }
        }
      }

      @Override
      public Map<String, CheckpointableWatermark> getCommittedWatermarks(
          Class<? extends CheckpointableWatermark> watermarkClass, Iterable<String> sourcePartitions)
          throws IOException {
        return null;
      }
    };


    WatermarkAwareWriter mockWatermarkWriter = new WatermarkAwareWriter() {

      private long watermark = 0;

      @Override
      public boolean isWatermarkCapable() {
        return true;
      }

      @Override
      public void writeEnvelope(RecordEnvelope recordEnvelope)
          throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public Map<String, CheckpointableWatermark> getCommittableWatermark() {
        watermark++;
        return Collections.singletonMap("default",
            (CheckpointableWatermark) new DefaultCheckpointableWatermark("default", new LongWatermark(watermark)));
      }

      @Override
      public Map<String, CheckpointableWatermark> getUnacknowledgedWatermark() {
        return null;
      }

      @Override
      public void write(Object record)
          throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public void commit()
          throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public void cleanup()
          throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public long recordsWritten() {
        return 0;
      }

      @Override
      public long bytesWritten()
          throws IOException {
        return 0;
      }

      @Override
      public void close()
          throws IOException {

      }
    };

    MultiWriterWatermarkManager watermarkManager = new MultiWriterWatermarkManager(mockWatermarkStorage, 1000, Optional.<Logger>absent());

    watermarkManager.registerWriter(mockWatermarkWriter);


    try {
      watermarkManager.start();
    } catch (Exception e) {
      Assert.fail("Should not throw exception", e);
    }

    Thread.sleep(2000);

    watermarkManager.close();

    MultiWriterWatermarkManager.CommitStatus commitStatus = watermarkManager.getCommitStatus();
    System.out.println(commitStatus);
    MultiWriterWatermarkManager.RetrievalStatus retrievalStatus = watermarkManager.getRetrievalStatus();
    Assert.assertTrue(retrievalStatus.getLastWatermarkRetrievalAttemptTimestampMillis() > 0);
    Assert.assertTrue(retrievalStatus.getLastWatermarkRetrievalSuccessTimestampMillis() > 0);
    Assert.assertTrue(retrievalStatus.getLastWatermarkRetrievalFailureTimestampMillis() == 0);
    System.out.println(retrievalStatus);

  }

}
