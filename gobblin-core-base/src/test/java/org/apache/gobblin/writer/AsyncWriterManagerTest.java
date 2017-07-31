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

package org.apache.gobblin.writer;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Throwables;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.metrics.RootMetricContext;
import org.apache.gobblin.metrics.reporter.OutputStreamReporter;
import org.apache.gobblin.test.ConstantTimingType;
import org.apache.gobblin.test.ErrorManager;
import org.apache.gobblin.test.NthTimingType;
import org.apache.gobblin.test.TestUtils;
import org.apache.gobblin.test.TimingManager;
import org.apache.gobblin.test.TimingResult;
import org.apache.gobblin.test.TimingType;


@Slf4j
public class AsyncWriterManagerTest {

  class FakeTimedAsyncWriter implements AsyncDataWriter {

    TimingManager timingManager;

    public FakeTimedAsyncWriter(TimingManager timingManager) {
      this.timingManager = timingManager;
    }

    @Override
    public Future<WriteResponse> write(final Object record, final WriteCallback callback) {
      final TimingResult result = this.timingManager.nextTime();
      log.debug("sync: " + result.isSync + " time : " + result.timeValueMillis);
      final FutureWrappedWriteCallback futureCallback = new FutureWrappedWriteCallback(callback);
      if (result.isSync) {
        try {
          Thread.sleep(result.timeValueMillis);
        } catch (InterruptedException e) {
        }
        futureCallback.onSuccess(new GenericWriteResponse(record));
      } else {
        Thread t = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              log.debug("Sleeping for ms: " + result.timeValueMillis);
              Thread.sleep(result.timeValueMillis);
            } catch (InterruptedException e) {
            }
            futureCallback.onSuccess(new GenericWriteResponse(record));
          }
        });
        t.setDaemon(true);
        t.start();
      }
      return futureCallback;
    }

    @Override
    public void flush()
        throws IOException {

    }

    @Override
    public void close()
        throws IOException {

    }
  }

  @Test
  public void testSlowWriters()
      throws Exception {
    // Every call incurs 1s latency, commit timeout is 40s
    testAsyncWrites(new ConstantTimingType(1000), 40000, 0, true);
    // Every call incurs 10s latency, commit timeout is 4s
    testAsyncWrites(new ConstantTimingType(10000), 4000, 0, false);
    // Every 7th call incurs 10s latency, every other call incurs 1s latency
    testAsyncWrites(new NthTimingType(7, 1000, 10000), 4000, 0, false);
    // Every 7th call incurs 10s latency, every other call incurs 1s latency, failures allowed < 11%
    testAsyncWrites(new NthTimingType(7, 1000, 10000), 4000, 11, true);
  }

  private void testAsyncWrites(TimingType timingType, long commitTimeoutInMillis, double failurePercentage,
      boolean success) {
    TimingManager timingManager = new TimingManager(false, timingType);

    AsyncDataWriter fakeTimedAsyncWriter = new FakeTimedAsyncWriter(timingManager);

    AsyncWriterManager asyncWriter = AsyncWriterManager.builder().config(ConfigFactory.empty())
        .commitTimeoutMillis(commitTimeoutInMillis).failureAllowanceRatio(failurePercentage / 100.0)
        .asyncDataWriter(fakeTimedAsyncWriter).build();

    try {
      for (int i = 0; i < 10; i++) {
        asyncWriter.write(TestUtils.generateRandomBytes());
      }
    } catch (Exception e) {
      Assert.fail("Should not throw any Exception");
    }

    try {
      asyncWriter.commit();
      if (!success) {
        Assert.fail("Commit should not succeed");
      }
    } catch (IOException e) {
      if (success) {
        Assert.fail("Commit should not throw IOException");
      }
    } catch (Exception e) {
      Assert.fail("Should not throw any exception other than IOException");
    }
    try {
      asyncWriter.close();
    } catch (Exception e) {
      Assert.fail("Should not throw any exception on close");
    }
  }

  public class FlakyAsyncWriter<D> implements AsyncDataWriter<D> {

    private final ErrorManager errorManager;

    public FlakyAsyncWriter(ErrorManager errorManager) {
      this.errorManager = errorManager;
    }

    @Override
    public Future<WriteResponse> write(final D record, WriteCallback callback) {
      final boolean error = this.errorManager.nextError(record);
      final FutureWrappedWriteCallback futureWrappedWriteCallback = new FutureWrappedWriteCallback(callback);
      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
          if (error) {
            final Exception e = new Exception();
            futureWrappedWriteCallback.onFailure(e);
          } else {
            futureWrappedWriteCallback.onSuccess(new GenericWriteResponse(record));
          }
        }
      });
      t.setDaemon(true);
      t.start();
      return futureWrappedWriteCallback;
    }

    @Override
    public void flush()
        throws IOException {

    }

    @Override
    public void close()
        throws IOException {

    }
  }

  @Test
  public void testCompleteFailureMode()
      throws Exception {

    FlakyAsyncWriter flakyAsyncWriter = new FlakyAsyncWriter(
        org.apache.gobblin.test.ErrorManager.builder().errorType(org.apache.gobblin.test.ErrorManager.ErrorType.ALL).build());

    AsyncWriterManager asyncWriterManager =
        AsyncWriterManager.builder().asyncDataWriter(flakyAsyncWriter).retriesEnabled(true).numRetries(5).build();
    byte[] messageBytes = TestUtils.generateRandomBytes();
    asyncWriterManager.write(messageBytes);

    try {
      asyncWriterManager.commit();
    } catch (IOException e) {
      // ok for commit to throw exception
    } finally {
      asyncWriterManager.close();
    }

    Assert.assertEquals(asyncWriterManager.recordsIn.getCount(), 1);
    Assert.assertEquals(asyncWriterManager.recordsAttempted.getCount(), 6);
    Assert.assertEquals(asyncWriterManager.recordsSuccess.getCount(), 0);
    Assert.assertEquals(asyncWriterManager.recordsWritten(), 0);
    Assert.assertEquals(asyncWriterManager.recordsFailed.getCount(), 1);
  }

  @Test
  public void testFlakyWritersWithRetries()
      throws Exception {

    FlakyAsyncWriter flakyAsyncWriter = new FlakyAsyncWriter(
        org.apache.gobblin.test.ErrorManager.builder().errorType(ErrorManager.ErrorType.NTH).errorEvery(4).build());

    AsyncWriterManager asyncWriterManager =
        AsyncWriterManager.builder().asyncDataWriter(flakyAsyncWriter).retriesEnabled(true).numRetries(5).build();
    for (int i = 0; i < 100; ++i) {
      byte[] messageBytes = TestUtils.generateRandomBytes();
      asyncWriterManager.write(messageBytes);
    }

    try {
      asyncWriterManager.commit();
    } catch (IOException e) {
      // ok for commit to throw exception
    } finally {
      asyncWriterManager.close();
    }

    log.info(asyncWriterManager.recordsAttempted.getCount() + "");
    Assert.assertEquals(asyncWriterManager.recordsIn.getCount(), 100);
    Assert.assertTrue(asyncWriterManager.recordsAttempted.getCount() > 100);
    Assert.assertEquals(asyncWriterManager.recordsSuccess.getCount(), 100);
    Assert.assertEquals(asyncWriterManager.recordsFailed.getCount(), 0);
  }

  /**
   * In the presence of lots of failures, the manager should slow down
   * and not overwhelm the system.
   */
  @Test
  public void testFlowControlWithWriteFailures()
      throws Exception {

    FlakyAsyncWriter flakyAsyncWriter =
        new FlakyAsyncWriter(org.apache.gobblin.test.ErrorManager.builder().errorType(ErrorManager.ErrorType.ALL).build());

    int maxOutstandingWrites = 2000;

    final AsyncWriterManager asyncWriterManager =
        AsyncWriterManager.builder().asyncDataWriter(flakyAsyncWriter).retriesEnabled(true).numRetries(5)
            .maxOutstandingWrites(maxOutstandingWrites).failureAllowanceRatio(1.0)  // ok to fail all the time
            .build();

    boolean verbose = false;
    if (verbose) {
      // Create a reporter for metrics. This reporter will write metrics to STDOUT.
      OutputStreamReporter.Factory.newBuilder().build(new Properties());
      // Start all metric reporters.
      RootMetricContext.get().startReporting();
    }
    final int load = 10000; // 10k records per sec
    final long tickDiffInNanos = (1000 * 1000 * 1000) / load;

    final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    scheduler.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        GenericRecord record = TestUtils.generateRandomAvroRecord();
        try {
          asyncWriterManager.write(record);
        } catch (IOException e) {
          log.error("Failure during write", e);
          Throwables.propagate(e);
        }
      }
    }, 0, tickDiffInNanos, TimeUnit.NANOSECONDS);

    LinkedBlockingQueue retryQueue = (LinkedBlockingQueue) asyncWriterManager.retryQueue.get();

    int sleepTime = 100;
    int totalTime = 10000;
    for (int i = 0; i < (totalTime / sleepTime); ++i) {
      Thread.sleep(sleepTime);
      int retryQueueSize = retryQueue.size();
      Assert.assertTrue(retryQueueSize <= (maxOutstandingWrites + 1),
          "Retry queue should never exceed the " + "maxOutstandingWrites. Found " + retryQueueSize);
      log.debug("Retry queue size = {}", retryQueue.size());
    }

    scheduler.shutdown();
    asyncWriterManager.commit();
    long recordsIn = asyncWriterManager.recordsIn.getCount();
    long recordsAttempted = asyncWriterManager.recordsAttempted.getCount();
    String msg = String.format("recordsIn = %d, recordsAttempted = %d.", recordsIn, recordsAttempted);
    log.info(msg);
    Assert.assertTrue(recordsAttempted > recordsIn, "There must have been a bunch of failures");
    Assert.assertTrue(retryQueue.size() == 0, "Retry queue should be empty");
  }
}
