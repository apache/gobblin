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

package gobblin.writer;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import gobblin.test.ConstantTimingType;
import gobblin.test.ErrorManager;
import gobblin.test.NthTimingType;
import gobblin.test.TestUtils;
import gobblin.test.TimingManager;
import gobblin.test.TimingResult;
import gobblin.test.TimingType;


@Slf4j
public class AsyncDataWriterTest {

  class FakeTimedAsyncWriter implements AsyncDataWriter
  {

    TimingManager timingManager;

    public FakeTimedAsyncWriter(TimingManager timingManager) {
      this.timingManager = timingManager;
    }

    @Override
    public void asyncWrite(Object record, final WriteCallback callback) {
      final TimingResult result = this.timingManager.nextTime();
      log.debug("sync: " + result.isSync + " time : " + result.timeValueMillis);
      if (result.isSync)
      {
        try {
          Thread.sleep(result.timeValueMillis);
        } catch (InterruptedException e) {
        }
        callback.onSuccess();
      }
      else
      {
        Thread t = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              log.debug("Sleeping for ms: " + result.timeValueMillis);
              Thread.sleep(result.timeValueMillis);
            } catch (InterruptedException e) {
            }
            callback.onSuccess();
          }
        });
        t.start();
      }
    }


    @Override
    public void cleanup()
        throws IOException {

    }

    @Override
    public long bytesWritten() {
      return 0;
    }

    @Override
    public void close()
        throws IOException {

    }
  }

  @Test
  public void testSlowWriters() throws Exception {
    // Every call incurs 1s latency, commit timeout is 40s
    testAsyncWrites(new ConstantTimingType(1000), 40000, 0, true);
    // Every call incurs 10s latency, commit timeout is 4s
    testAsyncWrites(new ConstantTimingType(10000), 4000, 0, false);
    // Every 7th call incurs 10s latency, every other call incurs 1s latency
    testAsyncWrites(new NthTimingType(7, 1000, 10000), 4000, 0, false);
    // Every 7th call incurs 10s latency, every other call incurs 1s latency, failures allowed < 11%
    testAsyncWrites(new NthTimingType(7, 1000, 10000), 4000, 11, true);

  }

  private void testAsyncWrites(TimingType timingType, long commitTimeoutInMillis,
      double failurePercentage, boolean success)
  {
    TimingManager timingManager = new TimingManager(false, timingType);

    AsyncDataWriter fakeTimedAsyncWriter = new FakeTimedAsyncWriter(timingManager);

    AsyncBestEffortDataWriter asyncWriter = AsyncBestEffortDataWriter.builder()
        .config(ConfigFactory.empty())
        .commitTimeoutInNanos(commitTimeoutInMillis * 1000 * 1000)
        .failureAllowance(failurePercentage/100.0)
        .asyncDataWriter(fakeTimedAsyncWriter)
        .build();

    try {
      for (int i = 0; i < 10; i++) {
        asyncWriter.write(TestUtils.generateRandomBytes());
      }
    }
    catch (Exception e)
    {
      Assert.fail("Should not throw any Exception");
    }

    try {
      asyncWriter.commit();
      if (!success) {
        Assert.fail("Commit should not succeed");
      }
    }
    catch (IOException e)
    {
      if (success)
      {
        Assert.fail("Commit should not throw IOException");
      }
    }
    catch (Exception e)
    {
      Assert.fail("Should not throw any exception other than IOException");
    }
    try {
      asyncWriter.close();
    }
    catch (Exception e)
    {
      Assert.fail("Should not throw any exception on close");
    }


  }

  public class FlakyAsyncWriter<D> implements AsyncDataWriter<D> {

    private final ErrorManager errorManager;

    public FlakyAsyncWriter(ErrorManager errorManager) {
      this.errorManager = errorManager;
    }

    @Override
    public void asyncWrite(D record, WriteCallback callback) {
      boolean error = errorManager.nextError(record);
      if (errorManager.nextError(record))
      {
        final Exception e = new Exception();
        callback.onFailure(e);
      }
      else {
        callback.onSuccess();
      }
    }

    @Override
    public void cleanup()
        throws IOException {
    }

    @Override
    public long bytesWritten() {
      return 0;
    }

    @Override
    public void close()
        throws IOException {

    }
  }

  @Test
  public void testCompleteFailureMode() throws Exception {

    FlakyAsyncWriter flakyAsyncWriter = new FlakyAsyncWriter(gobblin.test.ErrorManager.builder()
        .errorType(gobblin.test.ErrorManager.ErrorType.ALL)
        .build());

    AsyncBestEffortDataWriter asyncBestEffortDataWriter = AsyncBestEffortDataWriter.builder()
        .asyncDataWriter(flakyAsyncWriter)
        .build();
    byte[] messageBytes = TestUtils.generateRandomBytes();
    asyncBestEffortDataWriter.write(messageBytes);

    try {
      asyncBestEffortDataWriter.commit();
    }
    catch (IOException e)
    {
      // ok for commit to throw exception
    }
    finally
    {
      asyncBestEffortDataWriter.close();
    }

    Assert.assertEquals(asyncBestEffortDataWriter.recordsAttempted.getCount(), 1);
    Assert.assertEquals(asyncBestEffortDataWriter.recordsSuccess.getCount(), 0);
    Assert.assertEquals(asyncBestEffortDataWriter.recordsWritten(), 0);
    Assert.assertEquals(asyncBestEffortDataWriter.recordsFailed.getCount(), 1);

  }

}
