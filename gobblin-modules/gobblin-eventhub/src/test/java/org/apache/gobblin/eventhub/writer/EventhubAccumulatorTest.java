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
package org.apache.gobblin.eventhub.writer;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.gobblin.writer.SequentialBasedBatchAccumulator;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.apache.gobblin.writer.WriteCallback;


public class EventhubAccumulatorTest {
  private CountDownLatch latchEmpty = new CountDownLatch(1);
  private CountDownLatch latchCapacity = new CountDownLatch(1);

  @Test
  public void testAccumulatorEmpty() throws IOException, InterruptedException{
    SequentialBasedBatchAccumulator accumulator = new EventhubBatchAccumulator(64, 1000, 5);

    // Spawn a new thread to add new batches
    (new Thread(new AddBatchThread(accumulator))).start();

    // Below three get operation will be blocked until we fill the empty queue
    accumulator.getNextAvailableBatch();
    accumulator.getNextAvailableBatch();
    accumulator.getNextAvailableBatch();

    this.latchEmpty.await();
    // The spawned thread should unblock current thread because it removes some front batches
    Assert.assertTrue(accumulator.getNumOfBatches() >=2);
    Assert.assertTrue(accumulator.getNumOfBatches() <=5);
  }

  @Test
  public void testAccumulatorCapacity () throws IOException, InterruptedException {
    SequentialBasedBatchAccumulator accumulator = new EventhubBatchAccumulator(64, 1000, 5);
    StringBuffer buffer = new StringBuffer();
    for (int i = 0; i < 40; ++i) {
      buffer.append('a');
    }
    String record = buffer.toString();
    accumulator.append(record, WriteCallback.EMPTY);
    accumulator.append(record, WriteCallback.EMPTY);
    accumulator.append(record, WriteCallback.EMPTY);
    accumulator.append(record, WriteCallback.EMPTY);
    accumulator.append(record, WriteCallback.EMPTY);

    // Spawn a new thread to remove available batches
    (new Thread(new RemoveBatchThread(accumulator))).start();

    // Flowing two appends will be blocked
    accumulator.append(record, WriteCallback.EMPTY);
    accumulator.append(record, WriteCallback.EMPTY);

    this.latchCapacity.await();
    // The spawned thread should unblock current thread because it removes some front batches
    Assert.assertEquals(accumulator.getNumOfBatches(), 4);
  }

  @Test
  public void testCloseBeforeAwait () throws IOException, InterruptedException {
    SequentialBasedBatchAccumulator accumulator = new EventhubBatchAccumulator(64, 1000, 5);
    (new Thread(new CloseAccumulatorThread(accumulator))).start();
    Thread.sleep(1000);

    Assert.assertNull(accumulator.getNextAvailableBatch());
  }

  @Test
  public void testCloseAfterAwait () throws IOException, InterruptedException {
    SequentialBasedBatchAccumulator accumulator = new EventhubBatchAccumulator(64, 1000, 5);
    (new Thread(new CloseAccumulatorThread(accumulator))).start();

    // this thread should be blocked and waked up by spawned thread
    Assert.assertNull(accumulator.getNextAvailableBatch());
  }

  @Test
  public void testClose () throws IOException, InterruptedException {
    SequentialBasedBatchAccumulator accumulator = new EventhubBatchAccumulator(64, 3000, 5);
    StringBuffer buffer = new StringBuffer();
    for (int i = 0; i < 40; ++i) {
      buffer.append('a');
    }
    String record1 = buffer.toString() + "1";
    String record2 = buffer.toString() + "2";
    String record3 = buffer.toString() + "3";
    String record4 = buffer.toString() + "4";
    String record5 = buffer.toString() + "5";
    accumulator.append(record1, WriteCallback.EMPTY);
    accumulator.append(record2, WriteCallback.EMPTY);
    accumulator.append(record3, WriteCallback.EMPTY);
    accumulator.append(record4, WriteCallback.EMPTY);
    accumulator.append(record5, WriteCallback.EMPTY);

    (new Thread(new CloseAccumulatorThread(accumulator))).start();
    Thread.sleep(1000);
    Assert.assertEquals(accumulator.getNextAvailableBatch().getRecords().get(0), record1);
    Assert.assertEquals(accumulator.getNextAvailableBatch().getRecords().get(0), record2);
    Assert.assertEquals(accumulator.getNextAvailableBatch().getRecords().get(0), record3);
    Assert.assertEquals(accumulator.getNextAvailableBatch().getRecords().get(0), record4);
    Assert.assertEquals(accumulator.getNextAvailableBatch().getRecords().get(0), record5);
  }

  @Test
  public void testExpiredBatch () throws IOException, InterruptedException {
    SequentialBasedBatchAccumulator accumulator = new EventhubBatchAccumulator(64, 3000, 5);
    String record = "1";
    accumulator.append(record, WriteCallback.EMPTY);
    Assert.assertNull(accumulator.getNextAvailableBatch());
    Thread.sleep(3000);
    Assert.assertNotNull(accumulator.getNextAvailableBatch());
  }

  public class CloseAccumulatorThread implements Runnable {
    SequentialBasedBatchAccumulator accumulator;
    public CloseAccumulatorThread (SequentialBasedBatchAccumulator accumulator) {
      this.accumulator = accumulator;
    }
    public void run() {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
      }
      this.accumulator.close();
    }
  }

  public class RemoveBatchThread implements Runnable {
    SequentialBasedBatchAccumulator accumulator;
    public RemoveBatchThread (SequentialBasedBatchAccumulator accumulator) {
      this.accumulator = accumulator;
    }
    public void run() {
      try {
        Thread.sleep(1000);
        this.accumulator.getNextAvailableBatch();
        this.accumulator.getNextAvailableBatch();
        this.accumulator.getNextAvailableBatch();
        latchCapacity.countDown();
      } catch (InterruptedException e) {
      }
    }
  }

  public class AddBatchThread implements Runnable {
    SequentialBasedBatchAccumulator accumulator;
    public AddBatchThread (SequentialBasedBatchAccumulator accumulator) {
      this.accumulator = accumulator;
    }
    public void run() {
      try {
        Thread.sleep(1000);
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < 40; ++i) {
          buffer.append('a');
        }
        String record = buffer.toString();
        accumulator.append(record, WriteCallback.EMPTY);
        accumulator.append(record, WriteCallback.EMPTY);
        accumulator.append(record, WriteCallback.EMPTY);
        accumulator.append(record, WriteCallback.EMPTY);
        accumulator.append(record, WriteCallback.EMPTY);
        latchEmpty.countDown();
      } catch (InterruptedException e) {
      }
    }
  }
}
