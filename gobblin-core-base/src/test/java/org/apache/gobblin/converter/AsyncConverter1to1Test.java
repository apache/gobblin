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

package gobblin.converter;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import gobblin.configuration.WorkUnitState;
import gobblin.records.RecordStreamWithMetadata;
import gobblin.stream.RecordEnvelope;
import gobblin.util.ExponentialBackoff;

import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;


public class AsyncConverter1to1Test {

  @Test
  public void test1to1() throws Exception {
    MyAsyncConverter1to1 converter = new MyAsyncConverter1to1();

    List<Throwable> errors = Lists.newArrayList();
    AtomicBoolean done = new AtomicBoolean(false);

    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(AsyncConverter1to1.MAX_CONCURRENT_ASYNC_CONVERSIONS_KEY, 3);

    RecordStreamWithMetadata<String, String> stream =
        new RecordStreamWithMetadata<>(Flowable.range(0, 5).map(i -> i.toString()).map(RecordEnvelope::new), "schema");

    Set<String> outputRecords = Sets.newConcurrentHashSet();

    converter.processStream(stream, workUnitState).getRecordStream().subscribeOn(Schedulers.newThread())
        .subscribe(r -> outputRecords.add(((RecordEnvelope<String>)r).getRecord()), errors::add, () -> done.set(true));

    // Release record 0
    Assert.assertTrue(
        ExponentialBackoff.awaitCondition().maxWait(100L).callable(() -> converter.completeFutureIfPresent("0")).await());
    Assert.assertTrue(ExponentialBackoff.awaitCondition().maxWait(100L).callable(() -> outputRecords.contains("0")).await());
    Assert.assertEquals(outputRecords.size(), 1);

    // Record 4 should not be in the queue yet (max concurrent conversions is 3).
    Assert.assertFalse(
        ExponentialBackoff.awaitCondition().maxWait(200L).callable(() -> converter.completeFutureIfPresent("4")).await());

    // Release record 3 (out of order)
    Assert.assertTrue(
        ExponentialBackoff.awaitCondition().maxWait(100L).callable(() -> converter.completeFutureIfPresent("3")).await());
    Assert.assertTrue(ExponentialBackoff.awaitCondition().maxWait(100L).callable(() -> outputRecords.contains("3")).await());
    // only two records have been released
    Assert.assertEquals(outputRecords.size(), 2);

    // Release record 4 (now in queue)
    Assert.assertTrue(
        ExponentialBackoff.awaitCondition().maxWait(100L).callable(() -> converter.completeFutureIfPresent("4")).await());
    Assert.assertTrue(ExponentialBackoff.awaitCondition().maxWait(100L).callable(() -> outputRecords.contains("4")).await());
    Assert.assertEquals(outputRecords.size(), 3);

    // Release records 1 and 2
    Assert.assertTrue(
        ExponentialBackoff.awaitCondition().maxWait(100L).callable(() -> converter.completeFutureIfPresent("1")).await());
    Assert.assertTrue(
        ExponentialBackoff.awaitCondition().maxWait(100L).callable(() -> converter.completeFutureIfPresent("2")).await());

    Assert.assertTrue(ExponentialBackoff.awaitCondition().maxWait(100L).callable(() -> outputRecords.size() == 5).await());
    Assert.assertEquals(outputRecords, Sets.newHashSet("0", "1", "2", "3", "4"));

    Assert.assertTrue(errors.isEmpty());
    Assert.assertTrue(done.get());
  }

  @Test
  public void testFailedConversion() throws Exception {
    MyAsyncConverter1to1 converter = new MyAsyncConverter1to1();

    List<Throwable> errors = Lists.newArrayList();
    AtomicBoolean done = new AtomicBoolean(false);

    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(AsyncConverter1to1.MAX_CONCURRENT_ASYNC_CONVERSIONS_KEY, 3);

    RecordStreamWithMetadata<String, String> stream =
        new RecordStreamWithMetadata<>(Flowable.just("0", MyAsyncConverter1to1.FAIL, "1").map(RecordEnvelope::new), "schema");

    Set<String> outputRecords = Sets.newConcurrentHashSet();

    converter.processStream(stream, workUnitState).getRecordStream().subscribeOn(Schedulers.newThread())
        .subscribe(r -> outputRecords.add(((RecordEnvelope<String>)r).getRecord()), errors::add, () -> done.set(true));

    Assert.assertTrue(ExponentialBackoff.awaitCondition().maxWait(100L).callable(() -> errors.size() > 0).await());

    Assert.assertEquals(errors.size(), 1);
    Assert.assertEquals(errors.get(0).getCause().getMessage(), "injected failure");
  }

  public static class MyAsyncConverter1to1 extends AsyncConverter1to1<String, String, String, String> {

    public static final String FAIL = "fail";

    Map<String, CompletableFuture<String>> futures = Maps.newConcurrentMap();

    @Override
    public String convertSchema(String inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
      return inputSchema;
    }

    @Override
    protected CompletableFuture<String> convertRecordAsync(String outputSchema, String inputRecord, WorkUnitState workUnit)
        throws DataConversionException {
      CompletableFuture<String> future = new CompletableFuture<>();
      if (inputRecord.equals(FAIL)) {
        future.completeExceptionally(new DataConversionException("injected failure"));
      } else {
        this.futures.put(inputRecord, future);
      }
      return future;
    }

    public boolean completeFutureIfPresent(String record) {
      if (this.futures.containsKey(record)) {
        this.futures.get(record).complete(record);
        this.futures.remove(record);
        return true;
      }
      return false;
    }
  }

}
