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

package org.apache.gobblin.fork;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metadata.GlobalMetadata;
import org.apache.gobblin.records.RecordStreamWithMetadata;
import org.apache.gobblin.runtime.BasicTestControlMessage;
import org.apache.gobblin.stream.RecordEnvelope;
import org.apache.gobblin.stream.StreamEntity;

import io.reactivex.Flowable;
import lombok.Getter;


public class ForkerTest {

  @Test
  public void test() throws Exception {
    Forker forker = new Forker();
    MyFlowable<StreamEntity<byte[]>> flowable = new MyFlowable<>();

    RecordStreamWithMetadata<byte[], String> stream =
        new RecordStreamWithMetadata<>(flowable, GlobalMetadata.<String>builder().schema("schema").build());

    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(ConfigurationKeys.FORK_BRANCHES_KEY, "3");
    Forker.ForkedStream<byte[], String> forkedStream =  forker.forkStream(stream, new MyForkOperator(), workUnitState);

    Assert.assertEquals(forkedStream.getForkedStreams().size(), 3);

    Queue<StreamEntity<byte[]>> output0 = new LinkedList<>();
    forkedStream.getForkedStreams().get(0).getRecordStream().subscribe(output0::add);
    Queue<StreamEntity<byte[]>> output1 = new LinkedList<>();
    forkedStream.getForkedStreams().get(1).getRecordStream().subscribe(output1::add);
    Queue<StreamEntity<byte[]>> output2 = new LinkedList<>();
    forkedStream.getForkedStreams().get(2).getRecordStream().subscribe(output2::add);

    flowable._subscriber.onNext(new RecordEnvelope<>(new byte[]{1, 1, 1}));
    Assert.assertTrue(output0.poll() instanceof RecordEnvelope);
    Assert.assertTrue(output1.poll() instanceof RecordEnvelope);
    Assert.assertTrue(output2.poll() instanceof RecordEnvelope);

    flowable._subscriber.onNext(new RecordEnvelope<>(new byte[]{1, 0, 0}));
    Assert.assertTrue(output0.poll() instanceof RecordEnvelope);
    Assert.assertNull(output1.poll());
    Assert.assertNull(output2.poll());

    flowable._subscriber.onNext(new RecordEnvelope<>(new byte[]{0, 1, 1}));
    Assert.assertNull(output0.poll());
    Assert.assertTrue(output1.poll() instanceof RecordEnvelope);
    Assert.assertTrue(output2.poll() instanceof RecordEnvelope);

    flowable._subscriber.onNext(new BasicTestControlMessage<byte[]>("control"));
    Assert.assertTrue(output0.poll() instanceof BasicTestControlMessage);
    Assert.assertTrue(output1.poll() instanceof BasicTestControlMessage);
    Assert.assertTrue(output2.poll() instanceof BasicTestControlMessage);

    flowable._subscriber.onComplete();
  }

  public static class MyForkOperator implements ForkOperator<String, byte[]> {
    @Override
    public void init(WorkUnitState workUnitState) throws Exception {

    }

    @Override
    public int getBranches(WorkUnitState workUnitState) {
      return workUnitState.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY);
    }

    @Override
    public List<Boolean> forkSchema(WorkUnitState workUnitState, String input) {
      return Collections.nCopies(getBranches(workUnitState), true);
    }

    @Override
    public List<Boolean> forkDataRecord(WorkUnitState workUnitState, byte[] input) {
      List<Boolean> output = Lists.newArrayList();
      for (byte b : input) {
        output.add(b > 0);
      }
      return output;
    }

    @Override
    public void close() throws IOException {

    }
  }

  public static class MyFlowable<T> extends Flowable<T> {
    @Getter
    Subscriber<? super T> _subscriber;

    @Override
    protected void subscribeActual(Subscriber<? super T> s) {
      s.onSubscribe(new Subscription() {
        @Override
        public void request(long n) {
          // do nothing
        }

        @Override
        public void cancel() {
          // do nothing
        }
      });
      _subscriber = s;
    }
  }

}
