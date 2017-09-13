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

package org.apache.gobblin.converter;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

import org.apache.gobblin.ack.BasicAckableForTesting;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metadata.GlobalMetadata;
import org.apache.gobblin.records.RecordStreamWithMetadata;
import org.apache.gobblin.stream.ControlMessage;
import org.apache.gobblin.stream.RecordEnvelope;
import org.apache.gobblin.stream.StreamEntity;

import io.reactivex.Flowable;


public class ConverterTest {

  @Test
  public void testEmptyOutputIterable() throws Exception {
    MyConverter converter = new MyConverter();
    BasicAckableForTesting ackable = new BasicAckableForTesting();

    RecordStreamWithMetadata<Integer, String> stream =
        new RecordStreamWithMetadata<>(Flowable.just(new RecordEnvelope<>(0)),
            GlobalMetadata.<String>builder().schema("schema").build()).mapRecords(r -> {
          r.addCallBack(ackable);
          return r;
        });

    List<StreamEntity<Integer>> outputRecords = Lists.newArrayList();
    converter.processStream(stream, new WorkUnitState()).getRecordStream().subscribe(outputRecords::add);

    Assert.assertEquals(outputRecords.size(), 0);
    Assert.assertEquals(ackable.acked, 1); // record got filtered, acked immediately
  }

  @Test
  public void testSingleOutputIterable() throws Exception {
    MyConverter converter = new MyConverter();
    BasicAckableForTesting ackable = new BasicAckableForTesting();

    RecordStreamWithMetadata<Integer, String> stream =
        new RecordStreamWithMetadata<>(Flowable.just(new RecordEnvelope<>(1)),
            GlobalMetadata.<String>builder().schema("schema").build()).mapRecords(r -> {
          r.addCallBack(ackable);
          return r;
        });

    List<StreamEntity<Integer>> outputRecords = Lists.newArrayList();
    converter.processStream(stream, new WorkUnitState()).getRecordStream().subscribe(outputRecords::add);

    Assert.assertEquals(outputRecords.size(), 1);
    Assert.assertEquals(ackable.acked, 0); // output record has not been acked

    outputRecords.get(0).ack();
    Assert.assertEquals(ackable.acked, 1); // output record acked
  }

  @Test
  public void testMultiOutputIterable() throws Exception {
    MyConverter converter = new MyConverter();
    BasicAckableForTesting ackable = new BasicAckableForTesting();

    RecordStreamWithMetadata<Integer, String> stream =
        new RecordStreamWithMetadata<>(Flowable.just(new RecordEnvelope<>(2)),
            GlobalMetadata.<String>builder().schema("schema").build()).mapRecords(r -> {
          r.addCallBack(ackable);
          return r;
        });

    List<StreamEntity<Integer>> outputRecords = Lists.newArrayList();
    converter.processStream(stream, new WorkUnitState()).getRecordStream().subscribe(outputRecords::add);

    Assert.assertEquals(outputRecords.size(), 2);
    Assert.assertEquals(ackable.acked, 0); // output record has not been acked

    outputRecords.get(0).ack();
    Assert.assertEquals(ackable.acked, 0); // only one output record acked, still need to ack another derived record

    outputRecords.get(1).ack();
    Assert.assertEquals(ackable.acked, 1); // all output records acked
  }

  @Test
  public void testMixedStream() throws Exception {
    MyConverter converter = new MyConverter();
    BasicAckableForTesting ackable = new BasicAckableForTesting();

    RecordStreamWithMetadata<Integer, String> stream =
        new RecordStreamWithMetadata<>(Flowable.just(new RecordEnvelope<>(1), new MyControlMessage<>()),
            GlobalMetadata.<String>builder().schema("schema").build()).mapRecords(r -> {
          r.addCallBack(ackable);
          return r;
        });

    List<StreamEntity<Integer>> outputRecords = Lists.newArrayList();
    converter.processStream(stream, new WorkUnitState()).getRecordStream().subscribe(outputRecords::add);

    Assert.assertEquals(outputRecords.size(), 2);
    Assert.assertEquals(((RecordEnvelope<Integer>) outputRecords.get(0)).getRecord(), new Integer(0));
    Assert.assertTrue(outputRecords.get(1) instanceof MyControlMessage);
  }

  public static class MyConverter extends Converter<String, String, Integer, Integer> {
    @Override
    public String convertSchema(String inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
      return inputSchema;
    }

    @Override
    public Iterable<Integer> convertRecord(String outputSchema, Integer inputRecord, WorkUnitState workUnit)
        throws DataConversionException {
      List<Integer> output = Lists.newArrayList();
      for (int i = 0; i < inputRecord; i++) {
        output.add(0);
      }
      return output;
    }
  }

  public static class MyControlMessage<D> extends ControlMessage<D> {
    @Override
    protected StreamEntity<D> buildClone() {
      return new MyControlMessage<>();
    }
  }

}
