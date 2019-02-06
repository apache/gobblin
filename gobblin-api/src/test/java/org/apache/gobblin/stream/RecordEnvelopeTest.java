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

package org.apache.gobblin.stream;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.gson.JsonElement;

import org.apache.gobblin.ack.BasicAckableForTesting;
import org.apache.gobblin.fork.CopyNotSupportedException;
import org.apache.gobblin.fork.Copyable;
import org.apache.gobblin.source.extractor.CheckpointableWatermark;
import org.apache.gobblin.source.extractor.ComparableWatermark;
import org.apache.gobblin.source.extractor.Watermark;

import lombok.AllArgsConstructor;
import lombok.Getter;


public class RecordEnvelopeTest {

  @Test
  public void testDerivedRecordCreation() {
    BasicAckableForTesting ackable = new BasicAckableForTesting();

    RecordEnvelope<String> record = new RecordEnvelope<>("test", new MyWatermark(101));
    record.addCallBack(ackable);

    RecordEnvelope<String> derivedRecord = record.withRecord("testDerived");
    derivedRecord.ack();

    Assert.assertEquals(ackable.acked, 1);
    Assert.assertEquals(((MyWatermark) derivedRecord.getWatermark()).getId(), 101);
  }

  @Test
  public void testMultipleDerivedRecords() {
    BasicAckableForTesting ackable = new BasicAckableForTesting();

    RecordEnvelope<String> record = new RecordEnvelope<>("test", new MyWatermark(105));
    record.addCallBack(ackable);

    RecordEnvelope<String>.ForkRecordBuilder<String> forkRecordBuilder = record.forkRecordBuilder();

    RecordEnvelope<String> derivedRecord = forkRecordBuilder.childRecord("testDerived");
    derivedRecord.ack();

    // not acked yet as forkRecordBuilder has not been closed
    Assert.assertEquals(ackable.acked, 0);
    Assert.assertEquals(((MyWatermark) derivedRecord.getWatermark()).getId(), 105);

    RecordEnvelope<String> derivedRecord2 = forkRecordBuilder.childRecord("testDerived2");
    derivedRecord2.ack();

    forkRecordBuilder.close();
    Assert.assertEquals(ackable.acked, 1);
    Assert.assertEquals(((MyWatermark) derivedRecord2.getWatermark()).getId(), 105);
  }

  @Test
  public void testClone() {
    BasicAckableForTesting ackable = new BasicAckableForTesting();

    RecordEnvelope<CopyableRecord> record = new RecordEnvelope<>(new CopyableRecord(), new MyWatermark(110));
    record.addCallBack(ackable);

    RecordEnvelope<CopyableRecord> copy = (RecordEnvelope<CopyableRecord>) record.getSingleClone();

    Assert.assertEquals(record.getRecord().id, copy.getRecord().id);
    Assert.assertEquals(((MyWatermark) copy.getWatermark()).getId(), 110);

    copy.ack();
    Assert.assertEquals(ackable.acked, 1);

    try {
      record.getSingleClone();
      Assert.fail();
    } catch (IllegalStateException ise) {
      // expected, cannot clone more than once using getSingleClone
    }
  }

  @Test
  public void testMultipleClones() {
    BasicAckableForTesting ackable = new BasicAckableForTesting();

    RecordEnvelope<CopyableRecord> record = new RecordEnvelope<>(new CopyableRecord(), new MyWatermark(110));
    record.addCallBack(ackable);

    StreamEntity.ForkCloner cloner = record.forkCloner();

    RecordEnvelope<CopyableRecord> copy1 = (RecordEnvelope<CopyableRecord>) cloner.getClone();
    RecordEnvelope<CopyableRecord> copy2 = (RecordEnvelope<CopyableRecord>) cloner.getClone();
    cloner.close();

    Assert.assertEquals(record.getRecord().id, copy1.getRecord().id);
    Assert.assertEquals(((MyWatermark) copy1.getWatermark()).getId(), 110);
    Assert.assertEquals(record.getRecord().id, copy2.getRecord().id);
    Assert.assertEquals(((MyWatermark) copy2.getWatermark()).getId(), 110);

    copy1.ack();
    Assert.assertEquals(ackable.acked, 0);
    copy2.ack();
    Assert.assertEquals(ackable.acked, 1);
  }

  @Test
  public void testRecordMetadata() {
    RecordEnvelope<String> record = new RecordEnvelope<>("test", new MyWatermark(110));

    record.setRecordMetadata("meta1", "value1");
    Assert.assertEquals(record.getRecordMetadata("meta1"), "value1");
  }

  @Test
  public void testRecordMetadataWithDerivedRecords() {
    RecordEnvelope<String> record = new RecordEnvelope<>("test", new MyWatermark(110));

    record.setRecordMetadata("meta1", "value1");
    List list = new ArrayList();
    list.add("item1");
    record.setRecordMetadata("list", list);

    RecordEnvelope<String>.ForkRecordBuilder<String> forkRecordBuilder = record.forkRecordBuilder();

    RecordEnvelope<String> derived1 = forkRecordBuilder.childRecord("testDerived1");
    RecordEnvelope<String> derived2 = forkRecordBuilder.childRecord("testDerived2");
    RecordEnvelope<String> derived3 = derived2.withRecord("testDerived3");


    forkRecordBuilder.close();

    record.setRecordMetadata("meta2", "value2");
    derived1.setRecordMetadata("meta3", "value3");
    derived2.setRecordMetadata("meta4", "value4");
    derived3.setRecordMetadata("meta5", "value5");

    // clones should inherit the metadata at the time of the copy
    Assert.assertEquals(record.getRecordMetadata("meta1"), "value1");
    Assert.assertEquals(derived1.getRecordMetadata("meta1"), "value1");
    Assert.assertEquals(derived2.getRecordMetadata("meta1"), "value1");
    Assert.assertEquals(derived3.getRecordMetadata("meta1"), "value1");

    // new entries should not affect any copies
    Assert.assertEquals(record.getRecordMetadata("meta2"), "value2");
    Assert.assertNull(derived1.getRecordMetadata("meta2"));
    Assert.assertNull(derived2.getRecordMetadata("meta2"));
    Assert.assertNull(derived3.getRecordMetadata("meta2"));

    Assert.assertEquals(derived1.getRecordMetadata("meta3"), "value3");
    Assert.assertNull(record.getRecordMetadata("meta3"));
    Assert.assertNull(derived2.getRecordMetadata("meta3"));
    Assert.assertNull(derived3.getRecordMetadata("meta3"));

    Assert.assertEquals(derived2.getRecordMetadata("meta4"), "value4");
    Assert.assertNull(derived1.getRecordMetadata("meta4"));
    Assert.assertNull(derived3.getRecordMetadata("meta4"));
    Assert.assertNull(record.getRecordMetadata("meta4"));

    Assert.assertEquals(derived3.getRecordMetadata("meta5"), "value5");
    Assert.assertNull(derived1.getRecordMetadata("meta5"));
    Assert.assertNull(derived2.getRecordMetadata("meta5"));
    Assert.assertNull(record.getRecordMetadata("meta5"));

    // no deep copy for values
    ((List)record.getRecordMetadata("list")).add("item2");
    Assert.assertEquals(record.getRecordMetadata("list"), list);
    Assert.assertEquals(derived1.getRecordMetadata("list"), list);
    Assert.assertEquals(derived2.getRecordMetadata("list"), list);
    Assert.assertEquals(derived3.getRecordMetadata("list"), list);
  }

  @Test
  public void testRecordMetadataWithClones() {
    RecordEnvelope<String> record = new RecordEnvelope<>("test", new MyWatermark(110));

    record.setRecordMetadata("meta1", "value1");
    List list = new ArrayList();
    list.add("item1");
    record.setRecordMetadata("list", list);

    StreamEntity.ForkCloner cloner = record.forkCloner();

    RecordEnvelope<String> copy1 = (RecordEnvelope<String>) cloner.getClone();
    RecordEnvelope<String> copy2 = (RecordEnvelope<String>) cloner.getClone();
    cloner.close();
    RecordEnvelope<String> copy3 = (RecordEnvelope<String>)record.buildClone();


    record.setRecordMetadata("meta2", "value2");
    copy1.setRecordMetadata("meta3", "value3");
    copy2.setRecordMetadata("meta4", "value4");
    copy3.setRecordMetadata("meta5", "value5");

    // clones should inherit the metadata at the time of the copy
    Assert.assertEquals(record.getRecordMetadata("meta1"), "value1");
    Assert.assertEquals(copy1.getRecordMetadata("meta1"), "value1");
    Assert.assertEquals(copy2.getRecordMetadata("meta1"), "value1");
    Assert.assertEquals(copy3.getRecordMetadata("meta1"), "value1");

    // new entries should not affect any copies
    Assert.assertEquals(record.getRecordMetadata("meta2"), "value2");
    Assert.assertNull(copy1.getRecordMetadata("meta2"));
    Assert.assertNull(copy2.getRecordMetadata("meta2"));
    Assert.assertNull(copy3.getRecordMetadata("meta2"));

    Assert.assertEquals(copy1.getRecordMetadata("meta3"), "value3");
    Assert.assertNull(record.getRecordMetadata("meta3"));
    Assert.assertNull(copy2.getRecordMetadata("meta3"));
    Assert.assertNull(copy3.getRecordMetadata("meta3"));

    Assert.assertEquals(copy2.getRecordMetadata("meta4"), "value4");
    Assert.assertNull(copy1.getRecordMetadata("meta4"));
    Assert.assertNull(copy3.getRecordMetadata("meta4"));
    Assert.assertNull(record.getRecordMetadata("meta4"));

    Assert.assertEquals(copy3.getRecordMetadata("meta5"), "value5");
    Assert.assertNull(copy1.getRecordMetadata("meta5"));
    Assert.assertNull(copy2.getRecordMetadata("meta5"));
    Assert.assertNull(record.getRecordMetadata("meta5"));

    // no deep copy for values
    ((List)record.getRecordMetadata("list")).add("item2");
    Assert.assertEquals(record.getRecordMetadata("list"), list);
    Assert.assertEquals(copy1.getRecordMetadata("list"), list);
    Assert.assertEquals(copy2.getRecordMetadata("list"), list);
    Assert.assertEquals(copy3.getRecordMetadata("list"), list);
  }

  @AllArgsConstructor
  public static class MyWatermark implements CheckpointableWatermark {
    @Getter
    private final long id;

    @Override
    public String getSource() {
      return "wm";
    }

    @Override
    public ComparableWatermark getWatermark() {
      return null;
    }

    @Override
    public JsonElement toJson() {
      return null;
    }

    @Override
    public short calculatePercentCompletion(Watermark lowWatermark, Watermark highWatermark) {
      return 0;
    }

    @Override
    public int compareTo(CheckpointableWatermark o) {
      return 0;
    }
  }

  @AllArgsConstructor
  public static class CopyableRecord implements Copyable<CopyableRecord> {
    private final long id;

    public CopyableRecord() {
      this.id = new Random().nextLong();
    }

    @Override
    public CopyableRecord copy() throws CopyNotSupportedException {
      return new CopyableRecord(this.id);
    }
  }

}
