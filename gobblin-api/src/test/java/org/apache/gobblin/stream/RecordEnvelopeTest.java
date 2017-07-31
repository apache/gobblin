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
