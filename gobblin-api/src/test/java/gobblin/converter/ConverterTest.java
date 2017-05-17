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

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.RecordEnvelope;


public class ConverterTest {

  @Test
  public void testStream() {

    Set<String> recordsAcked = Sets.newHashSet();

    Stream<RecordEnvelope<String>> records = Lists.newArrayList("foo", "bar").stream().map(RecordEnvelope::new)
        .peek(env -> env.addSuccessCallback(() -> recordsAcked.add(env.getRecord())));
    MyConverter converter = new MyConverter();
    Stream<RecordEnvelope<String>> convertedStream = converter.convertRecordStream("schema", records, new WorkUnitState());

    Map<String, RecordEnvelope<String>> outputRecords = convertedStream.collect(Collectors.toMap(RecordEnvelope::getRecord, x -> x));

    Assert.assertEquals(outputRecords.size(), 4);
    Assert.assertTrue(converter.recordsAcked.isEmpty());
    Assert.assertTrue(recordsAcked.isEmpty());

    outputRecords.get("foo1").ack();
    Assert.assertTrue(recordsAcked.isEmpty());
    Assert.assertEquals(converter.recordsAcked, Sets.newHashSet("foo1"));

    outputRecords.get("bar1").ack();
    Assert.assertEquals(converter.recordsAcked, Sets.newHashSet("foo1", "bar1"));
    Assert.assertEquals(recordsAcked, Sets.newHashSet("bar"));

    outputRecords.get("foo2").ack();
    outputRecords.get("foo3").ack();
    Assert.assertEquals(converter.recordsAcked, Sets.newHashSet("foo1", "foo2", "foo3", "bar1"));
    Assert.assertEquals(recordsAcked, Sets.newHashSet("foo", "bar"));
  }

  private static class MyConverter extends Converter<String, String, String, String> {
    private Set<String> recordsAcked = Sets.newHashSet();

    @Override
    public String convertSchema(String inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
      return inputSchema;
    }

    @Override
    public void modifyOutputEnvelope(RecordEnvelope<String> recordEnvelope) {
      recordEnvelope.addSuccessCallback(() -> recordsAcked.add(recordEnvelope.getRecord()));
      super.modifyOutputEnvelope(recordEnvelope);
    }

    @Override
    public Iterable<String> convertRecord(String outputSchema, String inputRecord, WorkUnitState workUnit)
        throws DataConversionException {
      if (inputRecord.equals("foo")) {
        return Lists.newArrayList(inputRecord + "1", inputRecord + "2", inputRecord + "3");
      } else {
        return Lists.newArrayList(inputRecord + "1");
      }
    }
  }

}
