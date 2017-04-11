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

package gobblin.compaction.mapreduce.avro;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Test class for {@link AvroKeyDedupReducer}.
 */
public class AvroKeyDedupReducerTest {
  private static final String KEY_SCHEMA =
      "{ \"type\" : \"record\",  \"name\" : \"etl\",\"namespace\" : \"reducerTest\",  \"fields\" : [ { \"name\" : "
          + "\"key\", \"type\" : {\"type\" : \"record\", \"name\" : \"key_name\", \"namespace\" : \"key_namespace\",  "
          + "\"fields\" : [ {\"name\" : \"partitionKey\", \"type\" : \"long\", \"doc\" : \"\"}, { \"name\" : \"environment"
          + "\", \"type\" : \"string\",\"doc\" : \"\"}, {\"name\" : \"subKey\",\"type\" : \"string\", \"doc\" : \"\"} ]}, "
          + "\"doc\" : \"\", \"attributes_json\" : \"{\\\"delta\\\":false,\\\"pk\\\":true}\" }]}";

  private static final String FULL_SCHEMA =
      "{ \"type\" : \"record\",  \"name\" : \"etl\",\"namespace\" : \"reducerTest\",  \"fields\" : [ { \"name\" : "
          + "\"key\", \"type\" : {\"type\" : \"record\", \"name\" : \"key_name\", \"namespace\" : \"key_namespace\",  "
          + "\"fields\" : [ {\"name\" : \"partitionKey\", \"type\" : \"long\", \"doc\" : \"\"}, { \"name\" : \"environment"
          + "\", \"type\" : \"string\",\"doc\" : \"\"}, {\"name\" : \"subKey\",\"type\" : \"string\", \"doc\" : \"\"} ]}, "
          + "\"doc\" : \"\", \"attributes_json\" : \"{\\\"delta\\\":false,\\\"pk\\\":true}\" }"
          + ", {\"name\" : \"scn2\", \"type\": \"long\", \"doc\" : \"\", \"attributes_json\" : \"{\\\"nullable\\\":false,\\\"delta"
          + "\\\":false,\\\"pk\\\":false,\\\"type\\\":\\\"NUMBER\\\"}\"}"
          + " , {\"name\" : \"scn\", \"type\": \"long\", \"doc\" : \"\", \"attributes_json\" : \"{\\\"nullable\\\":false,\\\"delta"
          + "\\\":true,\\\"pk\\\":false,\\\"type\\\":\\\"NUMBER\\\"}\"}]}";

  private static final String FULL_SCHEMA_WITH_TWO_DELTA_FIELDS =
      "{ \"type\" : \"record\",  \"name\" : \"etl\",\"namespace\" : \"reducerTest\",  \"fields\" : [ { \"name\" : "
          + "\"key\", \"type\" : {\"type\" : \"record\", \"name\" : \"key_name\", \"namespace\" : \"key_namespace\",  "
          + "\"fields\" : [ {\"name\" : \"partitionKey\", \"type\" : \"long\", \"doc\" : \"\"}, { \"name\" : \"environment"
          + "\", \"type\" : \"string\",\"doc\" : \"\"}, {\"name\" : \"subKey\",\"type\" : \"string\", \"doc\" : \"\"} ]}, "
          + "\"doc\" : \"\", \"attributes_json\" : \"{\\\"delta\\\":false,\\\"pk\\\":true}\" }"
          + ", {\"name\" : \"scn2\", \"type\": \"long\", \"doc\" : \"\", \"attributes_json\" : \"{\\\"nullable\\\":false,\\\"delta"
          + "\\\":true,\\\"pk\\\":false,\\\"type\\\":\\\"NUMBER\\\"}\"}"
          + " , {\"name\" : \"scn\", \"type\": \"long\", \"doc\" : \"\", \"attributes_json\" : \"{\\\"nullable\\\":false,\\\"delta"
          + "\\\":true,\\\"pk\\\":false,\\\"type\\\":\\\"NUMBER\\\"}\"}]}";

  @Test
  public void testReduce()
      throws IOException, InterruptedException {
    Schema keySchema = new Schema.Parser().parse(KEY_SCHEMA);
    GenericRecordBuilder keyRecordBuilder = new GenericRecordBuilder(keySchema.getField("key").schema());
    keyRecordBuilder.set("partitionKey", 1);
    keyRecordBuilder.set("environment", "test");
    keyRecordBuilder.set("subKey", "2");
    GenericRecord record = keyRecordBuilder.build();
    keyRecordBuilder = new GenericRecordBuilder(keySchema);
    keyRecordBuilder.set("key", record);
    GenericRecord keyRecord = keyRecordBuilder.build();

    // Test reducer with delta field "scn"
    Schema fullSchema = new Schema.Parser().parse(FULL_SCHEMA);
    AvroValue<GenericRecord> fullRecord1 = new AvroValue<>();
    AvroValue<GenericRecord> fullRecord2 = new AvroValue<>();
    AvroValue<GenericRecord> fullRecord3 = new AvroValue<>();
    AvroValue<GenericRecord> fullRecord4 = new AvroValue<>();

    GenericRecordBuilder fullRecordBuilder1 = new GenericRecordBuilder(fullSchema);
    fullRecordBuilder1.set("key", record);
    fullRecordBuilder1.set("scn", 123);
    fullRecordBuilder1.set("scn2", 100);
    fullRecord1.datum(fullRecordBuilder1.build());
    fullRecordBuilder1.set("scn", 125);
    fullRecordBuilder1.set("scn2", 1);
    fullRecord2.datum(fullRecordBuilder1.build());
    fullRecordBuilder1.set("scn", 124);
    fullRecordBuilder1.set("scn2", 10);
    fullRecord3.datum(fullRecordBuilder1.build());
    fullRecordBuilder1.set("scn", 122);
    fullRecordBuilder1.set("scn2", 1000);
    fullRecord4.datum(fullRecordBuilder1.build());

    Configuration conf = mock(Configuration.class);
    when(conf.get(AvroKeyDedupReducer.DELTA_SCHEMA_PROVIDER))
        .thenReturn(FieldAttributeBasedDeltaFieldsProvider.class.getName());
    when(conf.get(FieldAttributeBasedDeltaFieldsProvider.ATTRIBUTE_FIELD)).thenReturn("attributes_json");

    when(conf.get(FieldAttributeBasedDeltaFieldsProvider.DELTA_PROP_NAME,
        FieldAttributeBasedDeltaFieldsProvider.DEFAULT_DELTA_PROP_NAME))
        .thenReturn(FieldAttributeBasedDeltaFieldsProvider.DEFAULT_DELTA_PROP_NAME);
    AvroKeyDedupReducer reducer = new AvroKeyDedupReducer();

    WrappedReducer.Context reducerContext = mock(WrappedReducer.Context.class);
    when(reducerContext.getConfiguration()).thenReturn(conf);
    Counter moreThan1Counter = new GenericCounter();
    when(reducerContext.getCounter(AvroKeyDedupReducer.EVENT_COUNTER.MORE_THAN_1)).thenReturn(moreThan1Counter);

    Counter dedupedCounter = new GenericCounter();
    when(reducerContext.getCounter(AvroKeyDedupReducer.EVENT_COUNTER.DEDUPED)).thenReturn(dedupedCounter);

    Counter recordCounter = new GenericCounter();
    when(reducerContext.getCounter(AvroKeyDedupReducer.EVENT_COUNTER.RECORD_COUNT)).thenReturn(recordCounter);
    reducer.setup(reducerContext);

    doNothing().when(reducerContext).write(any(AvroKey.class), any(NullWritable.class));
    List<AvroValue<GenericRecord>> valueIterable =
        Lists.newArrayList(fullRecord1, fullRecord2, fullRecord3, fullRecord4);

    AvroKey<GenericRecord> key = new AvroKey<>();
    key.datum(keyRecord);
    reducer.reduce(key, valueIterable, reducerContext);
    Assert.assertEquals(reducer.getOutKey().datum(), fullRecord2.datum());

    // Test reducer without delta field
    Configuration conf2 = mock(Configuration.class);
    when(conf2.get(AvroKeyDedupReducer.DELTA_SCHEMA_PROVIDER)).thenReturn(null);
    when(reducerContext.getConfiguration()).thenReturn(conf2);
    AvroKeyDedupReducer reducer2 = new AvroKeyDedupReducer();
    reducer2.setup(reducerContext);
    reducer2.reduce(key, valueIterable, reducerContext);
    Assert.assertEquals(reducer2.getOutKey().datum(), fullRecord1.datum());

    // Test reducer with compound delta key.
    Schema fullSchema2 = new Schema.Parser().parse(FULL_SCHEMA_WITH_TWO_DELTA_FIELDS);
    GenericRecordBuilder fullRecordBuilder2 = new GenericRecordBuilder(fullSchema2);
    fullRecordBuilder2.set("key", record);
    fullRecordBuilder2.set("scn", 123);
    fullRecordBuilder2.set("scn2", 100);
    fullRecord1.datum(fullRecordBuilder2.build());
    fullRecordBuilder2.set("scn", 125);
    fullRecordBuilder2.set("scn2", 1000);
    fullRecord2.datum(fullRecordBuilder2.build());
    fullRecordBuilder2.set("scn", 126);
    fullRecordBuilder2.set("scn2", 1000);
    fullRecord3.datum(fullRecordBuilder2.build());
    fullRecordBuilder2.set("scn", 130);
    fullRecordBuilder2.set("scn2", 100);
    fullRecord4.datum(fullRecordBuilder2.build());
    List<AvroValue<GenericRecord>> valueIterable2 =
        Lists.newArrayList(fullRecord1, fullRecord2, fullRecord3, fullRecord4);
    reducer.reduce(key, valueIterable2, reducerContext);
    Assert.assertEquals(reducer.getOutKey().datum(), fullRecord3.datum());



  }
}
