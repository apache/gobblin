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

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.testng.Assert;
import org.testng.annotations.Test;

import avro.shaded.com.google.common.collect.Lists;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Test class for {@link FieldAttributeBasedDeltaFieldsProvider}.
 */
@Test(groups = { "gobblin.compaction" })
public class FieldAttributeBasedDeltaFieldsProviderTest {
  private static final String FULL_SCHEMA_WITH_ATTRIBUTES =
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
  public void testGetDeltaFieldNamesForNewSchema(){
    Configuration conf = mock(Configuration.class);
    when(conf.get(FieldAttributeBasedDeltaFieldsProvider.ATTRIBUTE_FIELD)).thenReturn("attributes_json");
    when(conf.get(FieldAttributeBasedDeltaFieldsProvider.DELTA_PROP_NAME,
        FieldAttributeBasedDeltaFieldsProvider.DEFAULT_DELTA_PROP_NAME))
        .thenReturn(FieldAttributeBasedDeltaFieldsProvider.DEFAULT_DELTA_PROP_NAME);

    AvroDeltaFieldNameProvider provider = new FieldAttributeBasedDeltaFieldsProvider(conf);
    Schema original = new Schema.Parser().parse(FULL_SCHEMA_WITH_ATTRIBUTES);
    GenericRecord record = mock(GenericRecord.class);
    when(record.getSchema()).thenReturn(original);
    List<String> fields = provider.getDeltaFieldNames(record);
    Assert.assertEquals(fields, Lists.newArrayList("scn2", "scn"));
  }
}
