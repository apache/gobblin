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

package org.apache.gobblin.converter.filter;

import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.SchemaConversionException;


/**
 * Test for {@link GobblinTrackingEventFlattenFilterConverter}.
 */
public class GobblinTrackingEventFlattenFilterConverterTest {
  @Test
  public void testSchemaConversion()
      throws SchemaConversionException, IOException {
    GobblinTrackingEventFlattenFilterConverter converter = new GobblinTrackingEventFlattenFilterConverter();
    Properties props = new Properties();
    props.put(GobblinTrackingEventFlattenFilterConverter.class.getSimpleName() + "."
        + GobblinTrackingEventFlattenFilterConverter.FIELDS_TO_FLATTEN, "field1,field2");
    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.addAll(props);
    converter.init(workUnitState);
    Schema output = converter.convertSchema(
        new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream("GobblinTrackingEvent.avsc")),
        workUnitState);
    Assert.assertEquals(output, new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"GobblinTrackingEvent\",\"namespace\":\"org.apache.gobblin.metrics\",\"fields\":"
            + "[{\"name\":\"timestamp\",\"type\":\"long\",\"doc\":\"Time at which event was created.\",\"default\":0},"
            + "{\"name\":\"namespace\",\"type\":[\"string\",\"null\"],\"doc\":\"Namespace used for filtering of events.\"},"
            + "{\"name\":\"name\",\"type\":\"string\",\"doc\":\"Event name.\"},{\"name\":\"field1\",\"type\":\"string\",\"doc\":\"\"},"
            + "{\"name\":\"field2\",\"type\":\"string\",\"doc\":\"\"}]}"));

    props.put(GobblinTrackingEventFlattenFilterConverter.class.getSimpleName() + "."
        + GobblinTrackingEventFlattenFilterConverter.FIELDS_RENAME_MAP, "name:eventName,field1:field3");

    WorkUnitState workUnitState2 = new WorkUnitState();
    workUnitState2.addAll(props);
    converter.init(workUnitState2);
    Schema output2 = converter.convertSchema(
        new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream("GobblinTrackingEvent.avsc")),
        workUnitState2);
    Assert.assertEquals(output2, new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"GobblinTrackingEvent\",\"namespace\":\"org.apache.gobblin.metrics\",\"fields\":"
            + "[{\"name\":\"timestamp\",\"type\":\"long\",\"doc\":\"Time at which event was created.\",\"default\":0},"
            + "{\"name\":\"namespace\",\"type\":[\"string\",\"null\"],\"doc\":\"Namespace used for filtering of events.\"},"
            + "{\"name\":\"eventName\",\"type\":\"string\",\"doc\":\"Event name.\"},{\"name\":\"field3\",\"type\":\"string\",\"doc\":\"\"},"
            + "{\"name\":\"field2\",\"type\":\"string\",\"doc\":\"\"}]}"));
  }
}
