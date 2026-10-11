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

package org.apache.gobblin.converter.date;

import com.google.gson.*;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit test for {@link DateConverter}
 * @author Siddhesh Sudesh Narvekar
 *
 */
@Test(groups = {"gobblin.converter"})
public class DateConverterTest <SI, SO>{

    private static final String CONVERTER_INPUT_TIMEZONE = "converter.input.timezone";
    private static final String CONVERTER_INPUT_TIMEFORMAT = "converter.input.timeformat";
    private static final String CONVERTER_OUTPUT_TIMEZONE = "converter.output.timezone";
    private static final String CONVERTER_OUTPUT_TIMEFORMAT = "converter.output.timeformat";
    private static final String CONVERTER_DATE_FIELD = "converter.date.field";

    private JsonElement jsonElement;
    private WorkUnitState state;
    private static final String DATE = "1993-06-12 11:16:00";

    /**
     * @throws SchemaConversionException
     * @throws DataConversionException
     */

    private void initResources(){
        WorkUnit workUnit = new WorkUnit(new SourceState(),
                new Extract(new SourceState(), Extract.TableType.SNAPSHOT_ONLY, "namespace", "dummy_table"));
        state = new WorkUnitState(workUnit);
        String inputTimezone = "IST";
        String inputTimeformat = "yyyy-MM-dd HH:mm:ss";
        String outputTimezone = "PST";
        String outputTimeformat = "MM-dd-yyyy HH:mm:ss";
        state.setProp(CONVERTER_INPUT_TIMEZONE, inputTimezone);
        state.setProp(CONVERTER_INPUT_TIMEFORMAT, inputTimeformat);
        state.setProp(CONVERTER_OUTPUT_TIMEZONE, outputTimezone);
        state.setProp(CONVERTER_OUTPUT_TIMEFORMAT, outputTimeformat);
        state.setProp(CONVERTER_DATE_FIELD, "date");
    }

    private void initJsonElement(String date){
        JsonObject jsonObject = new JsonObject();
        Gson gson = new Gson();
        jsonObject.addProperty("date", date);
        this.jsonElement = gson.fromJson(jsonObject.toString(), JsonElement.class);
    }

    @Test
    public void testConverter()
            throws Exception{

        initResources();
        initJsonElement(DATE);

        /**
         * Test 1 for JsonElement
         */
        DateConverter<SI, SO, JsonElement, JsonElement> jsonConverter = new DateConverter();
        jsonConverter.init(this.state);
        JsonElement record = jsonConverter.convertRecord(null, this.jsonElement, state).iterator().next();
        JsonObject outputRecord = record.getAsJsonObject();
        Assert.assertEquals(outputRecord.get("date").getAsString(), "06-11-1993 22:46:00");

        /**
         * Test 2 for Unix UTC to MM-dd-yyyy HH:mm:ss UTC
         */
        this.state.setProp(CONVERTER_INPUT_TIMEFORMAT, "UNIX");
        this.state.setProp(CONVERTER_INPUT_TIMEZONE, "UTC");
        this.state.setProp(CONVERTER_OUTPUT_TIMEZONE, "UTC");
        initJsonElement("739883760");
        jsonConverter.init(this.state);
        record = jsonConverter.convertRecord(null, this.jsonElement, state).iterator().next();
        outputRecord = record.getAsJsonObject();
        Assert.assertEquals(outputRecord.get("date").getAsString(), "06-12-1993 11:16:00");

        /**
         * TODO: Tests for any object type
         */
    }
}


