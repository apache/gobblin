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

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;
import org.apache.gobblin.util.EmptyIterable;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;


/**
 * An implementation of {@link org.apache.gobblin.converter.Converter} for the date conversion.
 *
 *<p>
 *   This converter converts date from one format / timezone to another format / timezone.
 *   If the input record type is JsonElement, then the converter fetches input date using
 *   converter.date.field specified in jobconfig file.
 *   TODO: Identify the schema for reading and writing date to the outputRecord.
 * </p>
 *
 * @author Siddhesh Sudesh Narvekar
 */

public class DateConverter<SI, SO, DI, DO> extends Converter<SI, SO, DI, DO> {

    private static final String CONVERTER_INPUT_TIMEZONE = "converter.input.timezone";
    private static final String CONVERTER_INPUT_TIMEFORMAT = "converter.input.timeformat";
    private static final String CONVERTER_OUTPUT_TIMEZONE = "converter.output.timezone";
    private static final String CONVERTER_OUTPUT_TIMEFORMAT = "converter.output.timeformat";
    private static final String CONVERTER_DATE_FIELD = "converter.date.field";

    private String inputTimezone;
    private String inputTimeformat;
    private String outputTimezone;
    private String outputTimeformat;

    @Override
    public Converter<SI, SO, DI, DO> init(WorkUnitState workUnit) {
        Calendar cal = Calendar.getInstance();
        TimeZone tz = cal.getTimeZone();

        Preconditions.checkArgument(workUnit.contains(CONVERTER_INPUT_TIMEZONE), "Cannot use "
                + this.getClass().getName() + " without specifying " + CONVERTER_INPUT_TIMEZONE);
        Preconditions.checkArgument(workUnit.contains(CONVERTER_INPUT_TIMEFORMAT), "Cannot use "
                + this.getClass().getName() + " without specifying " + CONVERTER_INPUT_TIMEFORMAT);

        this.inputTimezone = workUnit.getProp(CONVERTER_INPUT_TIMEZONE);
        this.inputTimeformat = workUnit.getProp(CONVERTER_INPUT_TIMEFORMAT);
        this.outputTimezone = workUnit.contains(CONVERTER_OUTPUT_TIMEZONE) ? workUnit.getProp(CONVERTER_OUTPUT_TIMEZONE) : tz.toZoneId().toString();
        this.outputTimeformat = workUnit.contains(CONVERTER_OUTPUT_TIMEFORMAT) ? workUnit.getProp(CONVERTER_OUTPUT_TIMEFORMAT) : "MM-dd-yyyy HH:mm:ss z";

        return this;
    }

    @Override
    public SO convertSchema(SI inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
        return (SO)inputSchema;
    }

    @Override
    public Iterable<DO> convertRecord(SO outputSchema, DI inputRecord, WorkUnitState workUnit)
            throws DataConversionException {
        DO outputRecord;
        String inputField = "";

        if(inputRecord instanceof JsonElement)
            inputField = ((JsonElement) inputRecord).getAsJsonObject().get(workUnit.getProp(CONVERTER_DATE_FIELD)).getAsString();
        else {
            //TODO: Identify the schema and read date from inputRecord
        }

        // IF INPUT TIME FORMAT IS UNIX, HANDLE IT DIFFERENTLY
        if(this.inputTimeformat.equalsIgnoreCase("UNIX")) {
            long unixSeconds = Long.parseLong(inputField);
            Date date = new Date(unixSeconds * 1000L);
            SimpleDateFormat sdf = new SimpleDateFormat(this.outputTimeformat);
            sdf.setTimeZone(TimeZone.getTimeZone(this.outputTimezone));
            String formattedDate = sdf.format(date);
            outputRecord = setRecord(inputRecord, formattedDate, workUnit.getProp(CONVERTER_DATE_FIELD));
            return new SingleRecordIterable<>(outputRecord);
        }

        SimpleDateFormat inputTimeformat = new SimpleDateFormat(this.inputTimeformat);
        SimpleDateFormat outputTimeformat = new SimpleDateFormat(this.outputTimeformat);
        inputTimeformat.setTimeZone(TimeZone.getTimeZone(this.inputTimezone));
        outputTimeformat.setTimeZone(TimeZone.getTimeZone(this.outputTimezone));
        try {
            Date date = inputTimeformat.parse(inputField);
            String formattedDate = outputTimeformat.format(date);
            outputRecord = setRecord(inputRecord, formattedDate, workUnit.getProp(CONVERTER_DATE_FIELD));
            return new SingleRecordIterable<>(outputRecord);
        }catch(Exception e){
            System.out.println(e);
        }
        return new EmptyIterable<DO>();
    }

    private DO setRecord(DI inputRecord, String formattedDate, String jsonField){
        DO outputRecord = null;
        if(inputRecord instanceof JsonElement){
            JsonObject iR = ((JsonElement) inputRecord).getAsJsonObject();
            iR.addProperty(jsonField, formattedDate);
            Gson gson = new Gson();
            outputRecord = (DO) gson.fromJson(iR.toString(), JsonElement.class);
        }
        else {
            //TODO: Identify the schema and write date to outputRecord
        }
        return outputRecord;
    }
}
