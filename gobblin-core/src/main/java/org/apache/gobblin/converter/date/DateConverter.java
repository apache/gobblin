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
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;
import org.apache.gobblin.util.EmptyIterable;

import java.lang.reflect.Method;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class DateConverter<SI, SO, DI, DO> extends Converter<SI, SO, DI, DO> {

    private String inputTimezone;
    private String inputTimeformat;
    private String outputTimezone;
    private String outputTimeformat;

    private String date;

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    @Override
    public Converter<SI, SO, DI, DO> init(WorkUnitState workUnit) {
        Calendar cal = Calendar.getInstance();
        TimeZone tz = cal.getTimeZone();

        Preconditions.checkArgument(workUnit.contains(ConfigurationKeys.CONVERTER_INPUT_TIMEZONE), "Cannot use "
                + this.getClass().getName() + " without specifying " + ConfigurationKeys.CONVERTER_INPUT_TIMEZONE);
        Preconditions.checkArgument(workUnit.contains(ConfigurationKeys.CONVERTER_INPUT_TIMEFORMAT), "Cannot use "
                + this.getClass().getName() + " without specifying " + ConfigurationKeys.CONVERTER_INPUT_TIMEFORMAT);

        this.inputTimezone = workUnit.getProp(ConfigurationKeys.CONVERTER_INPUT_TIMEZONE);
        this.inputTimeformat = workUnit.getProp(ConfigurationKeys.CONVERTER_INPUT_TIMEFORMAT);
        this.outputTimezone = workUnit.contains(ConfigurationKeys.CONVERTER_OUTPUT_TIMEZONE) ? workUnit.getProp(ConfigurationKeys.CONVERTER_OUTPUT_TIMEZONE) : tz.toZoneId().toString();
        this.outputTimeformat = workUnit.contains(ConfigurationKeys.CONVERTER_OUTPUT_TIMEFORMAT) ? workUnit.getProp(ConfigurationKeys.CONVERTER_OUTPUT_TIMEFORMAT) : "MM-dd-yyyy HH:mm:ss z";

        return this;
    }

    @Override
    public SO convertSchema(SI inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
        return (SO)inputSchema;
    }

    @Override
    public Iterable<DO> convertRecord(SO outputSchema, DI iR, WorkUnitState workUnit)
            throws DataConversionException {
        String inputRecord = "";
        try{
            Class inputRecordClass = iR.getClass();
            Method inputRecordClassGetDate = inputRecordClass.getMethod("getDate", null);
            inputRecord = (String)inputRecordClassGetDate.invoke(iR, null);
        }catch (Exception e){
            System.out.println(iR.getClass() + " must implement DateObject: " + DateObject.class.getCanonicalName());
        }

        // IF INPUT TIME FORMAT IS UNIX, HANDLE IT DIFFERENTLY
        if(this.inputTimeformat.equalsIgnoreCase("UNIX")) {
            long unixSeconds = Long.parseLong(inputRecord);
            Date date = new Date(unixSeconds * 1000L);
            SimpleDateFormat sdf = new SimpleDateFormat(this.outputTimeformat);
            sdf.setTimeZone(TimeZone.getTimeZone(this.outputTimezone));
            String formattedDate = sdf.format(date);
            setRecord(iR, formattedDate);
            return new SingleRecordIterable<>((DO)iR);
        }

        DateFormat inputTimeformat = new SimpleDateFormat(this.inputTimeformat);
        DateFormat outputTimeformat = new SimpleDateFormat(this.outputTimeformat);
        inputTimeformat.setTimeZone(TimeZone.getTimeZone(this.inputTimezone));
        try {
            Date date = inputTimeformat.parse(inputRecord);
            String formattedDate = outputTimeformat.format(date);
            setRecord(iR, formattedDate);
            return new SingleRecordIterable<>((DO)iR);
        }catch(Exception e){
            System.out.println(e);
        }
        return new EmptyIterable<DO>();
    }

    private void setRecord(DI inputRecord, String formattedDate){
        try{
            Class inputRecordClass = inputRecord.getClass();
            Class cArg = String.class;
            Method inputRecordClassSetDate = inputRecordClass.getMethod("setDate", cArg);
            inputRecordClassSetDate.invoke(inputRecord, formattedDate);
        }catch (Exception e) {
            System.out.println(e);
        }
    }
}
