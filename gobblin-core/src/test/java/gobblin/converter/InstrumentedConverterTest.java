/*
 * (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.converter;

import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import gobblin.MetricsHelper;
import gobblin.configuration.WorkUnitState;


public class InstrumentedConverterTest {

  public class TestInstrumentedConverter extends InstrumentedConverter<String, String, String, String> {

    @Override
    public Iterable<String> convertRecordImpl(String outputSchema, String inputRecord, WorkUnitState workUnit)
        throws DataConversionException {
      return new SingleRecordIterable<String>(inputRecord);
    }

    @Override
    public String convertSchema(String inputSchema, WorkUnitState workUnit)
        throws SchemaConversionException {
      return null;
    }
  }

  @Test
  public void test() throws DataConversionException {
    TestInstrumentedConverter converter = new TestInstrumentedConverter();
    converter.init(new WorkUnitState());

    Iterable<String> iterable = converter.convertRecord("schema", "record", new WorkUnitState());

    Map<String, Long> metrics = MetricsHelper.dumpMetrics(converter.instrumented);
    Assert.assertEquals(metrics.get("gobblin.converter.records.in"), Long.valueOf(1));
    Assert.assertEquals(metrics.get("gobblin.converter.records.out"), Long.valueOf(0));
    Assert.assertEquals(metrics.get("gobblin.converter.conversion.time"), Long.valueOf(1));

    iterable.iterator().next();
    metrics = MetricsHelper.dumpMetrics(converter.instrumented);
    Assert.assertEquals(metrics.get("gobblin.converter.records.in"), Long.valueOf(1));
    Assert.assertEquals(metrics.get("gobblin.converter.records.out"), Long.valueOf(1));

    Assert.assertEquals(MetricsHelper.dumpTags(converter.instrumented).get("component"), "converter");

  }

}
