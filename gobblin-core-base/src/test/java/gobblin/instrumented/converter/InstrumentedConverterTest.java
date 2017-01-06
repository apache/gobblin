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

package gobblin.instrumented.converter;

import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.MetricsHelper;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.Constructs;
import gobblin.converter.DataConversionException;
import gobblin.converter.IdentityConverter;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;
import gobblin.metrics.MetricNames;


public class InstrumentedConverterTest {

  public class TestInstrumentedConverter extends InstrumentedConverter<String, String, String, String> {

    @Override
    public Iterable<String> convertRecordImpl(String outputSchema, String inputRecord, WorkUnitState workUnit)
        throws DataConversionException {
      return new SingleRecordIterable<>(inputRecord);
    }

    @Override
    public String convertSchema(String inputSchema, WorkUnitState workUnit)
        throws SchemaConversionException {
      return null;
    }
  }

  @Test
  public void testInstrumented() throws DataConversionException{
    TestInstrumentedConverter converter = new TestInstrumentedConverter();
    testBase(converter);
  }

  @Test
  public void testDecorator() throws DataConversionException{
    InstrumentedConverterBase instrumentedConverter = new InstrumentedConverterDecorator(
        new TestInstrumentedConverter()
    );
    testBase(instrumentedConverter);

    InstrumentedConverterBase nonInstrumentedConverter = new InstrumentedConverterDecorator(
        new IdentityConverter()
    );
    testBase(nonInstrumentedConverter);
  }

  public void testBase(InstrumentedConverterBase<String, String, String, String> converter)
      throws DataConversionException {

    WorkUnitState state = new WorkUnitState();
    state.setProp(ConfigurationKeys.METRICS_ENABLED_KEY, Boolean.toString(true));
    converter.init(state);

    Iterable<String> iterable = converter.convertRecord("schema", "record", new WorkUnitState());

    Map<String, Long> metrics = MetricsHelper.dumpMetrics(converter.getMetricContext());
    Assert.assertEquals(metrics.get(MetricNames.ConverterMetrics.RECORDS_IN_METER), Long.valueOf(1));
    Assert.assertEquals(metrics.get(MetricNames.ConverterMetrics.RECORDS_OUT_METER), Long.valueOf(0));
    Assert.assertEquals(metrics.get(MetricNames.ConverterMetrics.CONVERT_TIMER), Long.valueOf(1));

    iterable.iterator().next();
    metrics = MetricsHelper.dumpMetrics(converter.getMetricContext());
    Assert.assertEquals(metrics.get(MetricNames.ConverterMetrics.RECORDS_IN_METER), Long.valueOf(1));
    Assert.assertEquals(metrics.get(MetricNames.ConverterMetrics.RECORDS_OUT_METER), Long.valueOf(1));

    Assert.assertEquals(MetricsHelper.dumpTags(converter.getMetricContext()).get("construct"),
        Constructs.CONVERTER.toString());

  }


}
