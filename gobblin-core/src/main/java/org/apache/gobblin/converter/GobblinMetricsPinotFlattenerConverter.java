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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import gobblin.configuration.WorkUnitState;

import lombok.extern.slf4j.Slf4j;


/**
 * Flatten {@link gobblin.metrics.MetricReport} for use by Pinot.
 * The output schema can be found at "gobblin-metrics/src/main/avro/FlatGobblinMetric.avsc".
 */
@Slf4j
public class GobblinMetricsPinotFlattenerConverter extends Converter<Schema, Schema, GenericRecord, GenericRecord> {

  private final Schema schema;

  public GobblinMetricsPinotFlattenerConverter() throws IOException {
    try (InputStream is = GobblinMetricsPinotFlattenerConverter.class.getClassLoader().getResourceAsStream("FlatGobblinMetric.avsc")) {
      this.schema = new Schema.Parser().parse(is);
    }
  }

  @Override
  public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return this.schema;
  }

  @Override
  public Iterable<GenericRecord> convertRecord(Schema outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    GenericRecordBuilder baseBuilder = new GenericRecordBuilder(this.schema);
    Map<Utf8, Utf8> tags = (Map<Utf8, Utf8>) inputRecord.get("tags");
    List<String> tagList = Lists.newArrayList(Iterables.transform(tags.entrySet(), new Function<Map.Entry<Utf8, Utf8>, String>() {
      @Override
      public String apply(Map.Entry<Utf8, Utf8> input) {
        return input.getKey().toString() + ":" + input.getValue().toString();
      }
    }));
    baseBuilder.set("tags", tagList);
    baseBuilder.set("timestamp", inputRecord.get("timestamp"));

    List<GenericRecord> metrics = (List<GenericRecord>)inputRecord.get("metrics");

    List<GenericRecord> flatMetrics = Lists.newArrayList();

    for (GenericRecord metric : metrics) {
      GenericRecordBuilder thisMetric = new GenericRecordBuilder(baseBuilder);
      thisMetric.set("metricName", metric.get("name"));
      thisMetric.set("metricValue", metric.get("value"));
      flatMetrics.add(thisMetric.build());
    }

    return flatMetrics;
  }
}
