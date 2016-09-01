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
