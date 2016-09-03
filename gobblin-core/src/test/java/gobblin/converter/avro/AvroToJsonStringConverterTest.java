package gobblin.converter.avro;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import gobblin.configuration.WorkUnitState;
import gobblin.metrics.Metric;
import gobblin.metrics.MetricReport;
import gobblin.metrics.reporter.util.AvroBinarySerializer;
import gobblin.metrics.reporter.util.AvroSerializer;
import gobblin.metrics.reporter.util.NoopSchemaVersionWriter;
import gobblin.util.AvroUtils;


public class AvroToJsonStringConverterTest {

  @Test
  public void test() throws Exception {

    MetricReport metricReport = new MetricReport();
    metricReport.setTags(ImmutableMap.of("tag", "value"));
    metricReport.setTimestamp(10L);
    metricReport.setMetrics(Lists.newArrayList(new Metric("metric", 1.0), new Metric("metric2", 2.0)));

    AvroSerializer<MetricReport> serializer =
        new AvroBinarySerializer<>(MetricReport.SCHEMA$, new NoopSchemaVersionWriter());
    serializer.serializeRecord(metricReport);

    Schema metricReportUtf8 = new Schema.Parser().parse(this.getClass().getClassLoader().getResourceAsStream("MetricReport.avsc"));
    GenericRecord genericRecordMetric = AvroUtils.slowDeserializeGenericRecord(serializer.serializeRecord(metricReport), metricReportUtf8);

    AvroToJsonStringConverter converter = new AvroToJsonStringConverter();
    String outputSchema = converter.convertSchema(metricReportUtf8, new WorkUnitState());

    Iterable<String> converted = converter.convertRecord(outputSchema, genericRecordMetric, new WorkUnitState());
    List<String> convertedList = Lists.newArrayList(converted);

    Assert.assertEquals(convertedList.size(), 1);
    Assert.assertEquals(convertedList.get(0), "{\"tags\":{\"tag\":\"value\"},\"timestamp\":10,\"metrics\":[{\"name\":\"metric\",\"value\":1.0},{\"name\":\"metric2\",\"value\":2.0}]}");

  }

}
