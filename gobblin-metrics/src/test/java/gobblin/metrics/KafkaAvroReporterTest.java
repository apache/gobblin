/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */


package gobblin.metrics;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;

import kafka.consumer.ConsumerIterator;


/**
 * Test for KafkaAvroReporter
 * Extends KafkaReporterTest and just redefines the builder and the metrics deserializer
 *
 * @author ibuenros
 */
@Test(groups = {"gobblin.metrics"})
public class KafkaAvroReporterTest extends KafkaReporterTest {

  private Schema schema;
  private GenericDatumReader<GenericRecord> reader;

  public KafkaAvroReporterTest(String topic)
      throws IOException, InterruptedException {
    super(topic);

    schema = null;
    reader = null;

  }

  public KafkaAvroReporterTest() throws IOException, InterruptedException {
    this("KafkaAvroReporterTest");
  }

  @Override
  public KafkaReporter.Builder<?> getBuilder(MetricRegistry registry) {
    return KafkaAvroReporter.forRegistry(registry);
  }

  @Override
  public KafkaReporter.Builder<?> getBuilderFromContext(MetricContext context) {
    return KafkaAvroReporter.forContext(context);
  }

  @BeforeClass
  public void setup() {
    schema = KafkaAvroReporter.SCHEMA;

    reader = new GenericDatumReader<GenericRecord>(schema);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected KafkaReporter.Metric nextMetric(ConsumerIterator<byte[], byte[]> it)
      throws IOException {
    byte[] bytes = it.next().message();

    Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
    GenericRecord reuse = new GenericData.Record(schema);
    GenericRecord record = reader.read(reuse, decoder);

    KafkaReporter.Metric metric = new KafkaReporter.Metric();
    metric.name = record.get("name").toString();
    metric.value = record.get("value");
    metric.tags = new HashMap<String, String>();
    if (record.get("tags") != null) {
      for (Map.Entry<Utf8, Utf8> tag : ((HashMap<Utf8, Utf8>)record.get("tags")).entrySet()) {
        metric.tags.put(tag.getKey().toString(), tag.getValue().toString());
      }
    }

    System.out.println(String.format("%s:%s", metric.name, metric.value));

    return metric;
  }
}
