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

import com.codahale.metrics.MetricRegistry;
import java.io.IOException;
import java.util.HashSet;
import kafka.consumer.ConsumerIterator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


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

  @BeforeClass
  public void setup() {
    schema = KafkaAvroReporter.Schema;

    reader = new GenericDatumReader<GenericRecord>(schema);
  }

  @Override
  protected KafkaReporter.Metric nextMetric(ConsumerIterator<byte[], byte[]> it)
      throws IOException {
    byte[] bytes = it.next().message();

    Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
    GenericRecord reuse = new GenericData.Record(schema);
    GenericRecord record = reader.read(reuse, decoder);

    KafkaReporter.Metric metric = new KafkaReporter.Metric();
    metric.name = record.get("name").toString();
    metric.value = record.get("value");
    if (record.get("hostname") != null) {
      metric.host = record.get("hostname").toString();
    }
    if (record.get("environment") != null) {
      metric.env = record.get("environment").toString();
    }
    metric.tags = new HashSet<String>();
    if (record.get("tags") != null) {
      for (Object tag : (GenericArray) record.get("tags")) {
        metric.tags.add(tag.toString());
      }
    }

    System.out.println(String.format("%s:%s", metric.name, metric.value));

    return metric;
  }
}
