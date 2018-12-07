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

package org.apache.gobblin.metrics.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.reporter.util.AvroBinarySerializer;
import org.apache.gobblin.metrics.reporter.util.AvroSerializer;
import org.apache.gobblin.metrics.reporter.util.SchemaRegistryVersionWriter;
import org.apache.gobblin.metrics.reporter.util.SchemaVersionWriter;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;


/**
 * Implement of {@link KafkaEventKeyValueReporter} for avro records.
 */
@Slf4j
public class KafkaAvroEventKeyValueReporter extends KafkaEventKeyValueReporter {

  protected KafkaAvroEventKeyValueReporter(Builder<?> builder) throws IOException {
    super(builder);
    if(builder.registry.isPresent()) {
      Schema schema =
          new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream("GobblinTrackingEvent.avsc"));
      this.serializer.setSchemaVersionWriter(new SchemaRegistryVersionWriter(builder.registry.get(), builder.topic,
          Optional.of(schema)));
    }
  }

  @Override
  protected AvroSerializer<GobblinTrackingEvent> createSerializer(SchemaVersionWriter schemaVersionWriter)
      throws IOException {
    return new AvroBinarySerializer<>(GobblinTrackingEvent.SCHEMA$, schemaVersionWriter);
  }

  private static class BuilderImpl extends Builder<BuilderImpl> {
    private BuilderImpl(MetricContext context) {
      super(context);
    }

    @Override
    protected BuilderImpl self() {
      return this;
    }
  }

  public static abstract class Factory {
    /**
     * Returns a new {@link Builder} for {@link KafkaAvroEventKeyValueReporter}.
     *
     * @param context the {@link MetricContext} to report
     * @return KafkaAvroReporter builder
     */
    public static BuilderImpl forContext(MetricContext context) {
      return new BuilderImpl(context);
    }
  }

  /**
   * Builder for {@link KafkaAvroEventKeyValueReporter}.
   * Defaults to no filter, reporting rates in seconds and times in milliseconds.
   */
  public static abstract class Builder<T extends Builder<T>> extends KafkaEventKeyValueReporter.Builder<T> {
    private Optional<KafkaAvroSchemaRegistry> registry = Optional.absent();

    private Builder(MetricContext context) {
      super(context);
    }

    public T withSchemaRegistry(KafkaAvroSchemaRegistry registry) {
      this.registry = Optional.of(registry);
      return self();
    }

    /**
     * Builds and returns {@link KafkaAvroEventReporter}.
     *
     * @param brokers string of Kafka brokers
     * @param topic topic to send metrics to
     * @return KafkaAvroReporter
     */
    public KafkaAvroEventKeyValueReporter build(String brokers, String topic) throws IOException {
      this.brokers = brokers;
      this.topic = topic;
      return new KafkaAvroEventKeyValueReporter(this);
    }
  }
}
