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
import org.apache.gobblin.metrics.reporter.util.SchemaVersionWriter;

import java.io.IOException;
import java.util.Properties;

import com.google.common.base.Optional;


/**
 * {@link org.apache.gobblin.metrics.reporter.EventReporter} that emits events to Kafka as serialized Avro records with a key.
 * Key for these kafka messages is obtained from values of properties provided via {@link #keyPropertyName}.
 * If the GobblinTrackingEvent does not contain any of the required property, key is set to null. In that case, this reporter
 * will act like a {@link org.apache.gobblin.metrics.kafka.KafkaAvroEventReporter}
 */
@Slf4j
public class KafkaAvroEventKeyValueReporter extends KafkaEventKeyValueReporter {

  protected KafkaAvroEventKeyValueReporter(BuilderImpl builder) throws IOException {
    super(builder);
  }

  @Override
  protected AvroSerializer<GobblinTrackingEvent> createSerializer(SchemaVersionWriter schemaVersionWriter)
      throws IOException {
    return new AvroBinarySerializer<>(GobblinTrackingEvent.SCHEMA$, schemaVersionWriter);
  }

  public static class BuilderImpl extends KafkaEventKeyValueReporter.BuilderImpl {
    private Optional<KafkaAvroSchemaRegistry> registry = Optional.absent();

    public BuilderImpl(MetricContext context, Properties properties) {
      super(context, properties);
    }

    @Override
    public KafkaEventReporter build(String brokers, String topic) throws IOException {
      this.brokers = brokers;
      this.topic = topic;
      return new KafkaAvroEventKeyValueReporter(this);
    }

    @Override
    protected KafkaAvroEventKeyValueReporter.BuilderImpl self() {
      return this;
    }

    public BuilderImpl withSchemaRegistry(KafkaAvroSchemaRegistry registry) {
      this.registry = Optional.of(registry);
      return self();
    }
  }
}
