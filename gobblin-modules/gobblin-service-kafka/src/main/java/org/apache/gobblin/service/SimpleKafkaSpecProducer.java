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

package org.apache.gobblin.service;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.Future;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;

import org.apache.gobblin.metrics.reporter.util.AvroBinarySerializer;
import org.apache.gobblin.metrics.reporter.util.AvroSerializer;
import org.apache.gobblin.metrics.reporter.util.FixedSchemaVersionWriter;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.runtime.job_spec.AvroJobSpec;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.writer.AsyncDataWriter;
import org.apache.gobblin.writer.WriteCallback;

import static org.apache.gobblin.service.SimpleKafkaSpecExecutor.VERB_KEY;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NotThreadSafe
public class SimpleKafkaSpecProducer implements SpecProducer<Spec>, Closeable  {
  private static final String KAFKA_DATA_WRITER_CLASS_KEY = "spec.kafka.dataWriterClass";
  private static final String DEFAULT_KAFKA_DATA_WRITER_CLASS =
      "org.apache.gobblin.kafka.writer.Kafka08DataWriter";

  // Producer
  protected AsyncDataWriter<byte[]> _kafkaProducer;
  private final AvroSerializer<AvroJobSpec> _serializer;
  private Config _config;
  private final String _kafkaProducerClassName;

  public SimpleKafkaSpecProducer(Config config, Optional<Logger> log) {
    _kafkaProducerClassName = ConfigUtils.getString(config, KAFKA_DATA_WRITER_CLASS_KEY,
        DEFAULT_KAFKA_DATA_WRITER_CLASS);

    try {
      _serializer = new AvroBinarySerializer<>(AvroJobSpec.SCHEMA$, new FixedSchemaVersionWriter());
      _config = config;
    } catch (IOException e) {
      throw new RuntimeException("Could not create AvroBinarySerializer", e);
    }
  }

  public SimpleKafkaSpecProducer(Config config, Logger log) {
    this(config, Optional.of(log));
  }

  /** Constructor with no logging */
  public SimpleKafkaSpecProducer(Config config) {
    this(config, Optional.<Logger>absent());
  }

  @Override
  public Future<?> addSpec(Spec addedSpec) {
    AvroJobSpec avroJobSpec = convertToAvroJobSpec(addedSpec, SpecExecutor.Verb.ADD);

    log.info("Adding Spec: " + addedSpec + " using Kafka.");

    return getKafkaProducer().write(_serializer.serializeRecord(avroJobSpec), WriteCallback.EMPTY);
  }

  @Override
  public Future<?> updateSpec(Spec updatedSpec) {
    AvroJobSpec avroJobSpec = convertToAvroJobSpec(updatedSpec, SpecExecutor.Verb.UPDATE);

    log.info("Updating Spec: " + updatedSpec + " using Kafka.");

    return getKafkaProducer().write(_serializer.serializeRecord(avroJobSpec), WriteCallback.EMPTY);
  }

  @Override
  public Future<?> deleteSpec(URI deletedSpecURI) {

    AvroJobSpec avroJobSpec = AvroJobSpec.newBuilder().setUri(deletedSpecURI.toString())
        .setMetadata(ImmutableMap.of(VERB_KEY, SpecExecutor.Verb.DELETE.name())).build();

    log.info("Deleting Spec: " + deletedSpecURI + " using Kafka.");

    return getKafkaProducer().write(_serializer.serializeRecord(avroJobSpec), WriteCallback.EMPTY);
  }

  @Override
  public Future<? extends List<Spec>> listSpecs() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    _kafkaProducer.close();
  }

  private AsyncDataWriter<byte[]> getKafkaProducer() {
    if (null == _kafkaProducer) {
      try {
        Class<?> kafkaProducerClass = (Class<?>) Class.forName(_kafkaProducerClassName);
        _kafkaProducer = (AsyncDataWriter<byte[]>) ConstructorUtils.invokeConstructor(kafkaProducerClass,
            ConfigUtils.configToProperties(_config));
      } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
        log.error("Failed to instantiate Kafka consumer from class " + _kafkaProducerClassName, e);

        throw new RuntimeException("Failed to instantiate Kafka consumer", e);
      }
    }
    return _kafkaProducer;
  }

  private AvroJobSpec convertToAvroJobSpec(Spec spec, SpecExecutor.Verb verb) {
    if (spec instanceof JobSpec) {
      JobSpec jobSpec = (JobSpec) spec;
      AvroJobSpec.Builder avroJobSpecBuilder = AvroJobSpec.newBuilder();

      avroJobSpecBuilder.setUri(jobSpec.getUri().toString()).setVersion(jobSpec.getVersion())
          .setDescription(jobSpec.getDescription()).setProperties(Maps.fromProperties(jobSpec.getConfigAsProperties()))
          .setMetadata(ImmutableMap.of(VERB_KEY, verb.name()));

      if (jobSpec.getTemplateURI().isPresent()) {
        avroJobSpecBuilder.setTemplateUri(jobSpec.getTemplateURI().get().toString());
      }

      return avroJobSpecBuilder.build();
    } else {
      throw new RuntimeException("Unsupported spec type " + spec.getClass());
    }
  }
}