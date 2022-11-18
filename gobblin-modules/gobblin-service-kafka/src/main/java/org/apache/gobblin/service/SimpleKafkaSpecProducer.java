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

import com.google.common.base.Joiner;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.Future;
import java.util.Properties;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.slf4j.Logger;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.ServiceMetricNames;
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

  private Meter addSpecMeter;
  private Meter deleteSpecMeter;
  private Meter updateSpecMeter;
  private Meter cancelSpecMeter;
  private MetricContext metricContext = Instrumented.getMetricContext(new State(), getClass());

  public SimpleKafkaSpecProducer(Config config, Optional<Logger> log) {
    _kafkaProducerClassName = ConfigUtils.getString(config, KAFKA_DATA_WRITER_CLASS_KEY,
        DEFAULT_KAFKA_DATA_WRITER_CLASS);
    this.addSpecMeter = createMeter("-Add");
    this.deleteSpecMeter = createMeter("-Delete");
    this.updateSpecMeter = createMeter("-Update");
    this.cancelSpecMeter = createMeter("-Cancel");

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

  private Meter createMeter(String suffix) {
    return this.metricContext.meter(MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX, getClass().getSimpleName(), suffix));
  }

  private Spec addExecutionIdToJobSpecUri(Spec spec) {
    JobSpec newSpec = (JobSpec)spec;
    if (newSpec.getConfig().hasPath(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)) {
      try {
        newSpec.setUri(new URI(Joiner.on("/").
            join(spec.getUri().toString(), newSpec.getConfig().getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY))));
      } catch (URISyntaxException e) {
        log.error("Cannot create job uri to cancel job", e);
      }
    }
    return newSpec;
  }

  private URI getURIWithExecutionId(URI originalURI, Properties props) {
    URI result = originalURI;
    if (props.containsKey(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)) {
      try {
        result = new URI(Joiner.on("/").
            join(originalURI.toString(), props.getProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)));
      } catch (URISyntaxException e) {
        log.error("Cannot create job uri to cancel job", e);
      }
    }
    return result;
  }

  @Override
  public Future<?> addSpec(Spec addedSpec) {
    Spec spec = addExecutionIdToJobSpecUri(addedSpec);
    AvroJobSpec avroJobSpec = convertToAvroJobSpec(spec, SpecExecutor.Verb.ADD);

    log.info("Adding Spec: " + spec + " using Kafka.");
    this.addSpecMeter.mark();

    return getKafkaProducer().write(_serializer.serializeRecord(avroJobSpec), new KafkaWriteCallback(avroJobSpec));
  }

  @Override
  public Future<?> updateSpec(Spec updatedSpec) {
    Spec spec = addExecutionIdToJobSpecUri(updatedSpec);
    AvroJobSpec avroJobSpec = convertToAvroJobSpec(spec, SpecExecutor.Verb.UPDATE);

    log.info("Updating Spec: " + spec + " using Kafka.");
    this.updateSpecMeter.mark();

    return getKafkaProducer().write(_serializer.serializeRecord(avroJobSpec), new KafkaWriteCallback(avroJobSpec));
  }

  @Override
  public Future<?> deleteSpec(URI deletedSpecURI, Properties headers) {
    URI finalDeletedSpecURI = getURIWithExecutionId(deletedSpecURI, headers);

    AvroJobSpec avroJobSpec = AvroJobSpec.newBuilder().setUri(finalDeletedSpecURI.toString())
        .setMetadata(ImmutableMap.of(SpecExecutor.VERB_KEY, SpecExecutor.Verb.DELETE.name()))
        .setProperties(Maps.fromProperties(headers)).build();

    log.info("Deleting Spec: " + finalDeletedSpecURI + " using Kafka.");
    this.deleteSpecMeter.mark();

    return getKafkaProducer().write(_serializer.serializeRecord(avroJobSpec), new KafkaWriteCallback(avroJobSpec));
  }

  @Override
  public Future<?> cancelJob(URI deletedSpecURI, Properties properties) {
    URI finalDeletedSpecURI = getURIWithExecutionId(deletedSpecURI, properties);
    AvroJobSpec avroJobSpec = AvroJobSpec.newBuilder().setUri(finalDeletedSpecURI.toString())
        .setMetadata(ImmutableMap.of(SpecExecutor.VERB_KEY, SpecExecutor.Verb.CANCEL.name()))
        .setProperties(Maps.fromProperties(properties)).build();

    log.info("Cancelling job: " + finalDeletedSpecURI + " using Kafka.");
    this.cancelSpecMeter.mark();

    return getKafkaProducer().write(_serializer.serializeRecord(avroJobSpec), new KafkaWriteCallback(avroJobSpec));
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
          .setMetadata(ImmutableMap.of(SpecExecutor.VERB_KEY, verb.name()));

      if (jobSpec.getTemplateURI().isPresent()) {
        avroJobSpecBuilder.setTemplateUri(jobSpec.getTemplateURI().get().toString());
      }

      return avroJobSpecBuilder.build();
    } else {
      throw new RuntimeException("Unsupported spec type " + spec.getClass());
    }
  }

  static class KafkaWriteCallback implements WriteCallback {
    AvroJobSpec avroJobSpec;

    KafkaWriteCallback(AvroJobSpec avroJobSpec) {
      this.avroJobSpec = avroJobSpec;
    }

    @Override
    public void onSuccess(Object result) {

    }

    @Override
    public void onFailure(Throwable throwable) {
      log.error("Error while writing the following record to Kafka {}", avroJobSpec.toString(), throwable);
    }
  }
}