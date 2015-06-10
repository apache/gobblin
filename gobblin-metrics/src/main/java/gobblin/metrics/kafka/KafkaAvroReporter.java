/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metrics.kafka;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.DecoderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;

import gobblin.metrics.MetricContext;
import gobblin.metrics.MetricReport;


/**
 * Kafka reporter for codahale metrics writing metrics in Avro format.
 *
 * @author ibuenros
 */
public class KafkaAvroReporter extends KafkaReporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAvroReporter.class);

  private static Optional<SpecificDatumReader<MetricReport>> READER = Optional.absent();

  private final Optional<KafkaAvroSchemaRegistry> registry;
  private Optional<String> registrySchemaId;

  protected KafkaAvroReporter(Builder<?> builder) {
    super(builder);
    this.registry = builder.registry;
    this.registrySchemaId = Optional.absent();
  }

  /**
   * Returns a new {@link KafkaAvroReporter.Builder} for {@link KafkaAvroReporter}.
   * If the registry is of type {@link gobblin.metrics.MetricContext} tags will NOT be inherited.
   * To inherit tags, use forContext method.
   *
   * @param registry the registry to report
   * @return KafkaAvroReporter builder
   */
  public static Builder<? extends Builder<?>> forRegistry(MetricRegistry registry) {
    return new BuilderImpl(registry);
  }

  /**
   * Returns a new {@link KafkaAvroReporter.Builder} for {@link KafkaAvroReporter}.
   *
   * @param context the {@link gobblin.metrics.MetricContext} to report
   * @return KafkaAvroReporter builder
   */
  public static Builder<?> forContext(MetricContext context) {
    return forRegistry(context);
  }

  private static class BuilderImpl extends Builder<BuilderImpl> {
    public BuilderImpl(MetricRegistry registry) {
      super(registry);
    }

    @Override
    protected BuilderImpl self() {
      return this;
    }
  }

  /**
   * Builder for {@link KafkaAvroReporter}.
   * Defaults to no filter, reporting rates in seconds and times in milliseconds.
   */
  public static abstract class Builder<T extends Builder<T>> extends KafkaReporter.Builder<T> {

    private Optional<KafkaAvroSchemaRegistry> registry = Optional.absent();

    private Builder(MetricRegistry registry) {
      super(registry);
    }

    public T withSchemaRegistry(KafkaAvroSchemaRegistry registry) {
      this.registry = Optional.of(registry);
      return self();
    }

    /**
     * Builds and returns {@link KafkaAvroReporter}.
     *
     * @param brokers string of Kafka brokers
     * @param topic topic to send metrics to
     * @return KafkaAvroReporter
     */
    public KafkaAvroReporter build(String brokers, String topic) {
      this.brokers = brokers;
      this.topic = topic;
      return new KafkaAvroReporter(this);
    }

  }

  @Override
  protected Encoder getEncoder(OutputStream out) {
    return EncoderFactory.get().binaryEncoder(out, null);
  }

  /**
   * Implementation of {@link gobblin.metrics.SerializedMetricReportReporter#writeSchemaVersioningInformation} supporting
   * a {@link gobblin.metrics.kafka.KafkaAvroSchemaRegistry}.
   *
   * <p>
   * If a {@link gobblin.metrics.kafka.KafkaAvroSchemaRegistry} is provided to the reporter, this method writes
   * the {@link gobblin.metrics.kafka.KafkaAvroSchemaRegistry} id for the {@link gobblin.metrics.MetricReport} schema
   * instead of the static {@link gobblin.metrics.MetricReportUtils#SCHEMA_VERSION}. This allows processors like
   * Camus to retrieve the correct {@link org.apache.avro.Schema} from a REST schema registry. This method will also
   * automatically register the {@link org.apache.avro.Schema} to the REST registry. It is assumed that calling
   * {@link gobblin.metrics.kafka.KafkaAvroSchemaRegistry#register} more than once for the same
   * {@link org.apache.avro.Schema} is not a problem, as it will be called at least once per JVM.
   *
   * If no {@link gobblin.metrics.kafka.KafkaAvroSchemaRegistry} is provided, this method simply calls the super method.
   * </p>
   *
   * @param outputStream Empty {@link java.io.DataOutputStream} that will hold the serialized
   *                     {@link gobblin.metrics.MetricReport}. Any data written by this method
   *                     will appear at the beginning of the emitted message.
   * @throws IOException
   */
  @Override
  protected void writeSchemaVersioningInformation(DataOutputStream outputStream)
      throws IOException {

    if(this.registry.isPresent()) {
      if(!this.registrySchemaId.isPresent()) {
        this.registrySchemaId = Optional.of(this.registry.get().register(MetricReport.SCHEMA$));
      }
      outputStream.writeByte(KafkaAvroSchemaRegistry.MAGIC_BYTE);
      try {
        outputStream.write(Hex.decodeHex(this.registrySchemaId.get().toCharArray()));
      } catch(DecoderException exception) {
        throw new IOException(exception);
      }
    } else {
      super.writeSchemaVersioningInformation(outputStream);
    }

  }
}
