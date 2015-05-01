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

package gobblin.metrics.kafka;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
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

  protected KafkaAvroReporter(Builder<?> builder) {
    super(builder);
  }

  /**
   * Returns a new {@link KafkaAvroReporter.Builder} for {@link KafkaAvroReporter}.
   * If the registry is of type {@link gobblin.metrics.MetricContext} tags will NOT be inherited.
   * To inherit tags, use forContext method.
   *
   * @param registry the registry to report
   * @return KafkaAvroReporter builder
   */
  public static Builder<?> forRegistry(MetricRegistry registry) {
    if(MetricContext.class.isInstance(registry)) {
      LOGGER.warn("Creating Kafka Avro Reporter from MetricContext using forRegistry method. Will not inherit tags.");
    }
    return new BuilderImpl(registry);
  }

  /**
   * Returns a new {@link KafkaAvroReporter.Builder} for {@link KafkaAvroReporter}.
   *
   * @param context the {@link gobblin.metrics.MetricContext} to report
   * @return KafkaAvroReporter builder
   */
  public static Builder<?> forContext(MetricContext context) {
    return new BuilderImpl(context).withTags(context.getTags());
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

    private Builder(MetricRegistry registry) {
      super(registry);
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

  /**
   * Parses a {@link gobblin.metrics.MetricReport} from a byte array.
   * @param reuse MetricReport to reuse.
   * @param bytes Input bytes.
   * @return MetricReport.
   * @throws IOException
   */
  public static MetricReport deserializeReport(MetricReport reuse, byte[] bytes) throws IOException {
    if (!READER.isPresent()) {
      READER = Optional.of(new SpecificDatumReader<MetricReport>(MetricReport.class));
    }

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(bytes));

    // Check version byte
    if (inputStream.readInt() != KafkaReporter.SCHEMA_VERSION) {
      throw new IOException("MetricReport schema version not recognized.");
    }
    // Decode the rest
    Decoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
    return READER.get().read(reuse, decoder);
  }

  @Override
  protected Encoder getEncoder(OutputStream out) {
    return EncoderFactory.get().binaryEncoder(out, null);
  }
}
