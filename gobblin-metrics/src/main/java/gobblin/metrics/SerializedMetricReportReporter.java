/*
 * (c) 2014 LinkedIn Corp. All rights reserved.
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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.Nullable;

import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;


/**
 * Metric reporter that pushes serialized {@link gobblin.metrics.MetricReport}.
 *
 * <p>
 * The serialization format is defined by {@link #getEncoder}. By default, json encoder is used,
 * but subclasses can override this method to use a different encoder.
 * </p>
 *
 * Concrete subclasses should implement {@link #pushSerializedReport}.
 */
public abstract class SerializedMetricReportReporter extends MetricReportReporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(SerializedMetricReportReporter.class);

  private final Encoder encoder;
  private final ByteArrayOutputStream byteArrayOutputStream;
  private final DataOutputStream out;
  protected final boolean reportMetrics;

  private final Optional<SpecificDatumWriter<MetricReport>> writerOpt;

  public SerializedMetricReportReporter(Builder<?> builder) {
    super(builder);

    this.byteArrayOutputStream = new ByteArrayOutputStream();
    this.out = this.closer.register(new DataOutputStream(byteArrayOutputStream));
    this.encoder = getEncoder(out);
    this.reportMetrics = this.encoder != null;

    if (this.reportMetrics) {
      this.writerOpt = Optional.of(new SpecificDatumWriter<MetricReport>(MetricReport.class));
    } else {
      this.writerOpt = Optional.absent();
    }

  }

  public static abstract class Builder<T extends Builder<T>> extends MetricReportReporter.Builder<T> {
    public Builder(MetricRegistry registry) {
      super(registry);
    }
  }

  @Override
  protected void emitReport(MetricReport report) {
    pushSerializedReport(serializeReport(report));
  }

  /**
   * Push serialized metric report to metrics sink.
   * @param serializedReport bytes to send.
   */
  protected abstract void pushSerializedReport(@Nullable byte[] serializedReport);

  /**
   * Get {@link org.apache.avro.io.Encoder} for serializing Avro records.
   * @param out {@link java.io.OutputStream} where records should be written.
   * @return Encoder.
   */
  protected Encoder getEncoder(OutputStream out) {
    try {
      return EncoderFactory.get().jsonEncoder(MetricReport.SCHEMA$, out);
    } catch(IOException exception) {
      LOGGER.warn("KafkaReporter serializer failed to initialize. Will not report Metrics to Kafka.", exception);
      return null;
    }
  }

  /**
   * Converts a {@link gobblin.metrics.MetricReport} to bytes to send through Kafka.
   * @param report MetricReport to serialize.
   * @return Serialized bytes.
   */
  protected synchronized byte[] serializeReport(MetricReport report) {
    if (!this.writerOpt.isPresent()) {
      return null;
    }

    try {
      this.byteArrayOutputStream.reset();
      // Write version number at the beginning of the message.
      this.out.writeInt(MetricReportUtils.SCHEMA_VERSION);
      // Now write the report itself.
      this.writerOpt.get().write(report, this.encoder);
      this.encoder.flush();
      return this.byteArrayOutputStream.toByteArray();
    } catch(IOException exception) {
      LOGGER.warn("Could not serialize Avro record for Kafka Metrics.", exception);
      return null;
    }
  }

}
