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

package org.apache.gobblin.runtime.job_monitor;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collection;
import java.util.List;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.reporter.util.SchemaVersionWriter;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.MutableJobCatalog;
import org.apache.gobblin.runtime.metrics.RuntimeMetrics;
import org.apache.gobblin.util.Either;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * A job monitor for Avro messages. Uses a fixed input schema to parse the messages, then calls
 * {@link #parseJobSpec(T)} for each one.
 * @param <T> the subclass of {@link org.apache.avro.specific.SpecificRecord} that messages implement.
 */
@Slf4j
public abstract class KafkaAvroJobMonitor<T> extends KafkaJobMonitor {

  private final Schema schema;
  private final ThreadLocal<BinaryDecoder> decoder;
  private final ThreadLocal<SpecificDatumReader<T>> reader;
  @Getter
  private final SchemaVersionWriter<?> versionWriter;

  @Getter
  private Meter messageParseFailures;

  public KafkaAvroJobMonitor(String topic, MutableJobCatalog catalog, Config config, Schema schema,
      SchemaVersionWriter<?> versionWriter) {
    super(topic, catalog, config);

    this.schema = schema;
    this.decoder = new ThreadLocal<BinaryDecoder>() {
      @Override
      protected BinaryDecoder initialValue() {
        InputStream dummyInputStream = new ByteArrayInputStream(new byte[0]);
        return DecoderFactory.get().binaryDecoder(dummyInputStream, null);
      }
    };
    this.reader = new ThreadLocal<SpecificDatumReader<T>>() {
      @Override
      protected SpecificDatumReader<T> initialValue() {
        return new SpecificDatumReader<>(KafkaAvroJobMonitor.this.schema);
      }
    };
    this.versionWriter = versionWriter;
  }

  @Override
  protected List<Tag<?>> getTagsForMetrics() {
    List<Tag<?>> tags = super.getTagsForMetrics();
    tags.add(new Tag<>(RuntimeMetrics.SCHEMA, this.schema.getName()));
    return tags;
  }

  @Override
  protected void createMetrics() {
    super.createMetrics();
    this.messageParseFailures = this.getMetricContext().meter(
        RuntimeMetrics.GOBBLIN_JOB_MONITOR_KAFKA_MESSAGE_PARSE_FAILURES);
  }

  @Override
  public Collection<Either<JobSpec, URI>> parseJobSpec(byte[] message)
      throws IOException {

    InputStream is = new ByteArrayInputStream(message);
    this.versionWriter.readSchemaVersioningInformation(new DataInputStream(is));

    Decoder decoder = DecoderFactory.get().binaryDecoder(is, this.decoder.get());
    try {
      T decodedMessage = this.reader.get().read(null, decoder);
      return parseJobSpec(decodedMessage);
    } catch (AvroRuntimeException | IOException exc) {
      this.messageParseFailures.mark();
      if (this.messageParseFailures.getFiveMinuteRate() < 1) {
        log.warn("Unable to decode input message.", exc);
      } else {
        log.warn("Unable to decode input message.");
      }
      return Lists.newArrayList();
    }
  }

  /**
   * Extract {@link JobSpec}s from the Kafka message.
   */
  public abstract Collection<Either<JobSpec, URI>> parseJobSpec(T message);
}
