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

package org.apache.gobblin.metrics.reporter.util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.google.common.io.Closer;

import javax.annotation.Nullable;

import org.apache.gobblin.metrics.MetricReport;
import org.apache.gobblin.metrics.MetricReporterException;
import org.apache.gobblin.metrics.MultiReporterException;
import org.apache.gobblin.metrics.ReporterType;


/**
 * Utilities for {@link org.apache.gobblin.metrics.MetricReport}.
 */
public class MetricReportUtils {

  public static final int SCHEMA_VERSION = 1;
  private static Optional<SpecificDatumReader<MetricReport>> READER = Optional.absent();
  /**
   * Parses a {@link org.apache.gobblin.metrics.MetricReport} from a byte array representing a json input.
   * @param reuse MetricReport to reuse.
   * @param bytes Input bytes.
   * @return MetricReport.
   * @throws java.io.IOException
   */
  public synchronized static MetricReport deserializeReportFromJson(MetricReport reuse, byte[] bytes)
      throws IOException {
    if (!READER.isPresent()) {
      READER = Optional.of(new SpecificDatumReader<>(MetricReport.class));
    }

    Closer closer = Closer.create();

    try {
      DataInputStream inputStream = closer.register(new DataInputStream(new ByteArrayInputStream(bytes)));

      // Check version byte
      int versionNumber = inputStream.readInt();
      if (versionNumber != SCHEMA_VERSION) {
        throw new IOException(String
            .format("MetricReport schema version not recognized. Found version %d, expected %d.", versionNumber,
                SCHEMA_VERSION));
      }

      // Decode the rest
      Decoder decoder = DecoderFactory.get().jsonDecoder(MetricReport.SCHEMA$, inputStream);
      return READER.get().read(reuse, decoder);
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  /**
   * Parses a {@link org.apache.gobblin.metrics.MetricReport} from a byte array Avro serialization.
   * @param reuse MetricReport to reuse.
   * @param bytes Input bytes.
   * @return MetricReport.
   * @throws java.io.IOException
   */
  public synchronized static MetricReport deserializeReportFromAvroSerialization(MetricReport reuse, byte[] bytes)
      throws IOException {
    return deserializeReportFromAvroSerialization(reuse, bytes, null);
  }

  /**
   * Parses a {@link org.apache.gobblin.metrics.MetricReport} from a byte array Avro serialization.
   * @param reuse MetricReport to reuse.
   * @param bytes Input bytes.
   * @param schemaId Expected schemaId.
   * @return MetricReport.
   * @throws java.io.IOException
   */
  public synchronized static MetricReport deserializeReportFromAvroSerialization(MetricReport reuse, byte[] bytes,
      @Nullable String schemaId)
      throws IOException {
    if (!READER.isPresent()) {
      READER = Optional.of(new SpecificDatumReader<>(MetricReport.class));
    }

    Closer closer = Closer.create();

    try {
      DataInputStream inputStream = closer.register(new DataInputStream(new ByteArrayInputStream(bytes)));
      if (schemaId != null) {
        readAndVerifySchemaId(inputStream, schemaId);
      } else {
        readAndVerifySchemaVersion(inputStream);
      }
      // Decode the rest
      Decoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
      return READER.get().read(reuse, decoder);
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  public static void readAndVerifySchemaId(DataInputStream inputStream, String schemaId)
      throws IOException {
    //Read the magic byte
    inputStream.readByte();
    int schemaIdLengthBytes = schemaId.length() / 2;
    byte[] readId = new byte[schemaIdLengthBytes];
    int numBytesRead = inputStream.read(readId, 0, schemaIdLengthBytes);
    String readSchemaId = Hex.encodeHexString(readId);
    if (numBytesRead != schemaIdLengthBytes || !schemaId.equals(readSchemaId)) {
      throw new IOException(String
          .format("Schema version not recognized. Found version %s, expected %s.", readSchemaId,
              schemaId));
    }
  }

  public static void readAndVerifySchemaVersion(DataInputStream inputStream)
      throws IOException {
    // Check version byte
    int versionNumber = inputStream.readInt();
    if (versionNumber != SCHEMA_VERSION) {
      throw new IOException(String
          .format("Schema version not recognized. Found version %d, expected %d.", versionNumber,
              SCHEMA_VERSION));
    }
  }

  public static boolean shouldThrowException(Logger log, MultiReporterException ex, boolean isMetricReportingFailureFatal, boolean isEventReportingFailureFatal) {
    boolean shouldThrow = false;
    for (MetricReporterException e: ex.getExceptions()) {
      if ((isMetricReportingFailureFatal && ReporterType.isReporterTypeMetric(e.getReporterType())) || (
          isEventReportingFailureFatal && ReporterType.isReporterTypeEvent(e.getReporterType()))) {
        shouldThrow = true;
      }
      log.error("Failed to start {} {} reporter", e.getSinkType().name(), e.getReporterType().name(), e);
    }
    return shouldThrow;
  }
}
