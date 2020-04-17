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

import com.google.common.base.Optional;
import com.google.common.io.Closer;

import javax.annotation.Nullable;

import org.apache.gobblin.metrics.GobblinTrackingEvent;


/**
 * Utilities for {@link org.apache.gobblin.metrics.MetricReport}.
 */
public class EventUtils {

  public static final int SCHEMA_VERSION = 1;
  private static Optional<SpecificDatumReader<GobblinTrackingEvent>> reader = Optional.absent();

  /**
   * Parses a {@link org.apache.gobblin.metrics.MetricReport} from a byte array representing a json input.
   * @param reuse MetricReport to reuse.
   * @param bytes Input bytes.
   * @return MetricReport.
   * @throws java.io.IOException
   */
  public synchronized static GobblinTrackingEvent deserializeReportFromJson(GobblinTrackingEvent reuse, byte[] bytes) throws IOException {
    if (!reader.isPresent()) {
      reader = Optional.of(new SpecificDatumReader<>(GobblinTrackingEvent.class));
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
      Decoder decoder = DecoderFactory.get().jsonDecoder(GobblinTrackingEvent.SCHEMA$, inputStream);
      return reader.get().read(reuse, decoder);
    } catch(Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  /**
   * Parses a {@link org.apache.gobblin.metrics.GobblinTrackingEvent} from a byte array Avro serialization.
   * @param reuse GobblinTrackingEvent to reuse.
   * @param bytes Input bytes.
   * @return GobblinTrackingEvent.
   * @throws java.io.IOException
   */
  public synchronized static GobblinTrackingEvent deserializeEventFromAvroSerialization(GobblinTrackingEvent reuse, byte[] bytes)
      throws IOException {
    return deserializeEventFromAvroSerialization(reuse, bytes, null);
  }

  /**
   * Parses a {@link org.apache.gobblin.metrics.GobblinTrackingEvent} from a byte array Avro serialization.
   * @param reuse GobblinTrackingEvent to reuse.
   * @param bytes Input bytes.
   * @param schemaId Expected schemaId.
   * @return GobblinTrackingEvent.
   * @throws java.io.IOException
   */
  public synchronized static GobblinTrackingEvent deserializeEventFromAvroSerialization(GobblinTrackingEvent reuse, byte[] bytes, @Nullable String schemaId)
      throws IOException {
    if (!reader.isPresent()) {
      reader = Optional.of(new SpecificDatumReader<>(GobblinTrackingEvent.class));
    }

    Closer closer = Closer.create();

    try {
      DataInputStream inputStream = closer.register(new DataInputStream(new ByteArrayInputStream(bytes)));

      if (schemaId != null) {
        MetricReportUtils.readAndVerifySchemaId(inputStream, schemaId);
      } else {
        MetricReportUtils.readAndVerifySchemaVersion(inputStream);
      }

      // Decode the rest
      Decoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
      return reader.get().read(reuse, decoder);
    } catch(Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }
}
