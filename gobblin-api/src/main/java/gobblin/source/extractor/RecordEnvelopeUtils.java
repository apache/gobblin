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

package gobblin.source.extractor;

import java.util.Optional;

import gobblin.metadata.MetadataNames;


/**
 * Utilities for {@link RecordEnvelope}s.
 */
public class RecordEnvelopeUtils {

  /**
   * Get the {@link CheckpointableWatermark} in this record if present.
   */
  public static Optional<CheckpointableWatermark> getCheckpointableWatermark(RecordEnvelope<?> recordEnvelope) {
    try {
      return Optional.ofNullable((CheckpointableWatermark) recordEnvelope.getMetadata().getRecordMetadata().get(MetadataNames.WATERMARK));
    } catch (ClassCastException cce) {
      return Optional.empty();
    }

  }

  /**
   * Create a {@link RecordEnvelope} with a given {@link CheckpointableWatermark} written at key
   * {@link MetadataNames#WATERMARK} of the record metadata.
   */
  public static <D> RecordEnvelope<D> createRecordEnvelopeWithWatermark(D record, CheckpointableWatermark wm) {
    RecordEnvelope<D> recordEnvelope = new RecordEnvelope<>(record);
    recordEnvelope.getMetadata().getRecordMetadata().put(MetadataNames.WATERMARK, wm);
    return recordEnvelope;
  }

}
