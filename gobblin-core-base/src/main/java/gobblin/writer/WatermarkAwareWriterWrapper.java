/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package gobblin.writer;

import java.io.IOException;
import java.util.Map;

import com.google.common.base.Optional;

import gobblin.source.extractor.CheckpointableWatermark;
import gobblin.stream.RecordEnvelope;


/**
 * A convenience wrapper class for WatermarkAware writers.
 */
public abstract class WatermarkAwareWriterWrapper<D> extends WriterWrapper<D> implements WatermarkAwareWriter<D> {
  private Optional<WatermarkAwareWriter> watermarkAwareWriter = Optional.absent();

  public final void setWatermarkAwareWriter(WatermarkAwareWriter watermarkAwareWriter) {
    this.watermarkAwareWriter = Optional.of(watermarkAwareWriter);
  }

  public final boolean isWatermarkCapable() {
    return watermarkAwareWriter.get().isWatermarkCapable();
  }

  public void writeEnvelope(final RecordEnvelope<D> recordEnvelope) throws IOException {
    watermarkAwareWriter.get().writeEnvelope(recordEnvelope);
  }

  public final Map<String, CheckpointableWatermark> getCommittableWatermark() {
    return watermarkAwareWriter.get().getCommittableWatermark();
  }

  public Map<String, CheckpointableWatermark> getUnacknowledgedWatermark() {
    return watermarkAwareWriter.get().getUnacknowledgedWatermark();
  }

}
