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

package gobblin.watermark;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;

import gobblin.converter.IdentityConverter;
import gobblin.metadata.MetadataNames;
import gobblin.source.extractor.CheckpointableWatermark;
import gobblin.source.extractor.RecordEnvelope;
import gobblin.writer.watermarkTracker.WatermarkTracker;

import lombok.Getter;
import lombok.RequiredArgsConstructor;


/**
 * A {@link WatermarkTracker} that acts as a {@link gobblin.converter.Converter} in the pipeline. It assumes records seen
 * come in increasing order, add a {@link gobblin.source.extractor.Callback} to each record, and keeps track of acked
 * and unacked {@link gobblin.source.extractor.Callback}s.
 */
public class InOrderWatermarkTracker<S, D> extends IdentityConverter<S, D> implements WatermarkTracker {

  private ListMultimap<String, WatermarkAck> watermarkAcks =
      Multimaps.synchronizedListMultimap(LinkedListMultimap.create());
  private Map<String, WatermarkAck> lastAckedWatermarks = Maps.newConcurrentMap();

  @Override
  public void modifyOutputEnvelope(RecordEnvelope<D> recordEnvelope) {
    if (recordEnvelope.getMetadata().getRecordMetadata().containsKey(MetadataNames.WATERMARK)) {
      CheckpointableWatermark watermark = (CheckpointableWatermark) recordEnvelope.getMetadata().getRecordMetadata()
          .get(MetadataNames.WATERMARK);

      String source = MetadataNames.DEFAULT_SOURCE;
      if (recordEnvelope.getMetadata().getRecordMetadata().containsKey(MetadataNames.SOURCE)) {
        source = recordEnvelope.getMetadata().getRecordMetadata().get(MetadataNames.SOURCE).toString();
      }

      WatermarkAck watermarkAck = new WatermarkAck(source, watermark);
      recordEnvelope.addSuccessCallback(watermarkAck);
      this.watermarkAcks.put(source, watermarkAck);
    }
    super.modifyOutputEnvelope(recordEnvelope);
  }

  @Override
  public Map<String, CheckpointableWatermark> getAllCommitableWatermarks() {
    return Maps.transformValues(this.lastAckedWatermarks, WatermarkAck::getWatermark);
  }

  @Override
  public Map<String, CheckpointableWatermark> getAllUnacknowledgedWatermarks() {
    Map<String, CheckpointableWatermark> output = Maps.newHashMap();
    for (String source : this.watermarkAcks.keySet()) {
      WatermarkAck wmark = Optional.ofNullable((this.watermarkAcks.get(source)).get(0)).orElse(null);
      if (wmark != null) {
        output.put(source, wmark.getWatermark());
      }
    }
    return output;
  }

  private void purgeSource(String source) {
    Iterator<WatermarkAck> it = this.watermarkAcks.get(source).iterator();
    while (it.hasNext()) {
      WatermarkAck watermarkAck = it.next();
      if (watermarkAck.acked) {
        this.lastAckedWatermarks.put(source, watermarkAck);
        it.remove();
      } else {
        return;
      }
    }
  }

  @RequiredArgsConstructor
  @Getter
  private class WatermarkAck implements Runnable {
    private final String source;
    private final CheckpointableWatermark watermark;
    private volatile boolean acked = false;

    @Override
    public void run() {
      this.acked = true;
      InOrderWatermarkTracker.this.purgeSource(this.source);
    }
  }
}
