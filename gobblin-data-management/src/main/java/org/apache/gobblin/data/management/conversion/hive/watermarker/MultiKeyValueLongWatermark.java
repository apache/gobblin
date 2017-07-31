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
package org.apache.gobblin.data.management.conversion.hive.watermarker;

import java.math.RoundingMode;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import lombok.AccessLevel;
import lombok.Getter;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.math.LongMath;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

import org.apache.gobblin.source.extractor.Watermark;


/**
 * A {@link Watermark} that holds multiple key value watermarks. Neither the key nor the value can be null
 * It is backed by a {@link ConcurrentHashMap}.
 */
public class MultiKeyValueLongWatermark implements Watermark {

  private static final Gson GSON = new Gson();

  @Getter(AccessLevel.PACKAGE)
  private final Map<String, Long> watermarks;

  public MultiKeyValueLongWatermark() {
    this.watermarks = Maps.newConcurrentMap();
  }

  public MultiKeyValueLongWatermark(Map<String, Long> watermarks) {
    this.watermarks = watermarks == null ? Maps.<String, Long> newConcurrentMap() : new ConcurrentHashMap<>(watermarks);
  }

  @Override
  public JsonElement toJson() {
    return GSON.toJsonTree(this);
  }

  @Override
  public short calculatePercentCompletion(Watermark lowWatermark, Watermark highWatermark) {
    Preconditions.checkArgument(
        lowWatermark instanceof MultiKeyValueLongWatermark && highWatermark instanceof MultiKeyValueLongWatermark,
        String.format("lowWatermark and highWatermark are not instances of %s",
            MultiKeyValueLongWatermark.class.getSimpleName()));
    MultiKeyValueLongWatermark low = (MultiKeyValueLongWatermark) lowWatermark;
    MultiKeyValueLongWatermark high = (MultiKeyValueLongWatermark) highWatermark;

    long total = 0;
    long pulled = 0;
    for (Map.Entry<String, Long> entry : low.watermarks.entrySet()) {
      if (high.watermarks.containsKey(entry.getKey())) {
        total += (high.watermarks.get(entry.getKey()) - entry.getValue());
      }
    }

    for (Map.Entry<String, Long> entry : low.watermarks.entrySet()) {
      if (this.watermarks.containsKey(entry.getKey())) {
        pulled += (this.watermarks.get(entry.getKey()) - entry.getValue());
      }
    }

    if (pulled > total) {
      return 100;
    }

    return (short) LongMath.divide(pulled * 100, total, RoundingMode.CEILING);
  }

}
