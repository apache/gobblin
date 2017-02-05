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

package gobblin.source.extractor;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

import lombok.EqualsAndHashCode;

import gobblin.util.io.GsonInterfaceAdapter;


/**
 * A {@link CheckpointableWatermark} that wraps a {@link ComparableWatermark} specific to a source.
 */
@EqualsAndHashCode
public class DefaultCheckpointableWatermark implements CheckpointableWatermark {

  private static final Gson GSON = GsonInterfaceAdapter.getGson(Object.class);

  private final String source;
  private final ComparableWatermark comparable;

  public DefaultCheckpointableWatermark(String source, ComparableWatermark comparableWatermark) {
    this.source = source;
    this.comparable = comparableWatermark;
  }

  public String getSource() {
    return this.source;
  }

  @Override
  public ComparableWatermark getWatermark() {
    return this.comparable;
  }

  @Override
  public int compareTo(CheckpointableWatermark o) {
    if (!(this.source.equals(o.getSource()))) {
      throw new RuntimeException("Could not compare two checkpointable watermarks because they have different sources "
          + this.source + ":" + o.getSource());
    }
    return this.comparable.compareTo(o.getWatermark());
  }

  @Override
  public JsonElement toJson() {
    return GSON.toJsonTree(this);
  }

  @Override
  public short calculatePercentCompletion(Watermark lowWatermark, Watermark highWatermark) {
    return comparable.calculatePercentCompletion(lowWatermark, highWatermark);
  }

  @Override
  public String toString() {
    return String.format("%s : %s ", getSource(), GSON.toJson(this.comparable.toJson()));
  }
}
