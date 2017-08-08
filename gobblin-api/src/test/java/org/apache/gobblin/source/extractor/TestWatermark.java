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

package org.apache.gobblin.source.extractor;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

/**
 * Implementation of {@link Watermark} used for testing purposes in {@link TestWatermark}.
 */
public class TestWatermark implements Watermark {

  private static final Gson GSON = new Gson();

  private long watermark = -1;

  @Override
  public JsonElement toJson() {
    return WatermarkSerializerHelper.convertWatermarkToJson(this);
  }

  @Override
  public short calculatePercentCompletion(Watermark lowWatermark, Watermark highWatermark) {
    TestWatermark testLowWatermark = GSON.fromJson(lowWatermark.toJson(), TestWatermark.class);
    TestWatermark testHighWatermark = GSON.fromJson(highWatermark.toJson(), TestWatermark.class);
    return (short) (100 * (this.watermark - testLowWatermark.getLongWatermark()) / (testHighWatermark
        .getLongWatermark() - testLowWatermark.getLongWatermark()));
  }

  public void setLongWatermark(long watermark) {
    this.watermark = watermark;
  }

  public long getLongWatermark() {
    return this.watermark;
  }
}
