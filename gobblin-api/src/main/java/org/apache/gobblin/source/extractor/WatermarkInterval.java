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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;


/**
 * Each {@link gobblin.source.workunit.WorkUnit} has a corresponding {@link WatermarkInterval}. The
 * {@link WatermarkInterval} represents the range of the data that needs to be pulled for the {@link WorkUnit}. So, the
 * {@link gobblin.source.workunit.WorkUnit} should pull data from the {@link #lowWatermark} to the
 * {@link #expectedHighWatermark}.
 */
public class WatermarkInterval {

  public static final String LOW_WATERMARK_TO_JSON_KEY = "low.watermark.to.json";
  public static final String EXPECTED_HIGH_WATERMARK_TO_JSON_KEY = "expected.watermark.to.json";

  private final Watermark lowWatermark;
  private final Watermark expectedHighWatermark;

  public WatermarkInterval(Watermark lowWatermark, Watermark expectedHighWatermark) {
    this.lowWatermark = lowWatermark;
    this.expectedHighWatermark = expectedHighWatermark;
  }

  public Watermark getLowWatermark() {
    return this.lowWatermark;
  }

  public Watermark getExpectedHighWatermark() {
    return this.expectedHighWatermark;
  }

  public JsonElement toJson() {
    JsonObject jsonObject = new JsonObject();

    jsonObject.add(LOW_WATERMARK_TO_JSON_KEY, this.lowWatermark.toJson());
    jsonObject.add(EXPECTED_HIGH_WATERMARK_TO_JSON_KEY, this.expectedHighWatermark.toJson());

    return jsonObject;
  }
}
