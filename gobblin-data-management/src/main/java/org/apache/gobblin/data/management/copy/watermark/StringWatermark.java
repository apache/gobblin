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

package gobblin.data.management.copy.watermark;

import com.google.common.base.Preconditions;
import com.google.gson.JsonElement;

import gobblin.source.extractor.ComparableWatermark;
import gobblin.source.extractor.Watermark;
import gobblin.source.extractor.WatermarkSerializerHelper;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;


/**
 * String based {@link ComparableWatermark} implementation.
 */
@AllArgsConstructor
@EqualsAndHashCode
public class StringWatermark implements ComparableWatermark {

  @Getter
  String value;

  @Override
  public int compareTo(ComparableWatermark other) {
    Preconditions.checkArgument(other instanceof StringWatermark);
    return this.value.compareTo(((StringWatermark) other).getValue());
  }

  @Override
  public JsonElement toJson() {
    return WatermarkSerializerHelper.convertWatermarkToJson(this);
  }

  @Override
  public short calculatePercentCompletion(Watermark lowWatermark, Watermark highWatermark) {
    return 0;
  }
}
