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

package org.apache.gobblin.source.extractor.extract;

import java.math.RoundingMode;

import com.google.common.base.Preconditions;
import com.google.common.math.LongMath;
import com.google.common.primitives.Longs;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

import org.apache.gobblin.source.extractor.ComparableWatermark;
import org.apache.gobblin.source.extractor.Watermark;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString
@EqualsAndHashCode
public class LongWatermark implements ComparableWatermark {

  private static final Gson GSON = new Gson();

  @Getter
  @Setter
  private long value;

  public LongWatermark(long value) {
    this.value = value;
  }

  public void increment() {
    this.value++;
  }

  @Override
  public JsonElement toJson() {
    return GSON.toJsonTree(this);
  }

  @Override
  public short calculatePercentCompletion(Watermark lowWatermark, Watermark highWatermark) {
    Preconditions.checkArgument(lowWatermark instanceof LongWatermark);
    Preconditions.checkArgument(highWatermark instanceof LongWatermark);
    long total = ((LongWatermark) highWatermark).value - ((LongWatermark) lowWatermark).value;
    long pulled = this.value - ((LongWatermark) lowWatermark).value;
    Preconditions.checkState(total >= 0);
    Preconditions.checkState(pulled >= 0);
    if (total == 0) {
      return 0;
    }
    long percent = Math.min(100, LongMath.divide(pulled * 100, total, RoundingMode.HALF_UP));
    return (short) percent;
  }

  @Override
  public int compareTo(ComparableWatermark other) {
    Preconditions.checkArgument(other instanceof LongWatermark);
    return Longs.compare(this.value, ((LongWatermark)other).value);
  }

}
