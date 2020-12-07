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

import java.math.BigInteger;

import org.apache.gobblin.source.extractor.ComparableWatermark;
import org.apache.gobblin.source.extractor.Watermark;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;


/**
 * BigInteger based {@link ComparableWatermark} implementation.
 */
@AllArgsConstructor
@EqualsAndHashCode
public class BigIntegerWatermark implements ComparableWatermark {

  private static final Gson GSON = new Gson();

  @Getter
  @Setter
  private BigInteger value;

  public void increment() {
    this.value = this.value.add(BigInteger.ONE);
  }

  @Override
  public JsonElement toJson() {
    return GSON.toJsonTree(this);
  }

  @Override
  public short calculatePercentCompletion(Watermark lowWatermark, Watermark highWatermark) {
    Preconditions.checkArgument(lowWatermark instanceof BigIntegerWatermark);
    Preconditions.checkArgument(highWatermark instanceof BigIntegerWatermark);
    BigInteger total = ((BigIntegerWatermark) highWatermark).value.subtract(((BigIntegerWatermark) lowWatermark).value);
    BigInteger pulled = this.value.subtract(((BigIntegerWatermark) lowWatermark).value);
    Preconditions.checkState(total.compareTo(BigInteger.ZERO) >= 0);
    Preconditions.checkState(pulled.compareTo(BigInteger.ZERO) >= 0);
    if (total.compareTo(BigInteger.ZERO) == 0) {
      return 0;
    }
    long percent = (pulled.multiply(BigInteger.valueOf(100))).divide(total).longValue();
    return (short) percent;
  }

  @Override
  public int compareTo(ComparableWatermark other) {
    Preconditions.checkArgument(other instanceof BigIntegerWatermark);
    return this.value.compareTo(((BigIntegerWatermark) other).value);
  }
}
