/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor.extract.kafka;

import java.math.RoundingMode;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.math.LongMath;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

import gobblin.source.extractor.Watermark;


/**
 * A {@link gobblin.source.extractor.Watermark} that holds multiple long values.
 *
 * @author Ziyang Liu
 */
public class MultiLongWatermark implements Watermark {

  private static final Gson GSON = new Gson();

  private final List<Long> values;

  /**
   * Copy constructor.
   */
  public MultiLongWatermark(MultiLongWatermark other) {
    this.values = Lists.newArrayList(other.values);
  }

  public MultiLongWatermark(List<Long> values) {
    this.values = Lists.newArrayList(values);
  }

  /**
   * Increment the idx'th value. idx must be between 0 and size()-1.
   */
  public void increment(int idx) {
    Preconditions.checkElementIndex(idx, this.values.size());
    Preconditions.checkArgument(this.values.get(idx) < Long.MAX_VALUE);
    this.values.set(idx, this.values.get(idx) + 1);
  }

  /**
   * Serializes the MultiLongWatermark into a JsonElement.
   */
  @Override
  public JsonElement toJson() {
    return GSON.toJsonTree(this);
  }

  /**
   * Given a low watermark (starting point) and a high watermark (target), returns the percentage
   * of events pulled.
   *
   * @return a percentage value between 0 and 100.
   */
  @Override
  public short calculatePercentCompletion(Watermark lowWatermark, Watermark highWatermark) {
    Preconditions.checkArgument(
        lowWatermark instanceof MultiLongWatermark && highWatermark instanceof MultiLongWatermark,
        String.format("Arguments of %s.%s must be of type %s", MultiLongWatermark.class.getSimpleName(),
            Thread.currentThread().getStackTrace()[1].getMethodName(), MultiLongWatermark.class.getSimpleName()));

    long pulled = ((MultiLongWatermark) lowWatermark).getGap(this);
    long all = ((MultiLongWatermark) lowWatermark).getGap((MultiLongWatermark) highWatermark);
    Preconditions.checkState(all > 0);
    long percent = Math.min(100, LongMath.divide(pulled * 100, all, RoundingMode.HALF_UP));
    return (short) percent;
  }

  /**
   * Get the number of records that need to be pulled given the high watermark.
   */
  public long getGap(MultiLongWatermark highWatermark) {
    Preconditions.checkNotNull(highWatermark);
    Preconditions.checkArgument(this.values.size() == highWatermark.values.size());
    long diff = 0;
    for (int i = 0; i < this.values.size(); i++) {
      Preconditions.checkArgument(this.values.get(i) <= highWatermark.values.get(i));
      diff += highWatermark.values.get(i) - this.values.get(i);
    }
    return diff;
  }

  /**
   * @return the number of long values this watermark holds.
   */
  public int size() {
    return this.values.size();
  }

  /**
   * The idx'th value of this watermark. idx must be between 0 and size()-1.
   * @return
   */
  public long get(int idx) {
    Preconditions.checkElementIndex(idx, this.values.size());
    return this.values.get(idx);
  }

  /**
   * Set the idx'th value of this watermark. idx must be between 0 and size()-1.
   */
  public long set(int idx, long value) {
    Preconditions.checkElementIndex(idx, this.values.size());
    return this.values.set(idx, value);
  }

  @Override
  public String toString() {
    return this.values.toString();
  }
}
