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
package gobblin.data.management.convertion.hive;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

import gobblin.source.extractor.Watermark;


/**
 * A {@link Watermark} that uses a single long value as watermark.
 */
@Getter
@AllArgsConstructor
@EqualsAndHashCode
public class LongWatermark implements Watermark, Comparable<LongWatermark> {

  private static final Gson GSON = new Gson();

  private long value;

  @Override
  public JsonElement toJson() {
    return GSON.toJsonTree(this);
  }

  @Override
  public short calculatePercentCompletion(Watermark lowWatermark, Watermark highWatermark) {
    return 0;
  }

  @Override
  public int compareTo(LongWatermark o) {
    return Long.compare(this.value, o.getValue());
  }
}
