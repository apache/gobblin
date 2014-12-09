/* (c) 2014 LinkedIn Corp. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.uif.source.extractor.watermark;

import java.util.HashMap;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Strings;

import com.linkedin.uif.source.extractor.extract.QueryBasedExtractor;
import com.linkedin.uif.source.extractor.watermark.Predicate.PredicateType;


public class WatermarkPredicate {
  private static final String DEFAULT_WATERMARK_VALUE_FORMAT = "yyyyMMddHHmmss";
  private static final long DEFAULT_WATERMARK_VALUE = -1;

  private String watermarkColumn;
  private WatermarkType watermarkType;
  private Watermark watermark;

  public WatermarkPredicate(WatermarkType watermarkType) {
    this(null, watermarkType);
  }

  public WatermarkPredicate(String watermarkColumn, WatermarkType watermarkType) {
    super();
    this.watermarkColumn = watermarkColumn;
    this.watermarkType = watermarkType;

    switch (watermarkType) {
      case TIMESTAMP:
        this.watermark = new TimestampWatermark(watermarkColumn, DEFAULT_WATERMARK_VALUE_FORMAT);
        break;
      case DATE:
        this.watermark = new DateWatermark(watermarkColumn, DEFAULT_WATERMARK_VALUE_FORMAT);
        break;
      case HOUR:
        this.watermark = new HourWatermark(watermarkColumn, DEFAULT_WATERMARK_VALUE_FORMAT);
        break;
      case SIMPLE:
        this.watermark = new SimpleWatermark(watermarkColumn, DEFAULT_WATERMARK_VALUE_FORMAT);
        break;
      default:
        this.watermark = new SimpleWatermark(watermarkColumn, DEFAULT_WATERMARK_VALUE_FORMAT);
        break;
    }
  }

  public Predicate getPredicate(QueryBasedExtractor extractor, long watermarkValue, String operator,
      PredicateType type) {
    String condition = "";

    if (watermarkValue != DEFAULT_WATERMARK_VALUE) {
      condition = this.watermark.getWatermarkCondition(extractor, watermarkValue, operator);
    }

    if (StringUtils.isBlank(watermarkColumn) || condition.equals("")) {
      return null;
    }
    return new Predicate(this.watermarkColumn, watermarkValue, condition, this.getWatermarkSourceFormat(extractor),
        type);
  }

  public String getWatermarkSourceFormat(QueryBasedExtractor extractor) {
    return extractor.getWatermarkSourceFormat(this.watermarkType);
  }

  public HashMap<Long, Long> getPartitions(long lowWatermarkValue, long highWatermarkValue, int partitionInterval,
      int maxIntervals) {
    return this.watermark.getIntervals(lowWatermarkValue, highWatermarkValue, partitionInterval, maxIntervals);
  }

  public int getDeltaNumForNextWatermark() {
    return this.watermark.getDeltaNumForNextWatermark();
  }
}
