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

package gobblin.converter;


import java.util.Collections;
import java.util.Random;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.WorkUnitState;
import gobblin.util.ConfigUtils;


/**
 * A converter that samples records based on a configured sampling ratio.
 */
@Slf4j
public class SamplingConverter extends Converter<Object, Object, Object, Object> {

  public static final String SAMPLE_RATIO_KEY="converter.sample.ratio";
  public static final double DEFAULT_SAMPLE_RATIO=0.01; // Sample 1% by default

  private final Random random = new Random();
  private double sampleRatio = DEFAULT_SAMPLE_RATIO;

  @Override
  public Converter<Object, Object, Object, Object> init(WorkUnitState workUnit) {
    super.init(workUnit);
    try {
      Config config = ConfigUtils.propertiesToConfig(workUnit.getProperties());
      double sampleRatio = ConfigUtils.getDouble(config, SAMPLE_RATIO_KEY, DEFAULT_SAMPLE_RATIO);
      Preconditions.checkState(sampleRatio >= 0 && sampleRatio <= 1.0,
          "Sample ratio must be between 0.0 and 1.0. Found " + sampleRatio);
      this.sampleRatio = sampleRatio;
    } catch (Exception e) {
      log.warn("Unable to retrieve config", e);
      log.warn("Defaulting to default sample ratio: {}", this.sampleRatio);
    }

    log.debug("Sample ratio configured: {}", this.sampleRatio);
    return this;
  }

  @Override
  public Object convertSchema(Object inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return inputSchema;
  }

  @Override
  public Iterable<Object> convertRecord(Object outputSchema, Object inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    if (random.nextDouble() <= this.sampleRatio) {
      return new SingleRecordIterable<>(inputRecord);
    } else {
      return Collections.EMPTY_LIST;
    }
  }
}