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

package org.apache.gobblin.runtime.mapreduce;

import org.apache.hadoop.mapreduce.Mapper;


/**
 * A customized progresser that reports static value, dummy implementation of {@link CustomizedProgresser}
 * while still useful to prevent direct reporting of 1.0f to MR framework.
 *
 * Customized application implementation should extends this class instead of implementing {@link CustomizedProgresser}
 * directly as the interface could be changed if we are attempting to add Reducer's progress as well.
 */
public class CustomizedProgresserBase implements CustomizedProgresser {

  private static final String STATIC_PROGRESS = "customizedProgress.staticProgressValue";
  private static final float DEFAULT_STATIC_PROGRESS = 0.5f;

  private float staticProgress;

  public static class BaseFactory implements CustomizedProgresser.Factory {
    @Override
    public CustomizedProgresser createCustomizedProgresser(Mapper.Context mapperContext) {
      return new CustomizedProgresserBase(mapperContext);
    }
  }

  public CustomizedProgresserBase(Mapper.Context mapperContext) {
    this.staticProgress = mapperContext.getConfiguration().getFloat(STATIC_PROGRESS, DEFAULT_STATIC_PROGRESS);
  }

  @Override
  public float getCustomizedProgress() {
    return staticProgress;
  }
}
