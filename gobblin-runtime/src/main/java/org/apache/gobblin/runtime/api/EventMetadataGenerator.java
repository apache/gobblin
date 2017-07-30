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

package gobblin.runtime.api;

import java.util.Map;

import gobblin.metrics.event.EventName;
import gobblin.runtime.JobContext;

/**
 * For generating additional event metadata to associate with an event.
 */
public interface EventMetadataGenerator {
  /**
   * Generate a map of additional metadata for the specified event name.
   * @param eventName the event name used to determine which additional metadata should be emitted
   * @return {@link Map} with the additional metadata
   */
  Map<String, String> getMetadata(JobContext jobContext, EventName eventName);
}
