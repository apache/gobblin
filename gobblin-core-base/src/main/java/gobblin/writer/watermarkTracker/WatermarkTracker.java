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

package gobblin.writer.watermarkTracker;

import java.util.Map;

import gobblin.source.extractor.CheckpointableWatermark;


/**
 * An interface for a WatermarkTracker. Implementations are expected to serve as helper
 * classes to track watermarks in different use-cases.
 */
public interface WatermarkTracker {

  /**
   * @return the largest {@link CheckpointableWatermark}for each source such that all watermarks smaller
   * than or equal to that watermark have been acked.
   */
  Map<String, CheckpointableWatermark> getAllCommitableWatermarks();

  /**
   * @return the smallest unacked {@link CheckpointableWatermark} for each source.
   */
  Map<String, CheckpointableWatermark> getAllUnacknowledgedWatermarks();
}
