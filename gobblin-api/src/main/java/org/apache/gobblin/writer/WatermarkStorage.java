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
package org.apache.gobblin.writer;

import java.io.IOException;
import java.util.Map;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.source.extractor.CheckpointableWatermark;


/**
 * Storage for Watermarks. Used in streaming execution.
 */
@Alpha
public interface WatermarkStorage {

  /**
   * Commit a batch of watermarks to storage.
   *
   */
  void commitWatermarks(Iterable<CheckpointableWatermark> watermarks) throws IOException;

  /**
   * Retrieve previously committed watermarks.
   * @param watermarkClass: the specific class that corresponds to the watermark expected
   * @param sourcePartitions: a list of source partitions for whom we're retrieving committed watermarks.
   * @return a map of String -> CheckpointableWatermark.
   * The key corresponds to the source field in the CheckpointableWatermark and belongs to
   * the list of source partitions passed in.
   */
  Map<String, CheckpointableWatermark> getCommittedWatermarks(Class<? extends CheckpointableWatermark> watermarkClass,
      Iterable<String> sourcePartitions) throws IOException;

}
