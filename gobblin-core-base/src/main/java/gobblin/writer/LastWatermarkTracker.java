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

package gobblin.writer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import gobblin.source.extractor.CheckpointableWatermark;


/**
 * A {@link WatermarkTracker} that only tracks the last committed watermark and doesn't compare
 * previous existing watermarks. Useful for {@link WatermarkAwareWriter}s that are ordered and synchronous
 * in nature.
 */
public class LastWatermarkTracker implements WatermarkTracker {

  private final Map<String, CheckpointableWatermark> _committedWatermarkMap;
  private final Map<String, CheckpointableWatermark> _unackedWatermarkMap;
  private boolean ignoreUnacknowledged;

  public LastWatermarkTracker(boolean ignoreUnacknowledged) {
    _committedWatermarkMap = new ConcurrentHashMap<>();
    if (ignoreUnacknowledged) {
      _unackedWatermarkMap = null;
    } else {
      _unackedWatermarkMap = new ConcurrentHashMap<>();
    }

    this.ignoreUnacknowledged = ignoreUnacknowledged;
  }

  @Override
  public void reset() {
    _committedWatermarkMap.clear();
    if (_unackedWatermarkMap != null) {
      _unackedWatermarkMap.clear();
    }
  }

  @Override
  public void committedWatermarks(Map<String, CheckpointableWatermark> committedMap) {
    _committedWatermarkMap.putAll(committedMap);
  }

  @Override
  public void committedWatermark(CheckpointableWatermark committed) {
    _committedWatermarkMap.put(committed.getSource(), committed);
  }

  @Override
  public void unacknowledgedWatermark(CheckpointableWatermark unacked) {
    if (_unackedWatermarkMap != null) {
      _unackedWatermarkMap.put(unacked.getSource(), unacked);
    }
  }

  @Override
  public void unacknowledgedWatermarks(Map<String, CheckpointableWatermark> unackedMap) {
    if (_unackedWatermarkMap != null) {
      _unackedWatermarkMap.putAll(unackedMap);
    }
  }

  @Override
  public Map<String, CheckpointableWatermark> getAllCommitableWatermarks() {
    return new HashMap<>(_committedWatermarkMap);
  }

  @Override
  public Map<String, CheckpointableWatermark> getAllUnacknowledgedWatermarks() {
    if (_unackedWatermarkMap != null) {
      return new HashMap<>(_unackedWatermarkMap);
    } else {
      return Collections.EMPTY_MAP;
    }
  }
}
