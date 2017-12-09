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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Optional;

import org.apache.gobblin.source.extractor.CheckpointableWatermark;


/**
 * A helper class that tracks committed and uncommitted watermarks.
 * Useful for implementing {@link WatermarkAwareWriter}s that wrap other {@link WatermarkAwareWriter}s.
 *
 * Note: The current implementation is not meant to be used in a high-throughput scenario
 * (e.g. in the path of a write or a callback). See {@link LastWatermarkTracker}.
 */
public class MultiWriterWatermarkTracker implements WatermarkTracker {


  private final ConcurrentHashMap<String, Set<CheckpointableWatermark>> candidateCommittables = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Set<CheckpointableWatermark>> unacknowledgedWatermarks = new ConcurrentHashMap<>();


  /**
   * Reset current state
   */
  public synchronized void reset() {
    candidateCommittables.clear();
    unacknowledgedWatermarks.clear();
  }

  private synchronized Set<CheckpointableWatermark> getOrCreate(Map<String, Set<CheckpointableWatermark>> map, String key) {
    if (map.containsKey(key)) {
      return map.get(key);
    } else {
      Set<CheckpointableWatermark> set = new TreeSet<>();
      map.put(key, set);
      return set;
    }
  }

  @Override
  public void committedWatermarks(Map<String, CheckpointableWatermark> committedMap) {
    committedWatermarks(committedMap.values());
  }

  public void committedWatermarks(Iterable<CheckpointableWatermark> committedStream) {
    for (CheckpointableWatermark committed: committedStream) {
      committedWatermark(committed);
    }
  }


  @Override
  public void committedWatermark(CheckpointableWatermark committed) {
    getOrCreate(candidateCommittables, committed.getSource()).add(committed);
  }

  @Override
  public void unacknowledgedWatermark(CheckpointableWatermark unacked) {
    getOrCreate(unacknowledgedWatermarks, unacked.getSource()).add(unacked);
  }

  @Override
  public void unacknowledgedWatermarks(Map<String, CheckpointableWatermark> unackedMap) {
    for (CheckpointableWatermark unacked: unackedMap.values()) {
      unacknowledgedWatermark(unacked);
    }
  }



  @Override
  public Map<String, CheckpointableWatermark> getAllCommitableWatermarks() {
    Map<String, CheckpointableWatermark> commitables = new HashMap<>(candidateCommittables.size());
    for (String source: candidateCommittables.keySet()) {
      Optional<CheckpointableWatermark> commitable = getCommittableWatermark(source);
      if (commitable.isPresent()) {
        commitables.put(commitable.get().getSource(), commitable.get());
      }
    }
    return commitables;
  }


  @Override
  public Map<String, CheckpointableWatermark> getAllUnacknowledgedWatermarks() {
    Map<String, CheckpointableWatermark> unackedMap = new HashMap<>(unacknowledgedWatermarks.size());
    for (String source: unacknowledgedWatermarks.keySet()) {
      Optional<CheckpointableWatermark> unacked = getUnacknowledgedWatermark(source);
      if (unacked.isPresent()) {
        unackedMap.put(unacked.get().getSource(), unacked.get());
      }
    }
    return unackedMap;
  }


  public Optional<CheckpointableWatermark> getCommittableWatermark(String source) {
    Set<CheckpointableWatermark> unacked = unacknowledgedWatermarks.get(source);


    CheckpointableWatermark
        minUnacknowledgedWatermark = (unacked == null || unacked.isEmpty())? null: unacked.iterator().next();

    CheckpointableWatermark highestCommitableWatermark = null;
    for (CheckpointableWatermark commitableWatermark : candidateCommittables.get(source)) {
      if ((minUnacknowledgedWatermark == null) || (commitableWatermark.compareTo(minUnacknowledgedWatermark) < 0)) {
        // commitableWatermark < minUnacknowledgedWatermark
        highestCommitableWatermark = commitableWatermark;
      }
    }
    if (highestCommitableWatermark == null) {
      return Optional.absent();
    } else {
      return Optional.of(highestCommitableWatermark);
    }

  }

  public Optional<CheckpointableWatermark> getUnacknowledgedWatermark(String source) {
    Set<CheckpointableWatermark> unacked = unacknowledgedWatermarks.get(source);

    if (unacked.isEmpty()) {
      return Optional.absent();
    } else {
      return Optional.of(unacked.iterator().next());
    }
  }
}
