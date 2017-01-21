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

import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;

import gobblin.source.extractor.CheckpointableWatermark;


/**
 * A Checkpointable Watermark that can be acknowledged. Useful for tracking watermark progress
 */
public class AcknowledgableWatermark implements Comparable<AcknowledgableWatermark>, Ackable {

  private final CheckpointableWatermark _checkpointableWatermark;
  private AtomicInteger _acked = new AtomicInteger(1);  // default number of acks needed is 1

  public AcknowledgableWatermark(CheckpointableWatermark watermark) {
    _checkpointableWatermark = watermark;
  }

  @Override
  public void ack() {
    _acked.decrementAndGet();
  }

  public AcknowledgableWatermark incrementAck() {
    _acked.incrementAndGet();
    return this;
  }

  public boolean isAcked() {
    return (_acked.get() == 0);
  }

  public CheckpointableWatermark getCheckpointableWatermark() {
    return _checkpointableWatermark;
  }

  @Override
  public int compareTo(AcknowledgableWatermark o) {
    Preconditions.checkArgument(o instanceof AcknowledgableWatermark);
    return _checkpointableWatermark.compareTo(o._checkpointableWatermark);
  }
}
