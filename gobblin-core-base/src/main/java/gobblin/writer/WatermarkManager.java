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

import java.io.Closeable;
import java.util.Collections;
import java.util.Map;

import lombok.Getter;
import lombok.ToString;

import gobblin.source.extractor.CheckpointableWatermark;


/**
 * An interface for WatermarkManagers: classes that can keep track of watermarks and commit them to watermark storage.
 */
public interface WatermarkManager extends Closeable {

  void start();

  @Getter
  @ToString
  class RetrievalStatus {
    private long lastWatermarkRetrievalAttemptTimestampMillis = 0;
    private long lastWatermarkRetrievalSuccessTimestampMillis = 0;
    private long lastWatermarkRetrievalFailureTimestampMillis = 0;
    private Map<String, CheckpointableWatermark> lastRetrievedWatermarks = Collections.EMPTY_MAP;
    private Exception lastRetrievalException = null;

    synchronized void onAttempt() {
      this.lastWatermarkRetrievalAttemptTimestampMillis = System.currentTimeMillis();
    }

    synchronized void onSuccess(Map<String, CheckpointableWatermark> retrievedWatermarks) {
      this.lastWatermarkRetrievalSuccessTimestampMillis = System.currentTimeMillis();
      this.lastRetrievedWatermarks = retrievedWatermarks;
    }

    synchronized void onFailure(Exception retrievalException) {
      this.lastWatermarkRetrievalFailureTimestampMillis = System.currentTimeMillis();
      this.lastRetrievalException = retrievalException;
    }

  }

  @Getter
  @ToString
  class CommitStatus {
    private long lastWatermarkCommitAttemptTimestampMillis = 0;
    private long lastWatermarkCommitSuccessTimestampMillis = 0;
    private long lastWatermarkCommitFailureTimestampMillis = 0;
    private Map<String, CheckpointableWatermark> lastCommittedWatermarks = Collections.EMPTY_MAP;
    private Exception lastCommitException = null;
    private Map<String, CheckpointableWatermark> lastFailedWatermarks = Collections.EMPTY_MAP;

    synchronized void onAttempt() {
      lastWatermarkCommitAttemptTimestampMillis = System.currentTimeMillis();
    }

    synchronized void onSuccess(Map<String, CheckpointableWatermark> watermarksToCommit) {
      lastWatermarkCommitSuccessTimestampMillis = System.currentTimeMillis();
      lastCommittedWatermarks = watermarksToCommit;
    }

    synchronized void onFailure(Exception commitException, Map<String, CheckpointableWatermark> watermarksToCommit) {
      lastWatermarkCommitFailureTimestampMillis = System.currentTimeMillis();
      lastCommitException = commitException;
      lastFailedWatermarks = watermarksToCommit;
    }

  }

  CommitStatus getCommitStatus();

  RetrievalStatus getRetrievalStatus();


}
