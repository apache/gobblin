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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import lombok.Getter;
import lombok.ToString;

import gobblin.source.extractor.CheckpointableWatermark;
import gobblin.util.ExecutorsUtils;


/**
 * Responsible for managing continuous commit of watermarks.
 * Periodically fetches watermarks from WatermarkAwareWriters and commits them to WatermarkStorage.
 * TODO: Add metrics monitoring
 */
public class WatermarkManager implements Closeable {

  @Getter
  @ToString
  public static class RetrievalStatus {
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
  public static class CommitStatus {
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

  private final List<WatermarkAwareWriter> _watermarkAwareWriters;
  private final WatermarkStorage _watermarkStorage;
  private final long _commitIntervalMillis;
  private final ScheduledExecutorService _watermarkCommitThreadPool;
  private final Logger _logger;
  private final RetrievalStatus _retrievalStatus;
  private final CommitStatus _commitStatus;

  @VisibleForTesting
  final Runnable _watermarkCommitter = new

      Runnable() {
        @Override
        public void run() {
          long startTime = System.nanoTime();
          Map<String, CheckpointableWatermark> watermarksToCommit = null;
          try {
            _retrievalStatus.onAttempt();
            WatermarkTracker watermarkTracker = new WatermarkTracker();
            for (WatermarkAwareWriter writer : _watermarkAwareWriters) {
              Map<String, CheckpointableWatermark> writerWatermarks = writer.getCommittableWatermark();
              _logger.debug("Retrieved from writer {} : watermark {} ", writer.getClass().getName(), writerWatermarks);
              watermarkTracker.committedWatermarks(writerWatermarks);
            }
            watermarksToCommit = watermarkTracker.getAllCommitableWatermarks();
            _retrievalStatus.onSuccess(watermarksToCommit);
          }
          catch (Exception e) {
            _retrievalStatus.onFailure(e);
            _logger.error("Failed to get watermark", e);
          }
          // Prevent multiple commits concurrently
          synchronized (this) {
            if (watermarksToCommit != null && !watermarksToCommit.isEmpty()) {
              try {
                _commitStatus.onAttempt();
                _logger.info("Will commit watermark {}", watermarksToCommit.toString());
                //TODO: Not checking if this watermark has already been committed successfully.
                _watermarkStorage.commitWatermarks(watermarksToCommit.values());
                _commitStatus.onSuccess(watermarksToCommit);
              } catch (Exception e) {
                _commitStatus.onFailure(e, watermarksToCommit);
                _logger.error("Failed to write watermark", e);
              }
            } else {
              _logger.info("Nothing to commit");
            }
          }
          long duration = (System.nanoTime() - startTime)/1000000;
          _logger.info("Duration of run {} milliseconds", duration);
        }
      };


  public WatermarkManager(WatermarkStorage storage, long commitIntervalMillis, Optional<Logger> logger) {
    Preconditions.checkArgument(storage != null, "WatermarkStorage cannot be null");
    _watermarkAwareWriters = new ArrayList<>(1);
    _watermarkStorage = storage;
    _commitIntervalMillis = commitIntervalMillis;
    _logger = logger.or(LoggerFactory.getLogger(WatermarkManager.class));
    _watermarkCommitThreadPool = new ScheduledThreadPoolExecutor(1, ExecutorsUtils.newThreadFactory(logger,
        Optional.of("WatermarkManager-%d")));
    _retrievalStatus = new RetrievalStatus();
    _commitStatus = new CommitStatus();
  }

  public void registerWriter(WatermarkAwareWriter dataWriter) {
      _watermarkAwareWriters.add(dataWriter);
      _logger.info("Registered a watermark aware writer {}", dataWriter.getClass().getName());
  }

  public void start() {
    _watermarkCommitThreadPool
        .scheduleWithFixedDelay(_watermarkCommitter, 0, _commitIntervalMillis, TimeUnit.MILLISECONDS);
  }

  @Override
  public void close()
      throws IOException {
    _logger.info("Watermark committer closing");
    _watermarkCommitThreadPool.shutdown();
    try {
      long startTime = System.nanoTime();
      _watermarkCommitThreadPool.awaitTermination(1000, TimeUnit.MILLISECONDS);
      long duration = (System.nanoTime() - startTime)/ 1000000;
      _logger.info("Duration of termination wait was {} milliseconds", duration);
    }
    catch (InterruptedException ie) {
      throw new IOException("Interrupted while waiting for committer to shutdown", ie);
    }
    finally {
      // final watermark commit
      _logger.info("Watermark committer: one last commit before shutting down");
      _watermarkCommitter.run();
    }
  }

  public CommitStatus getCommitStatus() {
    return _commitStatus;
  }

  public RetrievalStatus getRetrievalStatus() {
    return _retrievalStatus;
  }
}
