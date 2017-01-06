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

package gobblin.kafka.writer;

import java.io.IOException;

import com.codahale.metrics.Meter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import gobblin.instrumented.writer.InstrumentedDataWriter;
import gobblin.util.ConfigUtils;


/**
 * A Data Writer to use as a base for writing async best-effort writers.
 * This Data Writer allows writers to implement an async write interface
 * and provides the following features:
 * 1. Calculates metrics for the total number of records attempted, successfully written and failed.
 * 2. Waits for a specified amount of time on commit for all pending writes to complete.
 * 3. Do not proceed if a certain failure threshold is exceeded.
 *
 */
@Slf4j
public class AsyncBestEffortDataWriter<D> extends InstrumentedDataWriter<D> {
  private static final long MILLIS_TO_NANOS = 1000 * 1000;
  public static final String ATTEMPTED_RECORDS_METRIC_NAME_DEFAULT = "gobblin.writer.async.records.attempted";
  public static final String SUCCESS_RECORDS_METRIC_NAME_DEFAULT = "gobblin.writer.async.records.success";
  public static final String FAILED_RECORDS_METRIC_NAME_DEFAULT = "gobblin.writer.async.records.failed";
  public static final long COMMIT_TIMEOUT_IN_NANOS_DEFAULT = 60000L * MILLIS_TO_NANOS; // 1 minute
  public static final long COMMIT_STEP_WAITTIME_MILLIS_DEFAULT = 500; // 500 ms sleep while waiting for commit
  public static final double FAILURE_ALLOWANCE_DEFAULT = 0.0;

  @VisibleForTesting
  final Meter recordsAttempted;
  @VisibleForTesting
  final Meter recordsSuccess;
  @VisibleForTesting
  final Meter recordsFailed;
  private final long commitTimeoutInNanos;
  private final long commitStepWaitTimeMillis;
  private final double failureAllowance;
  private final AsyncDataWriter asyncDataWriter;
  private final WriteCallback writeCallback;


  protected AsyncBestEffortDataWriter(Config config,
      String recordsAttemptedMetricName,
      String recordsSuccessMetricName,
      String recordsFailedMetricName,
      long commitTimeoutInNanos,
      long commitStepWaitTimeMillis,
      double failureAllowance,
      @NonNull AsyncDataWriter asyncDataWriter)
  {
    super(ConfigUtils.configToState(config));
    this.recordsAttempted = getMetricContext().meter(recordsAttemptedMetricName);
    this.recordsSuccess = getMetricContext().meter(recordsSuccessMetricName);
    this.recordsFailed = getMetricContext().meter(recordsFailedMetricName);
    this.commitTimeoutInNanos = commitTimeoutInNanos;
    this.commitStepWaitTimeMillis = commitStepWaitTimeMillis;
    Preconditions.checkArgument((failureAllowance <= 1.0 && failureAllowance >= 0), "Failure Allowance must be a ratio between 0 and 1");
    this.failureAllowance = failureAllowance;
    this.asyncDataWriter = asyncDataWriter;
    this.writeCallback = new WriteCallback() {
      @Override
      public void onSuccess() {
        recordsSuccess.mark();
      }

      @Override
      public void onFailure(Exception exception) {
        recordsFailed.mark();
      }
    };
    this.asyncDataWriter.setDefaultCallback(this.writeCallback);
  }

  @Override
  public void writeImpl(D record)
      throws IOException {
    this.asyncDataWriter.asyncWrite(record);
    this.recordsAttempted.mark();
  }

  @Override
  public void cleanup()
      throws IOException {
    this.asyncDataWriter.cleanup();
  }

  @Override
  public long recordsWritten() {
    return recordsSuccess.getCount();
  }

  @Override
  public long bytesWritten()
      throws IOException {
    return this.asyncDataWriter.bytesWritten();
  }

  @Override
  public void close()
      throws IOException {
    log.debug("Close called");
    try {
      this.asyncDataWriter.close();
    }
    finally {
      super.close();
    }
  }

  @Override
  public void commit()
      throws IOException {
    log.debug("Commit called, will wait for commitTimeout : " + commitTimeoutInNanos / MILLIS_TO_NANOS + "ms");
    long commitStartTime = System.nanoTime();
    while (((System.nanoTime() - commitStartTime) < commitTimeoutInNanos)  &&
        (recordsAttempted.getCount() != (recordsSuccess.getCount() + recordsFailed.getCount())))
    {
      log.debug("Commit waiting... records produced: " + recordsAttempted.getCount() + " written: "
          + recordsSuccess.getCount() + " failed: " + recordsFailed.getCount());
      try {
        Thread.sleep(commitStepWaitTimeMillis);
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while waiting for commit to complete", e);
      }
    }
    log.debug("Commit done waiting");
    long recordsProducedFinal = recordsAttempted.getCount();
    long recordsWrittenFinal = recordsSuccess.getCount();
    long recordsFailedFinal = recordsFailed.getCount();
    long unacknowledgedWrites  = recordsProducedFinal - recordsWrittenFinal - recordsFailedFinal;
    long totalFailures = unacknowledgedWrites + recordsFailedFinal;
    if (unacknowledgedWrites > 0) // timeout
    {
      log.warn("Timeout waiting for all writes to be acknowledged. Missing " + unacknowledgedWrites
          + " responses out of " + recordsProducedFinal);
    }
    if (totalFailures > 0 && recordsProducedFinal > 0)
    {
      String message = "Commit failed to write " + totalFailures
          + " records (" + recordsFailedFinal + " failed, " + unacknowledgedWrites + " unacknowledged) out of "
          + recordsProducedFinal + " produced.";
      double failureRatio = (double)totalFailures / (double)recordsProducedFinal;
      if (failureRatio > failureAllowance)
      {
        message += "\nAborting because this is greater than the failureAllowance percentage: " + failureAllowance*100.0;
        log.error(message);
        throw new IOException(message);
      }
      else
      {
        message += "\nCommitting because failureRatio percentage: " + (failureRatio * 100.0) +
            " is less than the failureAllowance percentage: " + (failureAllowance * 100.0);
        log.warn(message);
      }
    }
    log.info("Successfully committed " + recordsWrittenFinal + " records.");
  }


  public static AsyncBestEffortDataWriterBuilder builder() {
    return new AsyncBestEffortDataWriterBuilder();
  }

  public static class AsyncBestEffortDataWriterBuilder {
    private Config config = ConfigFactory.empty();
    private String recordsAttemptedMetricName = ATTEMPTED_RECORDS_METRIC_NAME_DEFAULT;
    private String recordsSuccessMetricName = SUCCESS_RECORDS_METRIC_NAME_DEFAULT;
    private String recordsFailedMetricName = FAILED_RECORDS_METRIC_NAME_DEFAULT;
    private long commitTimeoutInNanos = COMMIT_TIMEOUT_IN_NANOS_DEFAULT;
    private long commitStepWaitTimeMillis = COMMIT_STEP_WAITTIME_MILLIS_DEFAULT;
    private double failureAllowance = FAILURE_ALLOWANCE_DEFAULT;
    private AsyncDataWriter asyncDataWriter;

    AsyncBestEffortDataWriterBuilder config(Config config)
    {
      this.config = config;
      return this;
    }

    AsyncBestEffortDataWriterBuilder recordsAttemptedMetricName(String recordsAttemptedMetricName)
    {
      this.recordsAttemptedMetricName = recordsAttemptedMetricName;
      return this;
    }

    AsyncBestEffortDataWriterBuilder recordsSuccessMetricName(String recordsSuccessMetricName)
    {
      this.recordsSuccessMetricName = recordsSuccessMetricName;
      return this;
    }

    AsyncBestEffortDataWriterBuilder recordsFailedMetricName(String recordsFailedMetricName)
    {
      this.recordsFailedMetricName = recordsFailedMetricName;
      return this;
    }

    AsyncBestEffortDataWriterBuilder commitTimeoutInNanos(long commitTimeoutInNanos)
    {
      this.commitTimeoutInNanos = commitTimeoutInNanos;
      return this;
    }

    AsyncBestEffortDataWriterBuilder commitStepWaitTimeInMillis(long commitStepWaitTimeMillis)
    {
      this.commitStepWaitTimeMillis = commitStepWaitTimeMillis;
      return this;
    }

    AsyncBestEffortDataWriterBuilder failureAllowance(double failureAllowance)
    {
      Preconditions.checkArgument((failureAllowance <= 1.0 && failureAllowance >= 0), "Failure Allowance must be a ratio between 0 and 1");
      this.failureAllowance = failureAllowance;
      return this;
    }

    AsyncBestEffortDataWriterBuilder asyncDataWriter(AsyncDataWriter asyncDataWriter)
    {
      this.asyncDataWriter = asyncDataWriter;
      return this;
    }


    AsyncBestEffortDataWriter build()
    {
      return new AsyncBestEffortDataWriter(config, recordsAttemptedMetricName,
          recordsSuccessMetricName, recordsFailedMetricName,
          commitTimeoutInNanos, commitStepWaitTimeMillis,
          failureAllowance, asyncDataWriter);
    }

  }

}
