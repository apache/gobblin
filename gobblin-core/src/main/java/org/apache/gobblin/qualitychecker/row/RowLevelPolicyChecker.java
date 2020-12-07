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

package org.apache.gobblin.qualitychecker.row;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.gobblin.commit.SpeculativeAttemptAwareConstruct;
import org.apache.gobblin.stream.FlushControlMessage;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Strings;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.stream.ControlMessage;
import org.apache.gobblin.records.ControlMessageHandler;
import org.apache.gobblin.records.RecordStreamProcessor;
import org.apache.gobblin.records.RecordStreamWithMetadata;
import org.apache.gobblin.stream.RecordEnvelope;
import org.apache.gobblin.stream.StreamEntity;
import org.apache.gobblin.util.FinalState;
import org.apache.gobblin.util.HadoopUtils;

import io.reactivex.Flowable;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class RowLevelPolicyChecker<S, D> implements Closeable, FinalState, RecordStreamProcessor<S, S, D, D>,
                                                    SpeculativeAttemptAwareConstruct {

  /**
   * Given the existence of writer object when the policy is set to {@link RowLevelPolicy.Type#ERR_FILE}, objects of
   * this class needs to be speculative-attempt-aware.
   */
  @Override
  public boolean isSpeculativeAttemptSafe() {
    return this.list.stream().noneMatch(x -> x.getType().equals(RowLevelPolicy.Type.ERR_FILE)) || this.allowSpeculativeExecWhenWriteErrFile;
  }

  @Getter
  private final List<RowLevelPolicy> list;
  private final String stateId;
  private final FileSystem fs;
  private boolean errFileOpen;
  private final FrontLoadedSampler sampler;
  private RowLevelErrFileWriter writer;
  @Getter
  private final RowLevelPolicyCheckResults results;
  /** Flag to determine if it is safe to enable speculative execution when policy is set to ERR_FILE
   * Users are suggested to turn this off since it could potentially run into HDFS file lease contention if multiple
   * speculative execution are appending to the same ERR_FILE.
   *
   * When there are ERR_FILE policy appears and users are enforcing to set it to true, RowPolicyChecker will created
   * different ERR_FILE with timestamp in name to avoid contention but there's no guarantee as
   * different containers' clock is hard to coordinate.
   * */
  private boolean allowSpeculativeExecWhenWriteErrFile;

  static final String ALLOW_SPECULATIVE_EXECUTION_WITH_ERR_FILE_POLICY = "allowSpeculativeExecutionWithErrFilePolicy";

  public RowLevelPolicyChecker(List<RowLevelPolicy> list, String stateId, FileSystem fs) {
    this(list, stateId, fs, new State());
  }

  public RowLevelPolicyChecker(List<RowLevelPolicy> list, String stateId, FileSystem fs, State state) {
    this.list = list;
    this.stateId = stateId;
    this.fs = fs;
    this.errFileOpen = false;
    this.results = new RowLevelPolicyCheckResults();
    this.sampler = new FrontLoadedSampler(state.getPropAsLong(ConfigurationKeys.ROW_LEVEL_ERR_FILE_RECORDS_PER_TASK,
        ConfigurationKeys.DEFAULT_ROW_LEVEL_ERR_FILE_RECORDS_PER_TASK), 1.5);

    /** By default set to true as to maintain backward-compatibility */
    this.allowSpeculativeExecWhenWriteErrFile = state.getPropAsBoolean(ALLOW_SPECULATIVE_EXECUTION_WITH_ERR_FILE_POLICY, true);
  }

  public boolean executePolicies(Object record, RowLevelPolicyCheckResults results) throws IOException {
    for (RowLevelPolicy p : this.list) {
      RowLevelPolicy.Result result = p.executePolicy(record);
      results.put(p, result);
      if (!checkResult(result, p, record)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Handle the result of {@link RowLevelPolicy#executePolicy(Object)}
   */
  protected boolean checkResult(RowLevelPolicy.Result checkResult, RowLevelPolicy p, Object record) throws IOException {
    boolean result = true;
    if (checkResult.equals(RowLevelPolicy.Result.FAILED)) {
      if (p.getType().equals(RowLevelPolicy.Type.FAIL)) {
        throw new RuntimeException("RowLevelPolicy " + p + " failed on record " + record);
      } else if (p.getType().equals(RowLevelPolicy.Type.ERR_FILE)) {
        if (this.sampler.acceptNext()) {
          if (!this.errFileOpen) {
            this.writer = new RowLevelErrFileWriter(this.fs);
            this.writer.open(getErrFilePath(p));
            this.writer.write(record);
          } else {
            this.writer.write(record);
          }
          this.errFileOpen = true;
        }
      }
      result = false;
    }
    return result;
  }

  Path getErrFilePath(RowLevelPolicy policy) {
    String errFileName = HadoopUtils.sanitizePath(policy.toString(), "-");
    if (!Strings.isNullOrEmpty(this.stateId)) {
      errFileName += "-" + this.stateId;
    }
    if (allowSpeculativeExecWhenWriteErrFile) {
      errFileName += "-" + System.currentTimeMillis();
    }

    errFileName += ".err";
    return new Path(policy.getErrFileLocation(), errFileName);
  }

  @Override
  public void close() throws IOException {
    if (this.errFileOpen) {
      this.writer.close();
      this.errFileOpen = false;
    }
  }

  /**
   * Get final state for this object, obtained by merging the final states of the
   * {@link org.apache.gobblin.qualitychecker.row.RowLevelPolicy}s used by this object.
   * @return Merged {@link org.apache.gobblin.configuration.State} of final states for
   *                {@link org.apache.gobblin.qualitychecker.row.RowLevelPolicy} used by this checker.
   */
  @Override
  public State getFinalState() {
    State state = new State();
    for (RowLevelPolicy policy : this.list) {
      state.addAll(policy.getFinalState());
    }
    return state;
  }

  /**
   * Process the stream and drop any records that fail the quality check.
   */
  @Override
  public RecordStreamWithMetadata<D, S> processStream(RecordStreamWithMetadata<D, S> inputStream, WorkUnitState state) {
    Flowable<StreamEntity<D>> filteredStream =
        inputStream.getRecordStream().filter(r -> {
          if (r instanceof ControlMessage) {
            getMessageHandler().handleMessage((ControlMessage) r);
            return true;
          } else if (r instanceof RecordEnvelope) {
            boolean accept = executePolicies(((RecordEnvelope) r).getRecord(), this.results);
            if (!accept) {
              r.ack();
            }
            return accept;
          } else {
            return true;
          }
        });
    filteredStream = filteredStream.doFinally(this::close);
    return inputStream.withRecordStream(filteredStream);
  }

  /**
   * @return a {@link ControlMessageHandler}.
   */
  protected ControlMessageHandler getMessageHandler() {
    /**
     * When seeing {@link FlushControlMessage and using ERR_FILE as the quality-checker handling,
     * close the open error file and create new one.
     */
    return new ControlMessageHandler() {
      @Override
      public void handleMessage(ControlMessage message) {
        if (message instanceof FlushControlMessage ) {
          try {
            RowLevelPolicyChecker.this.close();
          } catch (IOException ioe) {
            log.error("Failed to close errFile", ioe);
          }
        }
      }
    };
  }

  /**
   * A sampler used to ensure the err file contains at most around {@link #targetRecordsAccepted}
   * records.
   *
   * Basically, we will write the first {@link #targetRecordsAccepted} records without sampling. After that we apply a
   * rapidly decaying sampling to make sure we write at most about 100 additional records, spread out through the
   * rest of the stream.
   */
  @ThreadSafe
  static class FrontLoadedSampler {

    private final long targetRecordsAccepted;
    /**
     * Specifies how sampling decays at the tail end of the stream (after the first {@link #targetRecordsAccepted} have
     * been accepeted). We will accept at most roughly
     * {@link #targetRecordsAccepted} + 8/log_{10}({@link #decayFactor}) records total.
     */
    private final double decayFactor;
    private final AtomicLong errorRecords = new AtomicLong();
    private final AtomicLong nextErrorRecordWritten = new AtomicLong();

    public FrontLoadedSampler(long targetRecordsAccepted, double decayFactor) {
      this.targetRecordsAccepted = targetRecordsAccepted;
      this.decayFactor = Math.max(1, decayFactor);
      if (this.targetRecordsAccepted <= 0) {
        this.nextErrorRecordWritten.set(Long.MAX_VALUE);
      }
    }

    public boolean acceptNext() {
      long recordNum = this.errorRecords.getAndIncrement();
      while (recordNum >= this.nextErrorRecordWritten.get()) {
        if (this.nextErrorRecordWritten.compareAndSet(recordNum, computeNextErrorRecordWritten())) {
          return true;
        }
      }
      return false;
    }

    private long computeNextErrorRecordWritten() {
      long current = this.nextErrorRecordWritten.get();
      if (current < this.targetRecordsAccepted) {
        return current + 1;
      } else {
        return (long) (this.decayFactor * current) + 1;
      }
    }
  }
}
