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

package gobblin.fork;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.BooleanUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.runtime.ForkBranchMismatchException;
import gobblin.source.extractor.Message;
import gobblin.source.extractor.RecordEnvelope;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


/**
 * Splits an input {@link Stream} into multiple output {@link Stream}s based on a {@link ForkOperator}.
 */
@Slf4j
public class ForkedStreams<D> implements Runnable {

  private final Stream<RecordEnvelope<D>> inputStream;
  private final WorkUnitState workUnitState;
  private final ForkOperator<?, D> forkOperator;
  private final List<BlockingQueue<RecordEnvelope<D>>> outputQueues;
  @Getter
  private final List<Stream<RecordEnvelope<D>>> outputStreams;
  private final int branches;
  private final List<Boolean> enabledForks;
  private final long timeoutMillis;

  public ForkedStreams(Stream<RecordEnvelope<D>> inputStream, WorkUnitState workUnitState, ForkOperator<?, D> forkOperator,
      List<Boolean> enabledForks) {
    this.inputStream = inputStream;
    this.workUnitState = workUnitState;
    this.forkOperator = forkOperator;
    this.branches = this.forkOperator.getBranches(workUnitState);
    this.outputQueues = Lists.newArrayList();
    this.outputStreams = Lists.newArrayList();
    this.enabledForks = enabledForks;
    this.timeoutMillis = workUnitState.getPropAsLong(ConfigurationKeys.FORK_RECORD_QUEUE_TIMEOUT_KEY,
        ConfigurationKeys.DEFAULT_FORK_RECORD_QUEUE_TIMEOUT);

    if (this.branches == 1 && areSingleBranchTasksSynchronous(workUnitState)) {
      this.outputStreams.add(inputStream.filter(env -> this.forkOperator.forkDataRecord(workUnitState, env.getRecord()).get(0)));
    } else {
      for (int i = 0; i < this.branches; i++) {
        if (this.enabledForks.get(i)) {
          BlockingQueue<RecordEnvelope<D>> queue = Queues.newLinkedBlockingQueue(workUnitState.getPropAsInt(
              ConfigurationKeys.FORK_RECORD_QUEUE_CAPACITY_KEY,
              ConfigurationKeys.DEFAULT_FORK_RECORD_QUEUE_CAPACITY));
          this.outputQueues.add(queue);
          this.outputStreams.add(StreamSupport.stream(Spliterators.spliteratorUnknownSize(new QueueToStreamBridge(queue),
              Spliterator.IMMUTABLE | Spliterator.NONNULL), false));
        } else {
          this.outputQueues.add(null);
          this.outputStreams.add(null);
        }
      }

      new Thread(this).start();
    }
  }

  /**
   * An iterator that consumes from a {@link java.util.Queue} and handles {@link Message}s in the stream.
   */
  @RequiredArgsConstructor
  private class QueueToStreamBridge implements Iterator<RecordEnvelope<D>> {
    private final BlockingQueue<RecordEnvelope<D>> queue;
    private RecordEnvelope<D> nextRecord;

    @Override
    public boolean hasNext() {
      if (this.nextRecord != null) {
        return true;
      }

      while (true) {
        try {
          this.nextRecord = this.queue.poll(ForkedStreams.this.timeoutMillis, TimeUnit.MILLISECONDS);
          if (this.nextRecord instanceof Message) {
            switch (((Message) this.nextRecord).getMessageType()) {
              case END_OF_STREAM:
                return false;
              case FAILED_STREAM:
                throw new RuntimeException("Pre-fork stream failed.");
              default:
                throw new IllegalStateException("Not a recognized message: " + ((Message) this.nextRecord).getMessageType());
            }
          } else {
            return this.nextRecord != null;
          }
        } catch (InterruptedException ie) {
          return false;
        }
      }
    }

    @Override
    public RecordEnvelope<D> next() {
      RecordEnvelope<D> record = this.nextRecord;
      this.nextRecord = null;
      return record;
    }
  }

  /**
   * The processing done in an async thread. Pulls records from the input stream and pushes them to the appropriate
   * output streams. This thread is automatically scheduled by the {@link ForkedStreams} object when necessary, it should not be
   * started from anywhere else.
   */
  @Override
  public void run() {
    try {
      this.inputStream.forEach(this::pushToAppropriateStreams);
      for (BlockingQueue<RecordEnvelope<D>> queue : this.outputQueues) {
        queue.put(new Message<>(Message.MessageType.END_OF_STREAM));
      }
    } catch (Throwable t) {
      try {
        for (BlockingQueue<RecordEnvelope<D>> queue : this.outputQueues) {
          queue.put(new Message<>(Message.MessageType.FAILED_STREAM));
        }
        log.error("Error in pre-fork stream.", t);
      } catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }
  }

  private void pushToAppropriateStreams(RecordEnvelope<D> envelope) {
    try {
      List<Boolean> forkedRecords = this.forkOperator.forkDataRecord(this.workUnitState, envelope.getRecord());

      if (forkedRecords.size() != branches) {
        throw new ForkBranchMismatchException(
            String.format("Number of forked data records [%d] is not equal to number of branches [%d]", forkedRecords.size(),
                branches));
      }

      boolean needToCopy = inMultipleBranches(forkedRecords);
      // we only have to copy a record if it needs to go into multiple forks
      if (needToCopy && !(CopyHelper.isCopyable(envelope.getRecord()))) {
        throw new CopyNotSupportedException(envelope.getRecord().getClass().getName() + " is not copyable");
      }

      int copyInstance = 0;
      RecordEnvelope.ForkedRecordBuilder forkedRecordBuilder = needToCopy ? envelope.forkedRecordBuilder() : null;
      for (int i = 0; i < forkedRecords.size(); i++) {
        if (this.enabledForks.get(i) && forkedRecords.get(i)) {
          D recordForFork = needToCopy ? (D) CopyHelper.copy(envelope.getRecord(), copyInstance) : envelope.getRecord();
          copyInstance++;
          if (!this.outputQueues.get(i).offer(forkedRecordBuilder == null ? envelope.withRecord(recordForFork)
              : forkedRecordBuilder.forkWithRecord(recordForFork), this.timeoutMillis, TimeUnit.MILLISECONDS)) {
            throw new TimeoutException("Could not add record into fork buffer for fork " + i);
          }
        }
      }
      if (copyInstance == 0) {
        // record was not sent anywhere
        envelope.ack();
      }
      if (forkedRecordBuilder != null) {
        forkedRecordBuilder.close();
      }
    } catch (ForkBranchMismatchException | CopyNotSupportedException | InterruptedException | TimeoutException exc) {
      throw new RuntimeException(exc);
    }
  }

  /**
   * Check if a schema or data record is being passed to more than one branches.
   */
  public static boolean inMultipleBranches(List<Boolean> branches) {
    int inBranches = 0;
    for (Boolean bool : branches) {
      if (bool && ++inBranches > 1) {
        break;
      }
    }
    return inBranches > 1;
  }

  private boolean areSingleBranchTasksSynchronous(WorkUnitState workUnitState) {
    return BooleanUtils.toBoolean(workUnitState
        .getProp(ConfigurationKeys.TASK_IS_SINGLE_BRANCH_SYNCHRONOUS, ConfigurationKeys.DEFAULT_TASK_IS_SINGLE_BRANCH_SYNCHRONOUS));
  }
}
