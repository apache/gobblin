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

import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.records.RecordStreamWithMetadata;
import gobblin.stream.ControlMessage;
import gobblin.stream.RecordEnvelope;
import gobblin.stream.StreamEntity;

import io.reactivex.Flowable;
import io.reactivex.flowables.ConnectableFlowable;
import io.reactivex.functions.Predicate;
import lombok.Data;


/**
 * Forks a {@link RecordStreamWithMetadata} into multiple branches specified by a {@link ForkOperator}.
 *
 * Each forked stream is a mirror of the original stream.
 */
public class Forker {

  /**
   * Obtain the {@link ForkedStream} for the input {@link RecordStreamWithMetadata} and {@link ForkOperator}.
   * @param inputStream input {@link Flowable} of records.
   * @param forkOperator {@link ForkOperator} specifying the fork behavior.
   * @param workUnitState work unit configuration.
   * @return a {@link ForkedStream} with the forked streams.
   * @throws Exception if the {@link ForkOperator} throws any exceptions.
   */
  public <D, S> ForkedStream<D, S>
     forkStream(RecordStreamWithMetadata<D, S> inputStream, ForkOperator<S, D> forkOperator, WorkUnitState workUnitState)
      throws Exception {

    int branches = forkOperator.getBranches(workUnitState);
    // Set fork.branches explicitly here so the rest task flow can pick it up
    workUnitState.setProp(ConfigurationKeys.FORK_BRANCHES_KEY, branches);

    forkOperator.init(workUnitState);
    List<Boolean> forkedSchemas = forkOperator.forkSchema(workUnitState, inputStream.getSchema());
    int activeForks = (int) forkedSchemas.stream().filter(b -> b).count();

    Preconditions.checkState(forkedSchemas.size() == branches, String
        .format("Number of forked schemas [%d] is not equal to number of branches [%d]", forkedSchemas.size(),
            branches));

    Flowable<RecordWithForkMap<D>> forkedStream = inputStream.getRecordStream().map(r -> {
      if (r instanceof RecordEnvelope) {
        RecordEnvelope<D> recordEnvelope = (RecordEnvelope<D>) r;
        return new RecordWithForkMap<>(recordEnvelope, forkOperator.forkDataRecord(workUnitState, recordEnvelope.getRecord()));
      } else if (r instanceof ControlMessage) {
        return new RecordWithForkMap<D>((ControlMessage<D>) r);
      } else {
        throw new IllegalStateException("Expected RecordEnvelope or ControlMessage.");
      }
    });

    if (activeForks > 1) {
      forkedStream = forkedStream.share();
    }

    List<RecordStreamWithMetadata<D, S>> forkStreams = Lists.newArrayList();

    boolean mustCopy = mustCopy(forkedSchemas);
    for(int i = 0; i < forkedSchemas.size(); i++) {
      if (forkedSchemas.get(i)) {
        final int idx = i;
        Flowable<StreamEntity<D>> thisStream =
            forkedStream.filter(new ForkFilter<>(idx)).map(RecordWithForkMap::getRecordCopyIfNecessary);
        forkStreams.add(inputStream.withRecordStream(thisStream,
            mustCopy ? (S) CopyHelper.copy(inputStream.getSchema()) : inputStream.getSchema()));
      } else {
        forkStreams.add(null);
      }
    }

    return new ForkedStream<>(forkStreams);
  }

  private static boolean mustCopy(List<Boolean> forkMap) {
    return forkMap.stream().filter(b -> b).count() >= 2;
  }

  /**
   * An object containing the forked streams and a {@link ConnectableFlowable} used to connect the stream when all
   * streams have been subscribed to.
   */
  @Data
  public static class ForkedStream<D, S> {
    /** A list of forked streams. Note some of the forks may be null if the {@link ForkOperator} marks them as disabled. */
    private final List<RecordStreamWithMetadata<D, S>> forkedStreams;
  }

  /**
   * Filter records so that only records corresponding to flow {@link #forkIdx} pass.
   */
  @Data
  private static class ForkFilter<D> implements Predicate<RecordWithForkMap<D>> {
    private final int forkIdx;

    @Override
    public boolean test(RecordWithForkMap<D> dRecordWithForkMap) {
      return dRecordWithForkMap.sendToBranch(this.forkIdx);
    }
  }

  /**
   * Used to hold a record as well and the map specifying which forks it should go to.
   */
  private static class RecordWithForkMap<D> {
    private final StreamEntity<D> record;
    private final List<Boolean> forkMap;
    private final boolean mustCopy;

    public RecordWithForkMap(RecordEnvelope<D> record, List<Boolean> forkMap) {
      this.record = record;
      this.forkMap = Lists.newArrayList(forkMap);
      this.mustCopy = mustCopy(forkMap);
    }

    public RecordWithForkMap(ControlMessage<D> record) {
      this.record = record;
      this.forkMap = null;
      this.mustCopy = true;
    }

    private StreamEntity<D> getRecordCopyIfNecessary() throws CopyNotSupportedException {
      if(this.mustCopy) {
        return this.record.getClone();
      } else {
        return this.record;
      }
    }

    public boolean sendToBranch(int idx) {
      if (record instanceof RecordEnvelope) {
        return this.forkMap.get(idx);
      } else {
        return true;
      }
    }

  }

}
