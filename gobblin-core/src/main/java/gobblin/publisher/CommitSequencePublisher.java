/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.publisher;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;

import gobblin.annotation.Alpha;
import gobblin.commit.CommitSequence;
import gobblin.commit.FsRenameCommitStep;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.util.ParallelRunner;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * An implementation of {@link DataPublisher} for exactly-once delivery.
 *
 * <p>
 *   This publisher does not actually publish data, instead it constructs a {@link CommitSequence.Builder}.
 *   The builder is used by Gobblin runtime to build a {@link CommitSequence}, which is then persisted
 *   and executed.
 * </p>
 *
 * @author ziliu
 */
@Alpha
@Slf4j
public class CommitSequencePublisher extends BaseDataPublisher {

  @Getter
  protected Optional<CommitSequence.Builder> commitSequenceBuilder = Optional.of(new CommitSequence.Builder());

  public CommitSequencePublisher(State state) throws IOException {
    super(state);
  }

  @Override
  public void publish(Collection<? extends WorkUnitState> states) throws IOException {
    super.publish(states);

    if (!states.isEmpty()) {

      String jobName = Iterables.get(states, 0).getProp(ConfigurationKeys.JOB_NAME_KEY);
      String datasetUrn =
          Iterables.get(states, 0).getProp(ConfigurationKeys.DATASET_URN_KEY, ConfigurationKeys.DEFAULT_DATASET_URN);
      this.commitSequenceBuilder.get().withJobName(jobName).withDatasetUrn(datasetUrn);
    } else {
      log.warn("No workunitstate to publish");
      this.commitSequenceBuilder = Optional.<CommitSequence.Builder> absent();
    }
  }

  /**
   * This method does not actually move data, but it creates an {@link FsRenameCommitStep}.
   */
  @Override
  protected void movePath(ParallelRunner parallelRunner, Path src, Path dst, int branchId) throws IOException {
    log.info(String.format("Creating CommitStep for moving %s to %s", src, dst));
    this.commitSequenceBuilder.get().beginStep(FsRenameCommitStep.Builder.class).withProps(this.state).from(src)
        .withSrcFs(this.writerFileSystemByBranches.get(branchId)).to(dst)
        .withDstFs(this.publisherFileSystemByBranches.get(branchId)).endStep();
  }

}
