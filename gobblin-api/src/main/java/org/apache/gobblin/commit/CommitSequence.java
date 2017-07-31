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

package org.apache.gobblin.commit;

import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.gobblin.annotation.Alpha;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * A sequence of {@link CommitStep}s that should be executed atomically.
 *
 * <p>
 *    Typical pattern for building a {@link CommitSequence} with two {@link CommitStep}s:
 *
 *    <pre>
 *      {@code
 *        CommitSequence sequence = CommitSequence.newBuilder()
 *                                  .withJobName(jobName)
 *                                  .withDatasetUrn(datasetUrn)
 *
 *                                  .beginStep(FsRenameCommitStep.Builder.class)
 *                                  .withProps(props)
 *                                  .from(srcPath)
 *                                  .to(dstPath)
 *                                  .endStep()
 *
 *                                  .beginStep(DatasetStateCommitStep.Builder.class)
 *                                  .withProps(props)
 *                                  .withDatasetUrn(datasetUrn)
 *                                  .withDatasetState(datasetState)
 *                                  .endStep()
 *
 *                                  .build();
 *      }
 *    </pre>
 * </p>
 *
 * @author Ziyang Liu
 */
@Alpha
@Slf4j
public class CommitSequence {

  @Getter
  private final String jobName;

  @Getter
  private final String datasetUrn;

  private final List<CommitStep> steps;

  private CommitSequence(Builder builder) {
    this.jobName = builder.jobName;
    this.datasetUrn = builder.datasetUrn;
    this.steps = ImmutableList.copyOf(builder.steps);
  }

  public static class Builder {

    private String jobName;
    private String datasetUrn;
    private final List<CommitStep> steps = Lists.newArrayList();

    /**
     * Set the job name for the commit sequence.
     */
    public Builder withJobName(String jobName) {
      this.jobName = jobName;
      return this;
    }

    /**
     * Set the dataset URN for the commit sequence.
     */
    public Builder withDatasetUrn(String datasetUrn) {
      this.datasetUrn = datasetUrn;
      return this;
    }

    /**
     * Build a {@link CommitStep}.
     *
     * @param builderClass The builder class for the {@link CommitStep}, which should extend
     * {@link CommitStepBase.Builder}.
     * @return An instance of the builder class for the {@link CommitStep}.
     */
    public <T extends CommitStepBase.Builder<?>> T beginStep(Class<T> builderClass) {
      try {
        return builderClass.getDeclaredConstructor(this.getClass()).newInstance(this);
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException("Failed to instantiate " + builderClass, e);
      }
    }

    /**
     * Add a {@link CommitStep} to the commit sequence.
     */
    public Builder addStep(CommitStep step) {
      this.steps.add(step);
      return this;
    }

    public CommitSequence build() {
      Preconditions.checkState(!Strings.isNullOrEmpty(this.jobName), "Job name not specified for commit sequence");
      Preconditions.checkState(!Strings.isNullOrEmpty(this.datasetUrn),
          "Dataset URN not specified for commit sequence");
      Preconditions.checkState(!this.steps.isEmpty(), "No commit steps specified for the commit sequence");

      return new CommitSequence(this);
    }

  }

  /**
   * Execute the {@link CommitStep}s in the order they are added to the commit sequence.
   */
  public void execute() {
    try {
      for (CommitStep step : this.steps) {
        if (!step.isCompleted()) {
          step.execute();
        }
      }
    } catch (Throwable t) {
      log.error("Commit failed for dataset " + this.datasetUrn, t);
      throw Throwables.propagate(t);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }
}
