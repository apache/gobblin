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


package org.apache.gobblin.runtime.util;

import java.util.List;
import java.util.concurrent.Callable;

import com.google.common.collect.ImmutableList;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.runtime.fork.Fork;

/**
 * An extension to {@link GobblinMetrics} specifically for {@link Fork}.
 */
public class ForkMetrics extends GobblinMetrics {
  private static final String FORK_METRICS_BRANCH_NAME_KEY = "forkBranchName";

  protected ForkMetrics(TaskState taskState, int index) {
    super(name(taskState, index), TaskMetrics.get(taskState).getMetricContext(), getForkMetricsTags(taskState, index));
  }

  public static ForkMetrics get(final TaskState taskState, int index) {
    return (ForkMetrics) GOBBLIN_METRICS_REGISTRY.getOrCreate(name(taskState, index), new Callable<GobblinMetrics>() {
      @Override
      public GobblinMetrics call() throws Exception {
        return new ForkMetrics(taskState, index);
      }
    });
  }

  /**
   * Creates a unique {@link String} representing this branch.
   */
  private static String getForkMetricsId(State state, int index) {
    return state.getProp(ConfigurationKeys.FORK_BRANCH_NAME_KEY + "." + index,
        ConfigurationKeys.DEFAULT_FORK_BRANCH_NAME + index);
  }

  /**
   * Creates a {@link List} of {@link Tag}s for a {@link Fork} instance. The {@link Tag}s are purely based on the
   * index and the branch name.
   */
  private static List<Tag<?>> getForkMetricsTags(State state, int index) {
    return ImmutableList.<Tag<?>>of(new Tag<>(FORK_METRICS_BRANCH_NAME_KEY, getForkMetricsId(state, index)));
  }

  /**
   * Creates a {@link String} that is a concatenation of the {@link TaskMetrics#getName()} and
   * {@link #getForkMetricsId(State, int)}.
   */
  protected static String name(TaskState taskState, int index) {
    return METRICS_ID_PREFIX + taskState.getJobId() + "." + taskState.getTaskId() + "." + getForkMetricsId(taskState, index);
  }
}
