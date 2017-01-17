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

package gobblin.cluster;

import java.util.List;
import java.util.concurrent.Callable;

import com.google.common.collect.ImmutableList;

import gobblin.annotation.Alpha;
import gobblin.configuration.State;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.Tag;


/**
 * Extension of {@link GobblinMetrics} specifically for {@link GobblinTaskRunner}s.
 */
@Alpha
public class ContainerMetrics extends GobblinMetrics {

  protected ContainerMetrics(State containerState, String applicationName, String taskRunnerId) {
    super(name(taskRunnerId), null, tagsForContainer(containerState, applicationName, taskRunnerId));
  }

  /**
   * Get a {@link ContainerMetrics} instance given the {@link State} of a container, the name of the application the
   * container belongs to, and the workerId of the container.
   *
   * @param containerState the {@link State} of the container
   * @param applicationName a {@link String} representing the name of the application the container belongs to
   * @return a {@link ContainerMetrics} instance
   */
  public static ContainerMetrics get(final State containerState, final String applicationName,
      final String workerId) {
    return (ContainerMetrics) GOBBLIN_METRICS_REGISTRY.getOrDefault(name(workerId), new Callable<GobblinMetrics>() {
      @Override public GobblinMetrics call() throws Exception {
        return new ContainerMetrics(containerState, applicationName, workerId);
      }
    });
  }

  private static String name(String workerId) {
    return "gobblin.metrics." + workerId;
  }

  private static List<Tag<?>> tagsForContainer(State containerState, String applicationName, String taskRunnerId) {
    ImmutableList.Builder<Tag<?>> tags = new ImmutableList.Builder<>();
    tags.add(new Tag<>(GobblinClusterMetricTagNames.APPLICATION_NAME, applicationName));
    tags.add(new Tag<>(GobblinClusterMetricTagNames.TASK_RUNNER_ID, taskRunnerId));
    tags.addAll(getCustomTagsFromState(containerState));
    return tags.build();
  }
}
