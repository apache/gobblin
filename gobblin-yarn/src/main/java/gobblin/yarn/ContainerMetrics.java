/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.yarn;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ContainerId;

import com.google.common.collect.ImmutableList;

import gobblin.configuration.State;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.Tag;


/**
 * Extension of {@link GobblinMetrics} specifically for YARN containers.
 */
public class ContainerMetrics extends GobblinMetrics {

  private static final String APPLICATION_NAME = "applicationName";
  private static final String APPLICATION_ID = "applicationId";
  private static final String APPLICATION_ATTEMPT_ID = "applicationAttemptId";
  private static final String CONTAINER_ID = "containerId";

  protected ContainerMetrics(State containerState, String applicationName, ContainerId containerId) {
    super(name(containerId), null, tagsForContainer(containerState, applicationName, containerId));
  }

  /**
   * Get a {@link ContainerMetrics} instance given the {@link State} of a container, the name of the application the
   * container belongs to, and the {@link ContainerId} of the container.
   *
   * @param containerState the {@link State} of the container
   * @param applicationName a {@link String} representing the name of the application the container belongs to
   * @param containerId the {@link ContainerId} of the container
   * @return a {@link ContainerMetrics} instance
   */
  public static ContainerMetrics get(State containerState, String applicationName, ContainerId containerId) {
    return (ContainerMetrics) GOBBLIN_METRICS_REGISTRY
        .getOrDefault(name(containerId), new ContainerMetrics(containerState, applicationName, containerId));
  }

  private static String name(ContainerId containerId) {
    return "gobblin.metrics." + containerId.toString();
  }

  private static List<Tag<?>> tagsForContainer(State containerState, String applicationName, ContainerId containerId) {
    ImmutableList.Builder<Tag<?>> tags = new ImmutableList.Builder<>();
    tags.add(new Tag<>(APPLICATION_NAME, applicationName));
    tags.add(new Tag<>(APPLICATION_ID, containerId.getApplicationAttemptId().getApplicationId().toString()));
    tags.add(new Tag<>(APPLICATION_ATTEMPT_ID, containerId.getApplicationAttemptId().getAttemptId()));
    tags.add(new Tag<>(CONTAINER_ID, containerId.toString()));
    tags.addAll(getCustomTagsFromState(containerState));
    return tags.build();
  }
}
