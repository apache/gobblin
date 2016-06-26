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

package gobblin.aws;

import java.nio.file.FileSystem;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;

import gobblin.cluster.GobblinClusterMetricTagNames;
import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.Tag;
import gobblin.metrics.event.EventSubmitter;
import gobblin.util.ConfigUtils;
import gobblin.util.ExecutorsUtils;


/**
 * This class is responsible for all AWS-related operations including ApplicationMaster launch,
 * EC2 workers management, etc.
 *
 * @author Abhishek Tiwari
 */
public class AWSService extends AbstractIdleService {

  private static final Logger LOGGER = LoggerFactory.getLogger(AWSService.class);

  private static final Splitter SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

  private final String applicationName;
  private final String applicationId;

  private final Config config;

  private final EventBus eventBus;

  private final FileSystem fs;

  private final Optional<GobblinMetrics> gobblinMetrics;
  private final Optional<EventSubmitter> eventSubmitter;

  private final ExecutorService containerLaunchExecutor;

  private final int helixInstanceMaxRetries;

  private final Optional<String> containerJvmArgs;

  private final Closer closer = Closer.create();

  // A generator for an integer ID of a Helix instance (participant)
  private final AtomicInteger helixInstanceIdGenerator = new AtomicInteger(0);

  // A map from Helix instance names to the number times the instances are retried to be started
  private final ConcurrentMap<String, AtomicInteger> helixInstanceRetryCount = Maps.newConcurrentMap();

  // A queue of unused Helix instance names. An unused Helix instance name gets put
  // into the queue if the container running the instance completes. Unused Helix
  // instance names get picked up when replacement containers get allocated.
  private final ConcurrentLinkedQueue<String> unusedHelixInstanceNames = Queues.newConcurrentLinkedQueue();

  private volatile boolean shutdownInProgress = false;

  public AWSService(Config config, String applicationName, String applicationId,
      FileSystem fs, EventBus eventBus) throws Exception {
    this.applicationName = applicationName;
    this.applicationId = applicationId;

    this.config = config;

    this.eventBus = eventBus;

    this.gobblinMetrics = config.getBoolean(ConfigurationKeys.METRICS_ENABLED_KEY) ?
        Optional.of(buildGobblinMetrics()) : Optional.<GobblinMetrics>absent();

    this.eventSubmitter = config.getBoolean(ConfigurationKeys.METRICS_ENABLED_KEY) ?
        Optional.of(buildEventSubmitter()) : Optional.<EventSubmitter>absent();

    this.fs = fs;

    this.helixInstanceMaxRetries = config.getInt(GobblinAWSConfigurationKeys.HELIX_INSTANCE_MAX_RETRIES);

    this.containerJvmArgs = config.hasPath(GobblinAWSConfigurationKeys.WORKER_JVM_ARGS_KEY) ?
        Optional.of(config.getString(GobblinAWSConfigurationKeys.WORKER_JVM_ARGS_KEY)) :
        Optional.<String>absent();

    this.containerLaunchExecutor = Executors.newFixedThreadPool(10,
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("ContainerLaunchExecutor")));
  }

  @Override
  protected void startUp()
      throws Exception {
    LOGGER.info("Starting the AWSService");

    // Register itself with the EventBus for container-related requests
    this.eventBus.register(this);

    // TODO: Start framework callbacks

    LOGGER.info("Requesting initial containers");
  }

  @Override
  protected void shutDown()
      throws Exception {

    LOGGER.info("Stopping the AWSService");

    this.shutdownInProgress = true;

    try {
      ExecutorsUtils.shutdownExecutorService(this.containerLaunchExecutor, Optional.of(LOGGER));

      // Stop the running containers
      // TODO: Clean shutdown

    } catch (Exception e) {
      LOGGER.error("Failed to shutdown the ApplicationMaster", e);
    } finally {
      try {
        this.closer.close();
      } finally {
        if (this.gobblinMetrics.isPresent()) {
          this.gobblinMetrics.get().stopMetricsReporting();
        }
      }
    }
  }

  private GobblinMetrics buildGobblinMetrics() {
    // Create tags list
    ImmutableList.Builder<Tag<?>> tags = new ImmutableList.Builder<>();
    tags.add(new Tag<>(GobblinClusterMetricTagNames.APPLICATION_ID, this.applicationId));
    tags.add(new Tag<>(GobblinClusterMetricTagNames.APPLICATION_NAME, this.applicationName));

    // Intialize Gobblin metrics and start reporters
    GobblinMetrics gobblinMetrics = GobblinMetrics.get(this.applicationId, null, tags.build());
    gobblinMetrics.startMetricReporting(ConfigUtils.configToProperties(config));

    return gobblinMetrics;
  }

  private EventSubmitter buildEventSubmitter() {
    return new EventSubmitter.Builder(this.gobblinMetrics.get().getMetricContext(),
        GobblinAWSEventConstants.EVENT_NAMESPACE)
        .build();
  }

  private void requestInitialContainers(int containersRequested) {
    for (int i = 0; i < containersRequested; i++) {
      requestContainer(Optional.<String>absent());
    }
  }

  private void requestContainer(Optional<String> preferredNode) {
    // TODO: Request containers with required configuration
  }
}
