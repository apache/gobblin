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

package org.apache.gobblin.cluster;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.typesafe.config.Config;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.runtime.api.SpecConsumer;
import org.apache.gobblin.runtime.api.SpecExecutor;


@Alpha
public class ScheduledJobConfigurationManager extends JobConfigurationManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ScheduledJobConfigurationManager.class);

  private static final long DEFAULT_JOB_SPEC_REFRESH_INTERVAL = 60;

  private Map<URI, JobSpec> jobSpecs;

  private final long refreshIntervalInSeconds;

  private final ScheduledExecutorService fetchJobSpecExecutor;

  private final SpecConsumer _specConsumer;

  private final ClassAliasResolver<SpecConsumer> aliasResolver;

  public ScheduledJobConfigurationManager(EventBus eventBus, Config config) {
    super(eventBus, config);

    this.jobSpecs = Maps.newHashMap();
    this.refreshIntervalInSeconds = ConfigUtils.getLong(config, GobblinClusterConfigurationKeys.JOB_SPEC_REFRESH_INTERVAL,
        DEFAULT_JOB_SPEC_REFRESH_INTERVAL);

    this.fetchJobSpecExecutor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("FetchJobSpecExecutor")));

    this.aliasResolver = new ClassAliasResolver<>(SpecConsumer.class);
    try {
      String specConsumerClassName = GobblinClusterConfigurationKeys.DEFAULT_SPEC_CONSUMER_CLASS;
      if (config.hasPath(GobblinClusterConfigurationKeys.SPEC_CONSUMER_CLASS_KEY)) {
        specConsumerClassName = config.getString(GobblinClusterConfigurationKeys.SPEC_CONSUMER_CLASS_KEY);
      }
      LOGGER.info("Using SpecConsumer ClassNameclass name/alias " + specConsumerClassName);
      this._specConsumer = (SpecConsumer) ConstructorUtils
          .invokeConstructor(Class.forName(this.aliasResolver.resolve(specConsumerClassName)), config);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
        | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void startUp() throws Exception {
    LOGGER.info("Starting the " + ScheduledJobConfigurationManager.class.getSimpleName());

    LOGGER.info(String.format("Scheduling the job spec refresh task with an interval of %d second(s)",
        this.refreshIntervalInSeconds));

    // Schedule the job config fetch task
    this.fetchJobSpecExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          fetchJobSpecs();
        } catch (InterruptedException | ExecutionException e) {
          LOGGER.error("Failed to fetch job specs", e);
          throw new RuntimeException("Failed to fetch specs", e);
        }
      }
    }, 0, this.refreshIntervalInSeconds, TimeUnit.SECONDS);
  }

  /***
   * TODO: Change cluster code to handle Spec. Right now all job properties are needed to be in config and template is not honored
   * TODO: Materialized JobSpec and make use of ResolvedJobSpec
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private void fetchJobSpecs() throws ExecutionException, InterruptedException {
    List<Pair<SpecExecutor.Verb, Spec>> changesSpecs =
        (List<Pair<SpecExecutor.Verb, Spec>>) this._specConsumer.changedSpecs().get();

    for (Pair<SpecExecutor.Verb, Spec> entry : changesSpecs) {

      SpecExecutor.Verb verb = entry.getKey();
      if (verb.equals(SpecExecutor.Verb.ADD)) {

        // Handle addition
        JobSpec jobSpec = (JobSpec) entry.getValue();
        postNewJobConfigArrival(jobSpec.getUri().toString(), jobSpec.getConfigAsProperties());
        jobSpecs.put(entry.getValue().getUri(), (JobSpec) entry.getValue());
      } else if (verb.equals(SpecExecutor.Verb.UPDATE)) {

        // Handle update
        JobSpec jobSpec = (JobSpec) entry.getValue();
        postUpdateJobConfigArrival(jobSpec.getUri().toString(), jobSpec.getConfigAsProperties());
        jobSpecs.put(entry.getValue().getUri(), (JobSpec) entry.getValue());
      } else if (verb.equals(SpecExecutor.Verb.DELETE)) {

        // Handle delete
        Spec anonymousSpec = (Spec) entry.getValue();
        postDeleteJobConfigArrival(anonymousSpec.getUri().toString(), new Properties());
        jobSpecs.remove(entry.getValue().getUri());
      }
    }
  }

  @Override
  protected void shutDown() throws Exception {
    ExecutorsUtils.shutdownExecutorService(this.fetchJobSpecExecutor, Optional.of(LOGGER));
  }
}