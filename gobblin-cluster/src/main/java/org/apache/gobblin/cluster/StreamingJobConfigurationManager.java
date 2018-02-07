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
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.Service;
import com.typesafe.config.Config;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.MutableJobCatalog;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;
import org.apache.gobblin.runtime.api.SpecConsumer;

import lombok.Getter;


/**
 * A {@link JobConfigurationManager} that fetches job specs from a {@link SpecConsumer} in a loop
 * without
 */
@Alpha
public class StreamingJobConfigurationManager extends JobConfigurationManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamingJobConfigurationManager.class);

  private final ExecutorService fetchJobSpecExecutor;

  @Getter
  private final SpecConsumer specConsumer;

  private final long stopTimeoutSeconds;

  public StreamingJobConfigurationManager(EventBus eventBus, Config config, MutableJobCatalog jobCatalog) {
    super(eventBus, config);

    this.stopTimeoutSeconds = ConfigUtils.getLong(config, GobblinClusterConfigurationKeys.STOP_TIMEOUT_SECONDS,
        GobblinClusterConfigurationKeys.DEFAULT_STOP_TIMEOUT_SECONDS);

    this.fetchJobSpecExecutor = Executors.newSingleThreadExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("FetchJobSpecExecutor")));

    String specExecutorInstanceConsumerClassName =
        ConfigUtils.getString(config, GobblinClusterConfigurationKeys.SPEC_CONSUMER_CLASS_KEY,
            GobblinClusterConfigurationKeys.DEFAULT_STREAMING_SPEC_CONSUMER_CLASS);

    LOGGER.info("Using SpecConsumer ClassNameclass name/alias " +
        specExecutorInstanceConsumerClassName);

    try {
      ClassAliasResolver<SpecConsumer> aliasResolver =
          new ClassAliasResolver<>(SpecConsumer.class);

      this.specConsumer = (SpecConsumer) GobblinConstructorUtils.invokeFirstConstructor(
          Class.forName(aliasResolver.resolve(specExecutorInstanceConsumerClassName)),
          ImmutableList.<Object>of(config, jobCatalog),
          ImmutableList.<Object>of(config));
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
        | ClassNotFoundException e) {
      throw new RuntimeException("Could not construct SpecConsumer " +
          specExecutorInstanceConsumerClassName, e);
    }
  }

  @Override
  protected void startUp() throws Exception {
    LOGGER.info("Starting the " + StreamingJobConfigurationManager.class.getSimpleName());

    // submit command to fetch job specs
    this.fetchJobSpecExecutor.execute(new Runnable() {
      @Override
      public void run() {
        try {
          while(true) {
            fetchJobSpecs();
          }
        } catch (InterruptedException e) {
          LOGGER.info("Fetch thread interrupted... will exit");
        } catch (ExecutionException e) {
          LOGGER.error("Failed to fetch job specs", e);
          throw new RuntimeException("Failed to fetch specs", e);
        }
      }
    });

    // if the instance consumer is a service then need to start it to consume job specs
    // IMPORTANT: StreamingKafkaSpecConsumer needs to be launched after a fetching thread is created.
    //            This is because StreamingKafkaSpecConsumer will invoke addListener(new JobSpecListener()) during startup,
    //            which will push job specs into a blocking queue _jobSpecQueue. A fetching thread will help to consume the
    //            blocking queue to prevent a hanging issue.
    if (this.specConsumer instanceof Service) {
      ((Service) this.specConsumer).startAsync().awaitRunning();
    }
  }

  private void fetchJobSpecs() throws ExecutionException, InterruptedException {
    List<Pair<SpecExecutor.Verb, Spec>> changesSpecs =
        (List<Pair<SpecExecutor.Verb, Spec>>) this.specConsumer.changedSpecs().get();

    // propagate thread interruption so that caller will exit from loop
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }

    for (Pair<SpecExecutor.Verb, Spec> entry : changesSpecs) {
      SpecExecutor.Verb verb = entry.getKey();
      if (verb.equals(SpecExecutor.Verb.ADD)) {
        // Handle addition
        JobSpec jobSpec = (JobSpec) entry.getValue();
        postNewJobConfigArrival(jobSpec.getUri().toString(), jobSpec.getConfigAsProperties());
      } else if (verb.equals(SpecExecutor.Verb.UPDATE)) {
        // Handle update
        JobSpec jobSpec = (JobSpec) entry.getValue();
        postUpdateJobConfigArrival(jobSpec.getUri().toString(), jobSpec.getConfigAsProperties());
      } else if (verb.equals(SpecExecutor.Verb.DELETE)) {
        // Handle delete
        Spec anonymousSpec = (Spec) entry.getValue();
        postDeleteJobConfigArrival(anonymousSpec.getUri().toString(), new Properties());
      }
    }
  }

  @Override
  protected void shutDown() throws Exception {
    if (this.specConsumer instanceof Service) {
      ((Service) this.specConsumer).stopAsync().awaitTerminated(this.stopTimeoutSeconds,
          TimeUnit.SECONDS);
    }

    ExecutorsUtils.shutdownExecutorService(this.fetchJobSpecExecutor, Optional.of(LOGGER));
  }
}