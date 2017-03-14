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

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.typesafe.config.Config;

import gobblin.annotation.Alpha;
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.Spec;
import gobblin.runtime.api.SpecExecutorInstance;
import gobblin.runtime.api.SpecExecutorInstanceConsumer;
import gobblin.util.ClassAliasResolver;
import gobblin.util.ConfigUtils;
import gobblin.util.ExecutorsUtils;

/**
 * A {@link JobConfigurationManager} that fetches job specs from a {@link SpecExecutorInstanceConsumer} in a loop
 * without
 */
@Alpha
public class StreamingJobConfigurationManager extends JobConfigurationManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamingJobConfigurationManager.class);

  private final ExecutorService fetchJobSpecExecutor;

  private final SpecExecutorInstanceConsumer specExecutorInstanceConsumer;

  public StreamingJobConfigurationManager(EventBus eventBus, Config config) {
    super(eventBus, config);

    this.fetchJobSpecExecutor = Executors.newSingleThreadExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("FetchJobSpecExecutor")));

    String specExecutorInstanceConsumerClassName =
        ConfigUtils.getString(config, GobblinClusterConfigurationKeys.SPEC_EXECUTOR_INSTANCE_CONSUMER_CLASS_KEY,
            GobblinClusterConfigurationKeys.DEFAULT_STREAMING_SPEC_EXECUTOR_INSTANCE_CONSUMER_CLASS);

    LOGGER.info("Using SpecExecutorInstanceConsumer ClassNameclass name/alias " +
        specExecutorInstanceConsumerClassName);

    try {
      ClassAliasResolver<SpecExecutorInstanceConsumer> aliasResolver =
          new ClassAliasResolver<>(SpecExecutorInstanceConsumer.class);

      this.specExecutorInstanceConsumer = (SpecExecutorInstanceConsumer) ConstructorUtils
          .invokeConstructor(Class.forName(aliasResolver.resolve(specExecutorInstanceConsumerClassName)), config);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
          | ClassNotFoundException e) {
      throw new RuntimeException("Could not construct SpecExecutorInstanceConsumer " +
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
  }

  private void fetchJobSpecs() throws ExecutionException, InterruptedException {
    List<Pair<SpecExecutorInstance.Verb, Spec>> changesSpecs =
        (List<Pair<SpecExecutorInstance.Verb, Spec>>) this.specExecutorInstanceConsumer.changedSpecs().get();

    for (Pair<SpecExecutorInstance.Verb, Spec> entry : changesSpecs) {
      SpecExecutorInstance.Verb verb = entry.getKey();
      if (verb.equals(SpecExecutorInstance.Verb.ADD)) {
        // Handle addition
        JobSpec jobSpec = (JobSpec) entry.getValue();
        postNewJobConfigArrival(jobSpec.getUri().toString(), jobSpec.getConfigAsProperties());
      } else if (verb.equals(SpecExecutorInstanceConsumer.Verb.UPDATE)) {
        // Handle update
        JobSpec jobSpec = (JobSpec) entry.getValue();
        postUpdateJobConfigArrival(jobSpec.getUri().toString(), jobSpec.getConfigAsProperties());
      } else if (verb.equals(SpecExecutorInstanceConsumer.Verb.DELETE)) {
        // Handle delete
        Spec anonymousSpec = (Spec) entry.getValue();
        postDeleteJobConfigArrival(anonymousSpec.getUri().toString(), new Properties());
      }
    }
  }

  @Override
  protected void shutDown() throws Exception {
    ExecutorsUtils.shutdownExecutorService(this.fetchJobSpecExecutor, Optional.of(LOGGER));
  }
}
