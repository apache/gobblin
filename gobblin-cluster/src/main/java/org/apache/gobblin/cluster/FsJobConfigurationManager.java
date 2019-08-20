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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Optional;
import com.google.common.eventbus.EventBus;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.runtime.api.FsSpecConsumer;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.MutableJobCatalog;
import org.apache.gobblin.runtime.api.SpecConsumer;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.ExecutorsUtils;


/**
 * A {@link JobConfigurationManager} that reads {@link JobSpec}s from a source path on a
 * {@link org.apache.hadoop.fs.FileSystem} and posts them to an {@link EventBus}.
 * The {@link FsJobConfigurationManager} has an underlying {@link FsSpecConsumer} that periodically reads the
 * {@link JobSpec}s from the filesystem and posts an appropriate JobConfigArrivalEvent with the job configuration to
 * the EventBus for consumption by the listeners.
 */
@Slf4j
public class FsJobConfigurationManager extends JobConfigurationManager {
  private static final long DEFAULT_JOB_SPEC_REFRESH_INTERVAL = 60;

  private final long refreshIntervalInSeconds;

  private final ScheduledExecutorService fetchJobSpecExecutor;

  protected final SpecConsumer _specConsumer;

  private final ClassAliasResolver<SpecConsumer> aliasResolver;

  private final Optional<MutableJobCatalog> _jobCatalogOptional;

  public FsJobConfigurationManager(EventBus eventBus, Config config) {
    this(eventBus, config, null);
  }

  public FsJobConfigurationManager(EventBus eventBus, Config config, MutableJobCatalog jobCatalog) {
    super(eventBus, config);
    this._jobCatalogOptional = jobCatalog != null ? Optional.of(jobCatalog) : Optional.absent();
    this.refreshIntervalInSeconds = ConfigUtils.getLong(config, GobblinClusterConfigurationKeys.JOB_SPEC_REFRESH_INTERVAL,
        DEFAULT_JOB_SPEC_REFRESH_INTERVAL);

    this.fetchJobSpecExecutor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(log), Optional.of("FetchJobSpecExecutor")));

    this.aliasResolver = new ClassAliasResolver<>(SpecConsumer.class);
    try {
      String specConsumerClassName = ConfigUtils.getString(config, GobblinClusterConfigurationKeys.SPEC_CONSUMER_CLASS_KEY,
          GobblinClusterConfigurationKeys.DEFAULT_SPEC_CONSUMER_CLASS);
      log.info("Using SpecConsumer ClassNameclass name/alias " + specConsumerClassName);
      this._specConsumer = (SpecConsumer) ConstructorUtils
          .invokeConstructor(Class.forName(this.aliasResolver.resolve(specConsumerClassName)), config);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
        | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  protected void startUp() throws Exception{
    super.startUp();
    // Schedule the job config fetch task
    this.fetchJobSpecExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          fetchJobSpecs();
        } catch (InterruptedException | ExecutionException e) {
          log.error("Failed to fetch job specs", e);
          throw new RuntimeException("Failed to fetch specs", e);
        }
      }
    }, 0, this.refreshIntervalInSeconds, TimeUnit.SECONDS);
  }

  void fetchJobSpecs() throws ExecutionException, InterruptedException {
    List<Pair<SpecExecutor.Verb, JobSpec>> jobSpecs =
        (List<Pair<SpecExecutor.Verb, JobSpec>>) this._specConsumer.changedSpecs().get();

    for (Pair<SpecExecutor.Verb, JobSpec> entry : jobSpecs) {
      JobSpec jobSpec = entry.getValue();
      SpecExecutor.Verb verb = entry.getKey();
      if (verb.equals(SpecExecutor.Verb.ADD)) {
        // Handle addition
        if (this._jobCatalogOptional.isPresent()) {
          this._jobCatalogOptional.get().put(jobSpec);
        }
        postNewJobConfigArrival(jobSpec.getUri().toString(), jobSpec.getConfigAsProperties());
      } else if (verb.equals(SpecExecutor.Verb.UPDATE)) {
        //Handle update.
        if (this._jobCatalogOptional.isPresent()) {
          this._jobCatalogOptional.get().put(jobSpec);
        }
        postUpdateJobConfigArrival(jobSpec.getUri().toString(), jobSpec.getConfigAsProperties());
      } else if (verb.equals(SpecExecutor.Verb.DELETE)) {
        // Handle delete
        if (this._jobCatalogOptional.isPresent()) {
          this._jobCatalogOptional.get().remove(jobSpec.getUri());
        }
        postDeleteJobConfigArrival(jobSpec.getUri().toString(), jobSpec.getConfigAsProperties());
      }

      try {
        //Acknowledge the successful consumption of the JobSpec back to the SpecConsumer, so that the
        //SpecConsumer can delete the JobSpec.
        this._specConsumer.commit(jobSpec);
      } catch (IOException e) {
        log.error("Error when committing to FsSpecConsumer: ", e);
      }
    }
  }
}
