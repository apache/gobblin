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
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;

import com.google.common.base.Optional;
import com.google.common.eventbus.EventBus;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.runtime.api.FsSpecConsumer;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.MutableJobCatalog;
import org.apache.gobblin.runtime.api.SpecConsumer;
import org.apache.gobblin.runtime.api.SpecExecutor;
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
  private final Optional<MutableJobCatalog> jobCatalogOptional;
  private final SpecConsumer specConsumer;

  public FsJobConfigurationManager(EventBus eventBus, Config config, FileSystem fs) {
    this(eventBus, config, null, fs);
  }

  public FsJobConfigurationManager(EventBus eventBus, Config config, MutableJobCatalog jobCatalog, FileSystem fs) {
    super(eventBus, config);
    this.jobCatalogOptional = jobCatalog != null ? Optional.of(jobCatalog) : Optional.absent();
    this.refreshIntervalInSeconds = ConfigUtils.getLong(config, GobblinClusterConfigurationKeys.JOB_SPEC_REFRESH_INTERVAL,
        DEFAULT_JOB_SPEC_REFRESH_INTERVAL);

    this.fetchJobSpecExecutor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(log), Optional.of("FetchJobSpecExecutor")));
    this.specConsumer = new FsSpecConsumer(fs, config);
  }

  protected void startUp() throws Exception {
    super.startUp();
    // Schedule the job config fetch task
    this.fetchJobSpecExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          fetchJobSpecs();
        } catch (Exception e) {
          //Log error and swallow exception to allow executor service to continue scheduling the thread
          log.error("Failed to fetch job specs due to: ", e);
        }
      }
    }, 0, this.refreshIntervalInSeconds, TimeUnit.SECONDS);
  }

  @Override
  protected void shutDown() throws Exception {
    ExecutorsUtils.shutdownExecutorService(this.fetchJobSpecExecutor, Optional.of(log));
    super.shutDown();
  }

  void fetchJobSpecs() throws ExecutionException, InterruptedException {
    List<Pair<SpecExecutor.Verb, JobSpec>> jobSpecs =
        (List<Pair<SpecExecutor.Verb, JobSpec>>) this.specConsumer.changedSpecs().get();

    log.info("Fetched {} job specs", jobSpecs.size());
    for (Pair<SpecExecutor.Verb, JobSpec> entry : jobSpecs) {
      JobSpec jobSpec = entry.getValue();
      SpecExecutor.Verb verb = entry.getKey();
      if (verb.equals(SpecExecutor.Verb.ADD)) {
        // Handle addition
        if (this.jobCatalogOptional.isPresent()) {
          this.jobCatalogOptional.get().put(jobSpec);
        }
        postNewJobConfigArrival(jobSpec.getUri().toString(), jobSpec.getConfigAsProperties());
      } else if (verb.equals(SpecExecutor.Verb.UPDATE)) {
        //Handle update.
        if (this.jobCatalogOptional.isPresent()) {
          this.jobCatalogOptional.get().put(jobSpec);
        }
        postUpdateJobConfigArrival(jobSpec.getUri().toString(), jobSpec.getConfigAsProperties());
      } else if (verb.equals(SpecExecutor.Verb.DELETE)) {
        // Handle delete
        if (this.jobCatalogOptional.isPresent()) {
          this.jobCatalogOptional.get().remove(jobSpec.getUri());
        }
        postDeleteJobConfigArrival(jobSpec.getUri().toString(), jobSpec.getConfigAsProperties());
      } else if (verb.equals(SpecExecutor.Verb.CANCEL)) {
          // Handle cancel
          postCancelJobConfigArrival(jobSpec.getUri().toString());
      }

      try {
        //Acknowledge the successful consumption of the JobSpec back to the SpecConsumer, so that the
        //SpecConsumer can delete the JobSpec.
        this.specConsumer.commit(jobSpec);
      } catch (IOException e) {
        log.error("Error when committing to FsSpecConsumer: ", e);
      }
    }
  }
}
