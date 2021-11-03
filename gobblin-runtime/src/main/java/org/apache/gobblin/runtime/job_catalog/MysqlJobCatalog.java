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
package org.apache.gobblin.runtime.job_catalog;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.typesafe.config.Config;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.runtime.api.GobblinInstanceEnvironment;
import org.apache.gobblin.runtime.api.JobCatalog;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.JobSpecNotFoundException;
import org.apache.gobblin.runtime.api.MutableJobCatalog;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.spec_serde.GsonJobSpecSerDe;
import org.apache.gobblin.runtime.spec_store.MysqlBaseSpecStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * MySQL-backed Job Catalog for persisting {@link JobSpec} job configuration information, which fully supports (mutation)
 * listeners and metrics.
 */
public class MysqlJobCatalog extends JobCatalogBase implements MutableJobCatalog {

  private static final Logger LOGGER = LoggerFactory.getLogger(MysqlJobCatalog.class);
  public static final String DB_CONFIG_PREFIX = "mysqlJobCatalog";

  protected final MutableJobCatalog.MutableStandardMetrics mutableMetrics;
  protected final MysqlBaseSpecStore jobSpecStore;

  /**
   * Initialize, with DB config contextualized by `DB_CONFIG_PREFIX`.
   */
  public MysqlJobCatalog(Config sysConfig)
      throws IOException {
      this(sysConfig, Optional.<MetricContext>absent(), GobblinMetrics.isEnabled(sysConfig));
  }

  public MysqlJobCatalog(GobblinInstanceEnvironment env) throws IOException {
    super(env);
    this.mutableMetrics = (MutableJobCatalog.MutableStandardMetrics) metrics;
    this.jobSpecStore = createJobSpecStore(env.getSysConfig().getConfig());
  }

  public MysqlJobCatalog(Config sysConfig, Optional<MetricContext> parentMetricContext,
      boolean instrumentationEnabled) throws IOException {
    super(Optional.of(LOGGER), parentMetricContext, instrumentationEnabled, Optional.of(sysConfig));
    this.mutableMetrics = (MutableJobCatalog.MutableStandardMetrics) metrics;
    this.jobSpecStore = createJobSpecStore(sysConfig);
  }

  @Override
  protected JobCatalog.StandardMetrics createStandardMetrics(Optional<Config> sysConfig) {
    log.info("create standard metrics {} for {}", MutableJobCatalog.MutableStandardMetrics.class.getName(), this.getClass().getName());
    return new MutableJobCatalog.MutableStandardMetrics(this, sysConfig);
  }

  protected MysqlBaseSpecStore createJobSpecStore(Config sysConfig) {
    try {
      return new MysqlBaseSpecStore(sysConfig, new GsonJobSpecSerDe()) {
        @Override
        protected String getConfigPrefix() {
          return MysqlJobCatalog.DB_CONFIG_PREFIX;
        }
      };
    } catch (IOException e) {
      throw new RuntimeException("unable to create `JobSpec` store", e);
    }
  }

  /** @return all {@link JobSpec}s */
  @Override
  public List<JobSpec> getJobs() {
    try {
      return (List) jobSpecStore.getSpecs();
    } catch (IOException e) {
      throw new RuntimeException("error getting (all) job specs", e);
    }
  }

  /**
   * Obtain an iterator to fetch all job specifications. Unlike {@link #getJobs()}, this method avoids loading
   * all job configs into memory in the very beginning.
   * Interleaving notes: jobs added/modified/deleted between `Iterator` creation and exhaustion MAY or MAY NOT be reflected.
   *
   * @return an iterator for (all) present {@link JobSpec}s
   */
  @Override
  public Iterator<JobSpec> getJobSpecIterator() {
    try {
      return Iterators.<Optional<JobSpec>, JobSpec>transform(
          Iterators.filter(
              Iterators.transform(jobSpecStore.getSpecURIs(), uri -> {
                try {
                  return Optional.of(MysqlJobCatalog.this.getJobSpec(uri));
                } catch (JobSpecNotFoundException e) {
                  MysqlJobCatalog.this.log.info("unable to retrieve previously identified JobSpec by URI '{}'", uri);
                  return Optional.absent();
                }}),
              Optional::isPresent),
          Optional::get);
    } catch (IOException e) {
      throw new RuntimeException("error iterating (all) job specs", e);
    }
  }

  /**
   * Fetch single {@link JobSpec} by URI.
   * @return the `JobSpec`
   * @throws {@link JobSpecNotFoundException}
   */
  @Override
  public JobSpec getJobSpec(URI uri)
      throws JobSpecNotFoundException {
    Preconditions.checkNotNull(uri);
    try {
      return (JobSpec) jobSpecStore.getSpec(uri);
    } catch (IOException e) {
      throw new RuntimeException(String.format("error accessing job spec '%s'", uri), e);
    } catch (SpecNotFoundException e) {
      throw new JobSpecNotFoundException(uri);
    }
  }

  /**
   * Add or update (when an existing) {@link JobSpec}, triggering the appropriate
   * {@link org.apache.gobblin.runtime.api.JobCatalogListener} callback.
   *
   * NOTE: `synchronized` (w/ `remove()`) for integrity of (existence) check-then-update.
   */
  @Override
  public synchronized void put(JobSpec jobSpec) {
    Preconditions.checkState(state() == State.RUNNING, String.format("%s is not running.", this.getClass().getName()));
    Preconditions.checkNotNull(jobSpec);
    try {
      long startTime = System.currentTimeMillis();
      boolean isUpdate = jobSpecStore.exists(jobSpec.getUri());
      if (isUpdate) {
        try {
          jobSpecStore.updateSpec(jobSpec);
          this.mutableMetrics.updatePutJobTime(startTime);
          this.listeners.onUpdateJob(jobSpec);
        } catch (SpecNotFoundException e) { // should never happen (since `synchronized`)
          throw new RuntimeException(String.format("error finding spec to update '%s'", jobSpec.getUri()), e);
        }
      } else {
        jobSpecStore.addSpec(jobSpec);
        this.mutableMetrics.updatePutJobTime(startTime);
        this.listeners.onAddJob(jobSpec);
      }
    } catch (IOException e) {
      throw new RuntimeException(String.format("error updating or adding JobSpec '%s'", jobSpec.getUri()), e);
    }
  }

  /**
   * Delete (an existing) {@link JobSpec}, triggering the appropriate {@link org.apache.gobblin.runtime.api.JobCatalogListener} callback.
   *
   * NOTE: `synchronized` w/ `put()` to protect its check-then-update.
   */
  @Override
  public synchronized void remove(URI jobURI) {
    remove(jobURI, false);
  }

  /**
   * NOTE: `synchronized` w/ `put()` to protect its check-then-update.
   *
   * @param alwaysTriggerListeners whether invariably to trigger {@link org.apache.gobblin.runtime.api.JobCatalogListener#onCancelJob(URI)}
   */
  @Override
  public synchronized void remove(URI jobURI, boolean alwaysTriggerListeners) {
    Preconditions.checkState(state() == State.RUNNING, String.format("%s is not running.", this.getClass().getName()));
    Preconditions.checkNotNull(jobURI);
    try {
      long startTime = System.currentTimeMillis();
      JobSpec jobSpec = (JobSpec) jobSpecStore.getSpec(jobURI);
      jobSpecStore.deleteSpec(jobURI);
      this.mutableMetrics.updateRemoveJobTime(startTime);
      this.listeners.onDeleteJob(jobURI, jobSpec.getVersion());
    } catch (SpecNotFoundException e) {
      LOGGER.warn("Unknown job spec URI: '" + jobURI + "'.  Deletion failed.");
    } catch (IOException e) {
      throw new RuntimeException("When removing a JobConf. file, issues unexpected happen:" + e.getMessage());
    } finally {
      // HelixRetriggeringJobCallable deletes the job file after submitting it to helix,
      // so trigger listeners regardless of its existence.
      if (alwaysTriggerListeners) {
        this.listeners.onCancelJob(jobURI);
      }
    }
  }
}
