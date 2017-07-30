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

package gobblin.runtime.job_catalog;

import java.net.URI;

import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import gobblin.metrics.MetricContext;
import gobblin.runtime.api.GobblinInstanceEnvironment;
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.MutableJobCatalog;


/**
 * A base for {@link MutableJobCatalog}s. Handles notifications to listeners on calls to {@link #put}, {@link #remove},
 * as well as the state of the {@link com.google.common.util.concurrent.Service}.
 */
public abstract class MutableJobCatalogBase extends JobCatalogBase implements MutableJobCatalog {

  public MutableJobCatalogBase() {
  }

  public MutableJobCatalogBase(Optional<Logger> log) {
    super(log);
  }

  public MutableJobCatalogBase(GobblinInstanceEnvironment env) {
    super(env);
  }

  public MutableJobCatalogBase(Optional<Logger> log, Optional<MetricContext> parentMetricContext,
      boolean instrumentationEnabled) {
    super(log, parentMetricContext, instrumentationEnabled);
  }

  @Override
  public void put(JobSpec jobSpec) {
    Preconditions.checkState(allowMutationsBeforeStartup() || state() == State.RUNNING, String.format("%s is not running.", this.getClass().getName()));
    Preconditions.checkNotNull(jobSpec);
    JobSpec oldSpec = doPut(jobSpec);
    if (null == oldSpec) {
      this.listeners.onAddJob(jobSpec);
    } else {
      this.listeners.onUpdateJob(jobSpec);
    }
  }

  /**
   * Add the {@link JobSpec} to the catalog. If a {@link JobSpec} already existed with that {@link URI}, it should be
   * replaced and the old one returned.
   * @return if a {@link JobSpec} already existed with that {@link URI}, return the old {@link JobSpec}. Otherwise return
   * null.
   */
  protected abstract JobSpec doPut(JobSpec jobSpec);

  @Override
  public void remove(URI uri) {
    Preconditions.checkState(state() == State.RUNNING, String.format("%s is not running.", this.getClass().getName()));
    Preconditions.checkNotNull(uri);
    JobSpec jobSpec = doRemove(uri);
    if (null != jobSpec) {
      this.listeners.onDeleteJob(jobSpec.getUri(), jobSpec.getVersion());
    }
  }

  /**
   * Remove the {@link JobSpec} with the given {@link URI} from the catalog.
   * @return The removed {@link JobSpec}, or null if no {@link JobSpec} with the given {@link URI} existed.
   */
  protected abstract JobSpec doRemove(URI uri);

  /**
   * Specifies whether the catalog can be mutated (add or remove {@link JobSpec}) before it has been started.
   */
  protected boolean allowMutationsBeforeStartup() {
    return true;
  }
}
