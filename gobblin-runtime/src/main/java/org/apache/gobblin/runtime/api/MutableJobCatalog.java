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
package org.apache.gobblin.runtime.api;

import java.net.URI;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import com.typesafe.config.Config;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.ContextAwareTimer;


/**
 * A {@link JobCatalog} that can have its {@link Collection} of {@link JobSpec}s modified
 * programmatically. Note that jobs in a job catalog can change from the outside. This is covered
 * by the base JobCatalog interface.
 */
@Alpha
public interface MutableJobCatalog extends JobCatalog {
  /**
   * Registers a new JobSpec. If a JobSpec with the same {@link JobSpec#getUri()} exists,
   * it will be replaced.
   * */
  public void put(JobSpec jobSpec);

  default void remove(URI uri, boolean alwaysTriggerListeners) {
    throw new UnsupportedOperationException("Method not implemented by " + this.getClass());
  }

  /**
   * Removes an existing JobSpec with the given URI. A no-op if such JobSpec does not exist.
   */
  void remove(URI uri);

  @Slf4j
  public static class MutableStandardMetrics extends JobCatalog.StandardMetrics {
    public static final String TIME_FOR_JOB_CATALOG_REMOVE = "timeForJobCatalogRemove";
    public static final String TIME_FOR_JOB_CATALOG_PUT = "timeForJobCatalogPut";
    @Getter private final ContextAwareTimer timeForJobCatalogPut;
    @Getter private final ContextAwareTimer timeForJobCatalogRemove;
    public MutableStandardMetrics(JobCatalog catalog, Optional<Config> sysConfig) {
      super(catalog, sysConfig);
      timeForJobCatalogPut = catalog.getMetricContext().contextAwareTimer(TIME_FOR_JOB_CATALOG_PUT, timeWindowSizeInMinutes, TimeUnit.MINUTES);
      timeForJobCatalogRemove =  catalog.getMetricContext().contextAwareTimer(TIME_FOR_JOB_CATALOG_REMOVE, this.timeWindowSizeInMinutes, TimeUnit.MINUTES);
      this.contextAwareMetrics.add(timeForJobCatalogPut);
      this.contextAwareMetrics.add(timeForJobCatalogRemove);
    }

    public void updatePutJobTime(long startTime) {
      log.info("updatePutJobTime...");
      Instrumented.updateTimer(Optional.of(this.timeForJobCatalogPut), System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
    }

    public void updateRemoveJobTime(long startTime) {
      log.info("updateRemoveJobTime...");
      Instrumented.updateTimer(Optional.of(this.timeForJobCatalogRemove), System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
    }
  }
}
