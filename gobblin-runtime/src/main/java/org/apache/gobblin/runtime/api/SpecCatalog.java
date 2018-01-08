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
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.gobblin.instrumented.GobblinMetricsKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.instrumented.StandardMetricsBridge;
import org.apache.gobblin.metrics.ContextAwareCounter;
import org.apache.gobblin.metrics.ContextAwareGauge;
import org.apache.gobblin.metrics.ContextAwareTimer;
import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


public interface SpecCatalog extends SpecCatalogListenersContainer, StandardMetricsBridge {
  /** Returns an immutable {@link Collection} of {@link Spec}s that are known to the catalog. */
  Collection<Spec> getSpecs();

  /** Metrics for the spec catalog; null if
   * ({@link #isInstrumentationEnabled()}) is false. */
  SpecCatalog.StandardMetrics getMetrics();

  default StandardMetricsBridge.StandardMetrics getStandardMetrics() {
    return this.getMetrics();
  }

  /**
   * Get a {@link Spec} by uri.
   * @throws SpecNotFoundException if no such Spec exists
   **/
  Spec getSpec(URI uri) throws SpecNotFoundException;

  @Slf4j
  public static class StandardMetrics extends StandardMetricsBridge.StandardMetrics implements SpecCatalogListener {
    public static final String NUM_ACTIVE_SPECS_NAME = "numActiveSpecs";
    public static final String TOTAL_ADD_CALLS = "totalAddCalls";
    public static final String TOTAL_DELETE_CALLS = "totalDeleteCalls";
    public static final String TOTAL_UPDATE_CALLS = "totalUpdateCalls";
    public static final String TRACKING_EVENT_NAME = "SpecCatalogEvent";
    public static final String SPEC_ADDED_OPERATION_TYPE = "SpecAdded";
    public static final String SPEC_DELETED_OPERATION_TYPE = "SpecDeleted";
    public static final String SPEC_UPDATED_OPERATION_TYPE = "SpecUpdated";
    public static final String TIME_FOR_SPEC_CATALOG_GET = "timeForSpecCatalogGet";

    private final MetricContext metricsContext;
    @Getter private final AtomicLong totalAddedSpecs;
    @Getter private final AtomicLong totalDeletedSpecs;
    @Getter private final AtomicLong totalUpdatedSpecs;
    @Getter private final ContextAwareGauge<Long> totalAddCalls;
    @Getter private final ContextAwareGauge<Long> totalDeleteCalls;
    @Getter private final ContextAwareGauge<Long> totalUpdateCalls;
    @Getter private final ContextAwareGauge<Integer> numActiveSpecs;

    @Getter private final ContextAwareTimer timeForSpecCatalogGet;

    public StandardMetrics(final SpecCatalog specCatalog) {
      this.metricsContext = specCatalog.getMetricContext();
      this.timeForSpecCatalogGet = metricsContext.contextAwareTimer(TIME_FOR_SPEC_CATALOG_GET, 1, TimeUnit.MINUTES);
      this.totalAddedSpecs = new AtomicLong(0);
      this.totalDeletedSpecs = new AtomicLong(0);
      this.totalUpdatedSpecs = new AtomicLong(0);
      this.numActiveSpecs = metricsContext.newContextAwareGauge(NUM_ACTIVE_SPECS_NAME,  ()->{
          long startTime = System.currentTimeMillis();
          int size = specCatalog.getSpecs().size();
          updateGetSpecTime(startTime);
          return size;
      });
      this.totalAddCalls = metricsContext.newContextAwareGauge(TOTAL_ADD_CALLS, ()->this.totalAddedSpecs.get());
      this.totalUpdateCalls = metricsContext.newContextAwareGauge(TOTAL_UPDATE_CALLS, ()->this.totalUpdatedSpecs.get());
      this.totalDeleteCalls = metricsContext.newContextAwareGauge(TOTAL_DELETE_CALLS, ()->this.totalDeletedSpecs.get());
    }

    public void updateGetSpecTime(long startTime) {
      log.info("updateGetSpecTime...");
      Instrumented.updateTimer(Optional.of(this.timeForSpecCatalogGet), System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
    }

    @Override
    public Collection<ContextAwareGauge<?>> getGauges() {
      return ImmutableList.of(numActiveSpecs, totalAddCalls, totalUpdateCalls, totalDeleteCalls);
    }

    @Override
    public Collection<ContextAwareCounter> getCounters() {
      return ImmutableList.of();
    }

    @Override
    public Collection<ContextAwareTimer> getTimers() {
      return ImmutableList.of(this.timeForSpecCatalogGet);
    }

    @Override public void onAddSpec(Spec addedSpec) {
      this.totalAddedSpecs.incrementAndGet();
      submitTrackingEvent(addedSpec, SPEC_ADDED_OPERATION_TYPE);
    }

    private void submitTrackingEvent(Spec spec, String operType) {
      submitTrackingEvent(spec.getUri(), spec.getVersion(), operType);
    }

    private void submitTrackingEvent(URI specSpecURI, String specSpecVersion, String operType) {
      GobblinTrackingEvent e = GobblinTrackingEvent.newBuilder()
          .setName(TRACKING_EVENT_NAME)
          .setNamespace(SpecCatalog.class.getName())
          .setMetadata(ImmutableMap.<String, String>builder()
              .put(GobblinMetricsKeys.OPERATION_TYPE_META, operType)
              .put(GobblinMetricsKeys.SPEC_URI_META, specSpecURI.toString())
              .put(GobblinMetricsKeys.SPEC_VERSION_META, specSpecVersion)
              .build())
          .build();
      this.metricsContext.submitEvent(e);
    }

    @Override
    public void onDeleteSpec(URI deletedSpecURI, String deletedSpecVersion) {
      this.totalDeletedSpecs.incrementAndGet();
      submitTrackingEvent(deletedSpecURI, deletedSpecVersion, SPEC_DELETED_OPERATION_TYPE);
    }

    @Override
    public void onUpdateSpec(Spec updatedSpec) {
      this.totalUpdatedSpecs.incrementAndGet();
      submitTrackingEvent(updatedSpec, SPEC_UPDATED_OPERATION_TYPE);
    }
  }
}
