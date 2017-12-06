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
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Gauge;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.gobblin.instrumented.GobblinMetricsKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.instrumented.StandardMetricsBridge;
import org.apache.gobblin.metrics.ContextAwareCounter;
import org.apache.gobblin.metrics.ContextAwareGauge;
import org.apache.gobblin.metrics.ContextAwareHistogram;
import org.apache.gobblin.metrics.ContextAwareTimer;
import org.apache.gobblin.metrics.GobblinTrackingEvent;

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
    public static final String NUM_ADDED_SPECS = "numAddedSpecs";
    public static final String NUM_DELETED_SPECS = "numDeletedSpecs";
    public static final String NUM_UPDATED_SPECS = "numUpdatedSpecs";
    public static final String TRACKING_EVENT_NAME = "SpecCatalogEvent";
    public static final String SPEC_ADDED_OPERATION_TYPE = "SpecAdded";
    public static final String SPEC_DELETED_OPERATION_TYPE = "SpecDeleted";
    public static final String SPEC_UPDATED_OPERATION_TYPE = "SpecUpdated";
    public static final String TIME_FOR_SPEC_CATALOG_GET = "timeForSpecCatalogGet";
    public static final String HISTOGRAM_FOR_SPEC_ADD = "histogramForSpecAdd";
    public static final String HISTOGRAM_FOR_SPEC_UPDATE = "histogramForSpecUpdate";
    public static final String HISTOGRAM_FOR_SPEC_DELETE = "histogramForSpecDelete";

    @Getter private final ContextAwareGauge<Integer> numActiveSpecs;
    @Getter private final ContextAwareCounter numAddedSpecs;
    @Getter private final ContextAwareCounter numDeletedSpecs;
    @Getter private final ContextAwareCounter numUpdatedSpecs;
    @Getter private final ContextAwareTimer timeForSpecCatalogGet;
    @Getter private final ContextAwareHistogram histogramForSpecAdd;
    @Getter private final ContextAwareHistogram histogramForSpecUpdate;
    @Getter private final ContextAwareHistogram histogramForSpecDelete;

    public StandardMetrics(final SpecCatalog specCatalog) {
      this.timeForSpecCatalogGet = specCatalog.getMetricContext().contextAwareTimerWithSlidingTimeWindow(TIME_FOR_SPEC_CATALOG_GET, 1, TimeUnit.MINUTES);
      this.numAddedSpecs = specCatalog.getMetricContext().contextAwareCounter(NUM_ADDED_SPECS);
      this.numDeletedSpecs = specCatalog.getMetricContext().contextAwareCounter(NUM_DELETED_SPECS);
      this.numUpdatedSpecs = specCatalog.getMetricContext().contextAwareCounter(NUM_UPDATED_SPECS);
      this.numActiveSpecs = specCatalog.getMetricContext().newContextAwareGauge(NUM_ACTIVE_SPECS_NAME,
          new Gauge<Integer>() {
            @Override public Integer getValue() {
              long startTime = System.currentTimeMillis();
              int size = specCatalog.getSpecs().size();
              updateGetSpecTime(startTime);
              return size;
            }
          });
      this.histogramForSpecAdd = specCatalog.getMetricContext().contextAwareHistogramWithSlidingTimeWindow(HISTOGRAM_FOR_SPEC_ADD, 1, TimeUnit.MINUTES);
      this.histogramForSpecUpdate = specCatalog.getMetricContext().contextAwareHistogramWithSlidingTimeWindow(HISTOGRAM_FOR_SPEC_UPDATE, 1, TimeUnit.MINUTES);
      this.histogramForSpecDelete = specCatalog.getMetricContext().contextAwareHistogramWithSlidingTimeWindow(HISTOGRAM_FOR_SPEC_DELETE, 1, TimeUnit.MINUTES);
    }

    public void updateGetSpecTime(long startTime) {
      log.info("updateGetSpecTime...");
      Instrumented.updateTimer(Optional.of(this.timeForSpecCatalogGet), System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
    }

    @Override
    public String getName() {
      return this.getClass().getName();
    }

    @Override
    public Collection<ContextAwareGauge<?>> getGauges() {
      return Collections.singleton(this.numActiveSpecs);
    }

    @Override
    public Collection<ContextAwareCounter> getCounters() {
      return ImmutableList.of(numAddedSpecs, numDeletedSpecs, numUpdatedSpecs);
    }

    @Override
    public Collection<ContextAwareHistogram> getHistograms() {
      return ImmutableList.of(histogramForSpecAdd, histogramForSpecDelete, histogramForSpecUpdate);
    }

    @Override
    public Collection<ContextAwareTimer> getTimers() {
      return ImmutableList.of(this.timeForSpecCatalogGet);
    }

    @Override public void onAddSpec(Spec addedSpec) {
      this.numAddedSpecs.inc();
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
      this.numAddedSpecs.getContext().submitEvent(e);
    }

    @Override
    public void onDeleteSpec(URI deletedSpecURI, String deletedSpecVersion) {
      this.numDeletedSpecs.inc();
      submitTrackingEvent(deletedSpecURI, deletedSpecVersion, SPEC_DELETED_OPERATION_TYPE);
    }

    @Override
    public void onUpdateSpec(Spec updatedSpec) {
      this.numUpdatedSpecs.inc();
      submitTrackingEvent(updatedSpec, SPEC_UPDATED_OPERATION_TYPE);
    }
  }
}
