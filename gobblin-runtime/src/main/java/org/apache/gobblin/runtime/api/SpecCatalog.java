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

import com.codahale.metrics.Gauge;
import com.google.common.collect.ImmutableMap;

import org.apache.gobblin.instrumented.GobblinMetricsKeys;
import org.apache.gobblin.instrumented.Instrumentable;
import org.apache.gobblin.metrics.ContextAwareCounter;
import org.apache.gobblin.metrics.ContextAwareGauge;
import org.apache.gobblin.metrics.GobblinTrackingEvent;

import lombok.Getter;


public interface SpecCatalog extends SpecCatalogListenersContainer, Instrumentable {
  /** Returns an immutable {@link Collection} of {@link Spec}s that are known to the catalog. */
  Collection<Spec> getSpecs();

  /** Metrics for the spec catalog; null if
   * ({@link #isInstrumentationEnabled()}) is false. */
  SpecCatalog.StandardMetrics getMetrics();

  /**
   * Get a {@link Spec} by uri.
   * @throws SpecNotFoundException if no such Spec exists
   **/
  Spec getSpec(URI uri) throws SpecNotFoundException;

  public static class StandardMetrics implements SpecCatalogListener {
    public static final String NUM_ACTIVE_SPECS_NAME = "numActiveSpecs";
    public static final String NUM_ADDED_SPECS = "numAddedSpecs";
    public static final String NUM_DELETED_SPECS = "numDeletedSpecs";
    public static final String NUM_UPDATED_SPECS = "numUpdatedSpecs";
    public static final String TRACKING_EVENT_NAME = "SpecCatalogEvent";
    public static final String SPEC_ADDED_OPERATION_TYPE = "SpecAdded";
    public static final String SPEC_DELETED_OPERATION_TYPE = "SpecDeleted";
    public static final String SPEC_UPDATED_OPERATION_TYPE = "SpecUpdated";

    @Getter
    private final ContextAwareGauge<Integer> numActiveSpecs;
    @Getter private final ContextAwareCounter numAddedSpecs;
    @Getter private final ContextAwareCounter numDeletedSpecs;
    @Getter private final ContextAwareCounter numUpdatedSpecs;

    public StandardMetrics(final SpecCatalog parent) {
      this.numAddedSpecs = parent.getMetricContext().contextAwareCounter(NUM_ADDED_SPECS);
      this.numDeletedSpecs = parent.getMetricContext().contextAwareCounter(NUM_DELETED_SPECS);
      this.numUpdatedSpecs = parent.getMetricContext().contextAwareCounter(NUM_UPDATED_SPECS);
      this.numActiveSpecs = parent.getMetricContext().newContextAwareGauge(NUM_ACTIVE_SPECS_NAME,
          new Gauge<Integer>() {
            @Override public Integer getValue() {
              return parent.getSpecs().size();
            }
          });
      parent.addListener(this);
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
