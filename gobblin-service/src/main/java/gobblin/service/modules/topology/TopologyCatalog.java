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

package gobblin.service.modules.topology;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.typesafe.config.Config;

import gobblin.annotation.Alpha;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.MetricContext;
import gobblin.metrics.Tag;
import gobblin.runtime.api.GobblinInstanceEnvironment;
import gobblin.runtime.api.MutableSpecCatalog;
import gobblin.runtime.api.Spec;
import gobblin.runtime.api.SpecCatalog;
import gobblin.runtime.api.SpecCatalogListener;
import gobblin.runtime.api.SpecNotFoundException;
import gobblin.runtime.api.SpecStore;
import gobblin.service.modules.SpecCatalogListenersList;
import gobblin.util.ConfigUtils;
import gobblin.util.reflection.GobblinConstructorUtils;


@Alpha
public class TopologyCatalog extends AbstractIdleService implements SpecCatalog, MutableSpecCatalog {

  public static final String GOBBLIN_SERVICE_TOPOLOGYSPEC_STORE_CLASS_KEY = "gobblin.service.topologySpec.store.class";
  public static final String DEFAULT_GOBBLIN_SERVICE_TOPOLOGYSPEC_STORE_CLASS = "gobblin.service.modules.topology.ConfigBasedTopologyStore";

  protected final SpecCatalogListenersList listeners;
  protected final Logger log;
  protected final MetricContext metricContext;
  protected final TopologyCatalog.StandardMetrics metrics;
  protected final SpecStore specStore;

  public TopologyCatalog(Config config) {
    this(config, Optional.<Logger>absent());
  }

  public TopologyCatalog(Config config, Optional<Logger> log) {
    this(config, log, Optional.<MetricContext>absent(), true);
  }

  public TopologyCatalog(Config config, GobblinInstanceEnvironment env) {
    this(config, Optional.of(env.getLog()), Optional.of(env.getMetricContext()),
        env.isInstrumentationEnabled());
  }

  public TopologyCatalog(Config config, Optional<Logger> log, Optional<MetricContext> parentMetricContext,
      boolean instrumentationEnabled) {
    this.log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());
    this.listeners = new SpecCatalogListenersList(log);
    if (instrumentationEnabled) {
      MetricContext realParentCtx =
          parentMetricContext.or(Instrumented.getMetricContext(new gobblin.configuration.State(), getClass()));
      this.metricContext = realParentCtx.childBuilder(TopologyCatalog.class.getSimpleName()).build();
      this.metrics = new SpecCatalog.StandardMetrics(this);
    }
    else {
      this.metricContext = null;
      this.metrics = null;
    }
    this.specStore = GobblinConstructorUtils.invokeConstructor(SpecStore.class, ConfigUtils.getString(config,
        GOBBLIN_SERVICE_TOPOLOGYSPEC_STORE_CLASS_KEY, DEFAULT_GOBBLIN_SERVICE_TOPOLOGYSPEC_STORE_CLASS));
  }

  /***************************************************
   /* Catalog init and shutdown handlers             *
   /**************************************************/

  @Override
  protected void startUp() throws Exception {
    notifyAllListeners();
  }

  @Override
  protected void shutDown() throws Exception {
    this.listeners.close();
  }

  /***************************************************
   /* Catalog listeners                              *
   /**************************************************/

  protected void notifyAllListeners() {
    for (Spec spec : getSpecs()) {
      this.listeners.onAddSpec(spec);
    }
  }

  @Override
  public void addListener(SpecCatalogListener specListener) {
    Preconditions.checkNotNull(specListener);
    this.listeners.addListener(specListener);

    if (state() == Service.State.RUNNING) {
      for (Spec spec : getSpecs()) {
        SpecCatalogListener.AddSpecCallback addJobCallback = new SpecCatalogListener.AddSpecCallback(spec);
        this.listeners.callbackOneListener(addJobCallback, specListener);
      }
    }
  }

  @Override
  public void removeListener(SpecCatalogListener specCatalogListener) {
    this.listeners.removeListener(specCatalogListener);
  }

  @Override
  public void registerWeakSpecCatalogListener(SpecCatalogListener specCatalogListener) {
    this.listeners.registerWeakSpecCatalogListener(specCatalogListener);
  }

  /***************************************************
   /* Catalog metrics                                *
   /**************************************************/

  @Nonnull
  @Override
  public MetricContext getMetricContext() {
    return this.metricContext;
  }

  @Override
  public boolean isInstrumentationEnabled() {
    return null != this.metricContext;
  }

  @Override
  public List<Tag<?>> generateTags(gobblin.configuration.State state) {
    return Collections.emptyList();
  }

  @Override
  public void switchMetricContext(List<Tag<?>> tags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void switchMetricContext(MetricContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SpecCatalog.StandardMetrics getMetrics() {
    return this.metrics;
  }

  /**************************************************
   /* Catalog core functionality                     *
   /**************************************************/

  @Override
  public Collection<Spec> getSpecs() {
    try {
      return specStore.getSpecs();
    } catch (IOException e) {
      throw new RuntimeException("Cannot retrieve Specs from Spec store", e);
    }
  }

  @Override
  public Spec getSpec(URI uri) throws SpecNotFoundException {
    try {
      return specStore.getSpec(uri);
    } catch (IOException e) {
      throw new RuntimeException("Cannot retrieve Spec from Spec store for URI: " + uri, e);
    }
  }

  @Override
  public void put(Spec spec) {
    try {
      Preconditions.checkState(state() == Service.State.RUNNING, String.format("%s is not running.", this.getClass().getName()));
      Preconditions.checkNotNull(spec);

      if (specStore.exists(spec.getUri())) {
        specStore.updateSpec(spec);
        this.listeners.onUpdateSpec(spec);
      } else {
        specStore.addSpec(spec);
        this.listeners.onAddSpec(spec);
      }

    } catch (IOException | SpecNotFoundException e) {
      throw new RuntimeException("Cannot add Spec to Spec store: " + spec, e);
    }
  }

  @Override
  public void remove(URI uri) {
    try {
      Preconditions.checkState(state() == Service.State.RUNNING, String.format("%s is not running.", this.getClass().getName()));
      Preconditions.checkNotNull(uri);

      Spec spec = specStore.getSpec(uri);
      specStore.deleteSpec(uri);
      this.listeners.onDeleteSpec(spec.getUri(), spec.getVersion());

    } catch (IOException | SpecNotFoundException e) {
      throw new RuntimeException("Cannot delete Spec from Spec store for URI: " + uri, e);
    }
  }
}
