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

package org.apache.gobblin.runtime.spec_catalog;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.typesafe.config.Config;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.Getter;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.GobblinInstanceEnvironment;
import org.apache.gobblin.runtime.api.MutableSpecCatalog;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecCatalog;
import org.apache.gobblin.runtime.api.SpecCatalogListener;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.api.SpecSerDe;
import org.apache.gobblin.runtime.api.SpecStore;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.runtime.spec_serde.JavaSpecSerDe;
import org.apache.gobblin.runtime.spec_store.FSSpecStore;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.callbacks.CallbackResult;
import org.apache.gobblin.util.callbacks.CallbacksDispatcher;


@Alpha
@Singleton
public class TopologyCatalog extends AbstractIdleService implements SpecCatalog, MutableSpecCatalog {

  public static final String DEFAULT_TOPOLOGYSPEC_STORE_CLASS = FSSpecStore.class.getCanonicalName();
  public static final String DEFAULT_TOPOLOGYSPEC_SERDE_CLASS = JavaSpecSerDe.class.getCanonicalName();

  protected final SpecCatalogListenersList listeners;
  protected final Logger log;
  protected final MetricContext metricContext;
  protected final TopologyCatalog.StandardMetrics metrics;
  protected final SpecStore specStore;
  @Getter
  protected CountDownLatch initComplete = new CountDownLatch(1);

  private final ClassAliasResolver<SpecStore> aliasResolver;

  public TopologyCatalog(Config config) {
    this(config, Optional.<Logger>absent());
  }

  public TopologyCatalog(Config config, Optional<Logger> log) {
    this(config, log, Optional.<MetricContext>absent(), true);
  }

  @Inject
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
          parentMetricContext.or(Instrumented.getMetricContext(new org.apache.gobblin.configuration.State(), getClass()));
      this.metricContext = realParentCtx.childBuilder(TopologyCatalog.class.getSimpleName()).build();
      this.metrics = new SpecCatalog.StandardMetrics(this, Optional.of(config));
      this.addListener(this.metrics);
    }
    else {
      this.metricContext = null;
      this.metrics = null;
    }

    this.aliasResolver = new ClassAliasResolver<>(SpecStore.class);
    try {
      Config newConfig = config;
      if (config.hasPath(ConfigurationKeys.TOPOLOGYSPEC_STORE_DIR_KEY)) {
        newConfig = config.withValue(FSSpecStore.SPECSTORE_FS_DIR_KEY,
            config.getValue(ConfigurationKeys.TOPOLOGYSPEC_STORE_DIR_KEY));
      }
      String specStoreClassName = ConfigUtils.getString(config, ConfigurationKeys.TOPOLOGYSPEC_STORE_CLASS_KEY,
          DEFAULT_TOPOLOGYSPEC_STORE_CLASS);
      this.log.info("Using SpecStore class name/alias " + specStoreClassName);
      String specSerDeClassName = ConfigUtils.getString(config, ConfigurationKeys.TOPOLOGYSPEC_SERDE_CLASS_KEY,
          DEFAULT_TOPOLOGYSPEC_SERDE_CLASS);
      this.log.info("Using SpecSerDe class name/alias " + specSerDeClassName);

      SpecSerDe specSerDe = (SpecSerDe) ConstructorUtils.invokeConstructor(Class.forName(
          new ClassAliasResolver<>(SpecSerDe.class).resolve(specSerDeClassName)));
      this.specStore = (SpecStore) ConstructorUtils.invokeConstructor(Class.forName(this.aliasResolver.resolve(
          specStoreClassName)), newConfig, specSerDe);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
        | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
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
  public List<Tag<?>> generateTags(org.apache.gobblin.configuration.State state) {
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
  public int getSize() {
    try {
      return specStore.getSize();
    } catch (IOException e) {
      throw new RuntimeException("Cannot retrieve number of specs from Spec store", e);
    }
  }

  @Override
  public Spec getSpecs(URI uri) throws SpecNotFoundException {
    try {
      return specStore.getSpec(uri);
    } catch (IOException e) {
      throw new RuntimeException("Cannot retrieve Spec from Spec store for URI: " + uri, e);
    }
  }

  @Override
  public Map<String, AddSpecResponse> put(Spec spec) {
    Map<String, AddSpecResponse> responseMap = new HashMap<>();

    try {
      Preconditions.checkState(state() == Service.State.RUNNING, String.format("%s is not running.", this.getClass().getName()));
      Preconditions.checkNotNull(spec);

      log.info(String.format("Adding TopologySpec with URI: %s and Config: %s", spec.getUri(),
          ((TopologySpec) spec).getConfigAsProperties()));
        specStore.addSpec(spec);
      AddSpecResponse<CallbacksDispatcher.CallbackResults<SpecCatalogListener, AddSpecResponse>> response = this.listeners.onAddSpec(spec);
      for (Map.Entry<SpecCatalogListener, CallbackResult<AddSpecResponse>> entry: response.getValue().getSuccesses().entrySet()) {
        responseMap.put(entry.getKey().getName(), entry.getValue().getResult());
      }
    } catch (IOException e) {
      throw new RuntimeException("Cannot add Spec to Spec store: " + spec, e);
    }
    return responseMap;
  }

  public void remove(URI uri) {
    remove(uri, new Properties());
  }

  @Override
  public void remove(URI uri, Properties headers) {
    try {
      Preconditions.checkState(state() == Service.State.RUNNING, String.format("%s is not running.", this.getClass().getName()));
      Preconditions.checkNotNull(uri);

      log.info(String.format("Removing TopologySpec with URI: %s", uri));
      this.listeners.onDeleteSpec(uri, FlowSpec.Builder.DEFAULT_VERSION, headers);
      specStore.deleteSpec(uri);
    } catch (IOException e) {
      throw new RuntimeException("Cannot delete Spec from Spec store for URI: " + uri, e);
    }
  }
}
