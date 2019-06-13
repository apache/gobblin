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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;

import javax.annotation.Nonnull;

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
import org.apache.gobblin.runtime.spec_serde.JavaSpecSerDe;
import org.apache.gobblin.runtime.spec_store.FSSpecStore;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.callbacks.CallbackResult;
import org.apache.gobblin.util.callbacks.CallbacksDispatcher;


/**
 * A service that interact with FlowSpec storage.
 * The FlowSpec storage, a.k.a. {@link SpecStore} should be plugable with different implementation.
 */
public class FlowCatalog extends AbstractIdleService implements SpecCatalog, MutableSpecCatalog {

  /***
   * Configuration properties related to FlowSpec Store
   */
  public static final String FLOWSPEC_STORE_CLASS_KEY = "flowSpec.store.class";
  public static final String FLOWSPEC_STORE_DIR_KEY = "flowSpec.store.dir";
  public static final String DEFAULT_FLOWSPEC_STORE_CLASS = FSSpecStore.class.getCanonicalName();
  public static final String FLOWSPEC_SERDE_CLASS_KEY = "flowSpec.serde.class";
  public static final String DEFAULT_FLOWSPEC_SERDE_CLASS = JavaSpecSerDe.class.getCanonicalName();

  protected final SpecCatalogListenersList listeners;
  protected final Logger log;
  protected final MetricContext metricContext;
  protected final MutableStandardMetrics metrics;
  protected final SpecStore specStore;

  private final ClassAliasResolver<SpecStore> aliasResolver;

  public FlowCatalog(Config config) {
    this(config, Optional.<Logger>absent());
  }

  public FlowCatalog(Config config, Optional<Logger> log) {
    this(config, log, Optional.<MetricContext>absent(), true);
  }

  public FlowCatalog(Config config, GobblinInstanceEnvironment env) {
    this(config, Optional.of(env.getLog()), Optional.of(env.getMetricContext()),
        env.isInstrumentationEnabled());
  }

  public FlowCatalog(Config config, Optional<Logger> log, Optional<MetricContext> parentMetricContext,
      boolean instrumentationEnabled) {
    this.log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());
    this.listeners = new SpecCatalogListenersList(log);
    if (instrumentationEnabled) {
      MetricContext realParentCtx =
          parentMetricContext.or(Instrumented.getMetricContext(new org.apache.gobblin.configuration.State(), getClass()));
      this.metricContext = realParentCtx.childBuilder(FlowCatalog.class.getSimpleName()).build();
      this.metrics = new MutableStandardMetrics(this, Optional.of(config));
      this.addListener(this.metrics);
    } else {
      this.metricContext = null;
      this.metrics = null;
    }

    this.aliasResolver = new ClassAliasResolver<>(SpecStore.class);
    try {
      Config newConfig = config;
      if (config.hasPath(FLOWSPEC_STORE_DIR_KEY)) {
        newConfig = config.withValue(FSSpecStore.SPECSTORE_FS_DIR_KEY,
            config.getValue(FLOWSPEC_STORE_DIR_KEY));
      }
      String specStoreClassName = ConfigUtils.getString(config, FLOWSPEC_STORE_CLASS_KEY, DEFAULT_FLOWSPEC_STORE_CLASS);
      this.log.info(String.format("Using class name/alias [%s] for specstore", specStoreClassName));
      String specSerDeClassName = ConfigUtils.getString(config, FLOWSPEC_SERDE_CLASS_KEY, DEFAULT_FLOWSPEC_SERDE_CLASS);
      this.log.info(String.format("Using class name/alias [%s] for spec serde", specSerDeClassName));

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
    //Do nothing
  }

  @Override
  protected void shutDown() throws Exception {
    this.listeners.close();
  }

  /***************************************************
   /* Catalog listeners                              *
   /**************************************************/

  protected void notifyAllListeners() {
    try {
      Iterator<URI> uriIterator = getSpecURIs();
      while (uriIterator.hasNext()) {
        this.listeners.onAddSpec(getSpecWrapper(uriIterator.next()));
      }
    } catch (IOException e) {
      log.error("Cannot retrieve specs from catalog:", e);
    }
  }

  @Override
  public void addListener(SpecCatalogListener specListener) {
    Preconditions.checkNotNull(specListener);
    this.listeners.addListener(specListener);

    if (state() == State.RUNNING) {
      try {
        Iterator<URI> uriIterator = getSpecURIs();
        while (uriIterator.hasNext()) {
          SpecCatalogListener.AddSpecCallback addJobCallback =
              new SpecCatalogListener.AddSpecCallback(getSpecWrapper(uriIterator.next()));
          this.listeners.callbackOneListener(addJobCallback, specListener);
        }
      } catch (IOException e) {
        log.error("Cannot retrieve specs from catalog:", e);
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

  public Iterator<URI> getSpecURIs() throws IOException {
    return specStore.getSpecURIs();
  }

  public Iterator<URI> getSpecURISWithTag(String tag) throws IOException {
    return specStore.getSpecURIsWithTag(tag);
  }

  /**
   * Get all specs from {@link SpecStore}
   * Not suggested for {@link FlowCatalog} where the total amount of space that all {@link FlowSpec}s occupied
   * would be large and loading process is slow.
   */
  @Deprecated
  @Override
  public Collection<Spec> getSpecs() {
    try {
      return specStore.getSpecs();
      // TODO: Have kind of metrics keeping track of specs that failed to be deserialized.
    } catch (IOException e) {
      throw new RuntimeException("Cannot retrieve Specs from Spec store", e);
    }
  }

  public boolean exists(URI uri) {
    try {
      return specStore.exists(uri);
    } catch (IOException e) {
      throw new RuntimeException("Cannot retrieve Spec from Spec store for URI: " + uri, e);
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

  /**
   * A wrapper of getSpec that handles {@link SpecNotFoundException} properly.
   * This is the most common way to fetch {@link Spec}. For customized way to deal with exception, one will
   * need to implement specific catch-block logic.
   */
  public Spec getSpecWrapper(URI uri) {
    Spec spec = null;
    try {
      spec = getSpec(uri);
    } catch (SpecNotFoundException snfe) {
      log.error(String.format("The URI %s discovered in SpecStore is missing in FlowCatlog"
          + ", suspecting current modification on SpecStore", uri), snfe);
    }
    return spec;
  }

  /**
   * Persist {@link Spec} into {@link SpecStore} and notify {@link SpecCatalogListener} if triggerListener
   * is set to true.
   *
   * @param spec The Spec to be added
   * @param triggerListener True if listeners should be notified.
   * @return
   */
  public Map<String, AddSpecResponse> put(Spec spec, boolean triggerListener) {
    Map<String, AddSpecResponse> responseMap = new HashMap<>();
    try {
      Preconditions.checkState(state() == State.RUNNING, String.format("%s is not running.", this.getClass().getName()));
      Preconditions.checkNotNull(spec);

      long startTime = System.currentTimeMillis();
      log.info(String.format("Adding FlowSpec with URI: %s and Config: %s", spec.getUri(),
          ((FlowSpec) spec).getConfigAsProperties()));
      specStore.addSpec(spec);
      metrics.updatePutSpecTime(startTime);
      if (triggerListener) {
        AddSpecResponse<CallbacksDispatcher.CallbackResults<SpecCatalogListener, AddSpecResponse>> response = this.listeners.onAddSpec(spec);
        for (Map.Entry<SpecCatalogListener, CallbackResult<AddSpecResponse>> entry: response.getValue().getSuccesses().entrySet()) {
          responseMap.put(entry.getKey().getName(), entry.getValue().getResult());
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Cannot add Spec to Spec store: " + spec, e);
    }
    return responseMap;
  }

  @Override
  public Map<String, AddSpecResponse> put(Spec spec) {
    return put(spec, true);
  }

  public void remove(URI uri) {
    remove(uri, new Properties());
  }

  @Override
  public void remove(URI uri, Properties headers) {
    this.remove(uri, headers, true);
  }

  public void remove(URI uri, Properties headers, boolean triggerListener) {
    try {
      Preconditions.checkState(state() == State.RUNNING, String.format("%s is not running.", this.getClass().getName()));
      Preconditions.checkNotNull(uri);
      long startTime = System.currentTimeMillis();
      log.info(String.format("Removing FlowSpec with URI: %s", uri));
      specStore.deleteSpec(uri);
      this.metrics.updateRemoveSpecTime(startTime);
      if (triggerListener) {
        this.listeners.onDeleteSpec(uri, FlowSpec.Builder.DEFAULT_VERSION, headers);
      }
    } catch (IOException e) {
      throw new RuntimeException("Cannot delete Spec from Spec store for URI: " + uri, e);
    }
  }
}
