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

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Optional;
import com.typesafe.config.Config;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Instrumented version of {@link SpecStore} automatically capturing certain metrics.
 * Subclasses should implement addSpecImpl instead of addSpec and so on.
 */
public abstract class InstrumentedSpecStore implements SpecStore {
  private Optional<Timer> getTimer;
  private Optional<Timer> existsTimer;
  private Optional<Timer> deleteTimer;
  private Optional<Timer> addTimer;
  private Optional<Timer> updateTimer;
  private Optional<Timer> getAllTimer;
  private Optional<Timer> getURIsTimer;
  private MetricContext metricContext;
  private final boolean instrumentationEnabled;

  public InstrumentedSpecStore(Config config, SpecSerDe specSerDe) {
    this.instrumentationEnabled = GobblinMetrics.isEnabled(new State(ConfigUtils.configToProperties(config)));
    this.metricContext = Instrumented.getMetricContext(new State(), getClass());
    if (instrumentationEnabled) {
      this.getTimer = Optional.of(this.metricContext.timer(getClass() + "-GET"));
      this.existsTimer = Optional.of(this.metricContext.timer(
          MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,getClass().getSimpleName(), "-EXISTS")));
      this.deleteTimer = Optional.of(this.metricContext.timer(
          MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,getClass().getSimpleName(), "-DELETE")));
      this.addTimer = Optional.of(this.metricContext.timer(
          MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,getClass().getSimpleName(), "-ADD")));
      this.updateTimer = Optional.of(this.metricContext.timer(
          MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,getClass().getSimpleName(), "-UPDATE")));
      this.getAllTimer = Optional.of(this.metricContext.timer(
          MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,getClass().getSimpleName(), "-GETALL")));
      this.getURIsTimer = Optional.of(this.metricContext.timer(
          MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,getClass().getSimpleName(), "-GETURIS")));
    } else {
      this.getTimer = Optional.absent();
      this.existsTimer = Optional.absent();
      this.deleteTimer = Optional.absent();
      this.addTimer = Optional.absent();
      this.updateTimer = Optional.absent();
      this.getAllTimer = Optional.absent();
      this.getURIsTimer = Optional.absent();
    }
  }

  @Override
  public boolean exists(URI specUri) throws IOException {
    if (instrumentationEnabled) {
      return existsImpl(specUri);
    } else {
      long startTimeNanos = System.currentTimeMillis();
      boolean ret = existsImpl(specUri);
      Instrumented.updateTimer(this.existsTimer, System.currentTimeMillis() - startTimeNanos, TimeUnit.MILLISECONDS);
      return ret;
    }
  }

  @Override
  public void addSpec(Spec spec) throws IOException {
    if (instrumentationEnabled) {
      addSpecImpl(spec);
    } else {
      long startTimeNanos = System.currentTimeMillis();
      addSpecImpl(spec);
      Instrumented.updateTimer(this.addTimer, System.currentTimeMillis() - startTimeNanos, TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public boolean deleteSpec(URI specUri) throws IOException {
    if (instrumentationEnabled) {
      return deleteSpecImpl(specUri);
    } else {
      long startTimeNanos = System.currentTimeMillis();
      boolean ret = deleteSpecImpl(specUri);
      Instrumented.updateTimer(this.deleteTimer, System.currentTimeMillis() - startTimeNanos, TimeUnit.MILLISECONDS);
      return ret;
    }
  }

  @Override
  public Spec getSpec(URI specUri) throws IOException, SpecNotFoundException {
    if (instrumentationEnabled) {
      return getSpecImpl(specUri);
    } else {
      long startTimeNanos = System.currentTimeMillis();
      Spec spec = getSpecImpl(specUri);
      Instrumented.updateTimer(this.getTimer, System.currentTimeMillis() - startTimeNanos, TimeUnit.MILLISECONDS);
      return spec;
    }
  }

  @Override
  public Spec updateSpec(Spec spec) throws IOException, SpecNotFoundException {
    if (instrumentationEnabled) {
      return updateSpecImpl(spec);
    } else {
      long startTimeNanos = System.currentTimeMillis();
      Spec ret = updateSpecImpl(spec);
      Instrumented.updateTimer(this.updateTimer, System.currentTimeMillis() - startTimeNanos, TimeUnit.MILLISECONDS);
      return ret;
    }
  }

  @Override
  public Collection<Spec> getSpecs() throws IOException {
    if (instrumentationEnabled) {
      return getSpecsImpl();
    } else {
      long startTimeNanos = System.currentTimeMillis();
      Collection<Spec> spec = getSpecsImpl();
      Instrumented.updateTimer(this.getAllTimer, System.currentTimeMillis() - startTimeNanos, TimeUnit.MILLISECONDS);
      return spec;
    }
  }

  @Override
  public Iterator<URI> getSpecURIs() throws IOException {
    if (instrumentationEnabled) {
      return getSpecURIsImpl();
    } else {
      long startTimeNanos = System.currentTimeMillis();
      Iterator<URI> specURIs = getSpecURIsImpl();
      Instrumented.updateTimer(this.getURIsTimer, System.currentTimeMillis() - startTimeNanos, TimeUnit.MILLISECONDS);
      return specURIs;
    }
  }

  public abstract void addSpecImpl(Spec spec) throws IOException;
  public abstract Spec updateSpecImpl(Spec spec) throws IOException, SpecNotFoundException;
  public abstract boolean existsImpl(URI specUri) throws IOException;
  public abstract Spec getSpecImpl(URI specUri) throws IOException, SpecNotFoundException;
  public abstract boolean deleteSpecImpl(URI specUri) throws IOException;
  public abstract Collection<Spec> getSpecsImpl() throws IOException;
  public abstract Iterator<URI> getSpecURIsImpl() throws IOException;
}
