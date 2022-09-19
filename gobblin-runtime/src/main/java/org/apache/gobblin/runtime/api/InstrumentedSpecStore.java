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

  /** Record timing for an operation, when an `Optional<Timer>.isPresent()`, otherwise simply perform the operation w/o timing */
  static class OptionallyTimingInvoker {

    /** `j.u.function.Supplier` variant for an operation that may @throw IOException: preserves method signature, including checked exceptions */
    @FunctionalInterface
    public interface SupplierMayThrowIO<T> {
      public T get() throws IOException;
    }

    /** `j.u.function.Supplier` variant for an operation that may @throw IOException or SpecNotFoundException: preserves method signature exceptions */
    @FunctionalInterface
    public interface SupplierMayThrowBoth<T> {
      public T get() throws IOException, SpecNotFoundException;
    }

    private final Optional<Timer> timer;

    public OptionallyTimingInvoker(Optional<Timer> timer) {
      this.timer = timer != null ? timer : Optional.absent();
    }

    public <T> T invokeMayThrowIO(SupplierMayThrowIO<T> f) throws IOException {
      try {
        return invokeMayThrowBoth(() -> f.get());
      } catch (SpecNotFoundException e) {
        throw new RuntimeException("IMPOSSIBLE - prohibited by static checking!", e);
      }
    }

    public <T> T invokeMayThrowBoth(SupplierMayThrowBoth<T> f) throws IOException, SpecNotFoundException {
      if (timer.isPresent()) {
        final long startTimeNanos = System.nanoTime(); // ns resolution, being internal granularity of `metrics.Timer`
        final T result = f.get();
        timer.get().update(System.nanoTime() - startTimeNanos, TimeUnit.NANOSECONDS);
        return result;
      } else { // skip timing, when no `Timer`
        return f.get();
      }
    }
  }

  private OptionallyTimingInvoker getTimer;
  private OptionallyTimingInvoker existsTimer;
  private OptionallyTimingInvoker deleteTimer;
  private OptionallyTimingInvoker addTimer;
  private OptionallyTimingInvoker updateTimer;
  private OptionallyTimingInvoker getAllTimer;
  private OptionallyTimingInvoker getSizeTimer;
  private OptionallyTimingInvoker getURIsTimer;
  private OptionallyTimingInvoker getURIsWithTagTimer;
  private MetricContext metricContext;
  private final boolean instrumentationEnabled;

  public InstrumentedSpecStore(Config config, SpecSerDe specSerDe) {
    this.instrumentationEnabled = GobblinMetrics.isEnabled(new State(ConfigUtils.configToProperties(config)));
    this.metricContext = Instrumented.getMetricContext(new State(), getClass());
    this.getTimer = createTimingInvoker("-GET");
    this.existsTimer = createTimingInvoker("-EXISTS");
    this.deleteTimer = createTimingInvoker("-DELETE");
    this.addTimer = createTimingInvoker("-ADD");
    this.updateTimer = createTimingInvoker("-UPDATE");
    this.getAllTimer = createTimingInvoker("-GETALL");
    this.getSizeTimer = createTimingInvoker("-GETCOUNT");
    this.getURIsTimer = createTimingInvoker("-GETURIS");
    this.getURIsWithTagTimer = createTimingInvoker("-GETURISWITHTAG");
  }

  private OptionallyTimingInvoker createTimingInvoker(String suffix) {
    return new OptionallyTimingInvoker(createTimer(suffix));
  }

  private Optional<Timer> createTimer(String suffix) {
    return instrumentationEnabled
        ? Optional.of(this.metricContext.timer(MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX, getClass().getSimpleName(), suffix)))
        : Optional.absent();
  }

  @Override
  public boolean exists(URI specUri) throws IOException {
    return this.existsTimer.invokeMayThrowIO(() -> existsImpl(specUri));
  }

  @Override
  public void addSpec(Spec spec) throws IOException {
    this.addTimer.invokeMayThrowIO(() -> { addSpecImpl(spec); /* sadly, unable to infer `SupplierMayThrowIO<Void>`, thus explicitly... */ return null; });
  }

  @Override
  public boolean deleteSpec(URI specUri) throws IOException {
    return this.deleteTimer.invokeMayThrowIO(() -> deleteSpecImpl(specUri));
  }

  @Override
  public Spec getSpec(URI specUri) throws IOException, SpecNotFoundException {
    return this.getTimer.invokeMayThrowBoth(() -> getSpecImpl(specUri));
  }

  @Override
  public Collection<Spec> getSpecs(SpecSearchObject specSearchObject) throws IOException {
    // NOTE: uses same `getTimer` as `getSpec`; TODO: explore separating, since measuring different operation
    return this.getTimer.invokeMayThrowIO(() -> getSpecsImpl(specSearchObject));
  }

  @Override
  public Spec updateSpec(Spec spec) throws IOException, SpecNotFoundException {
    return this.updateTimer.invokeMayThrowBoth(() -> updateSpecImpl(spec));
  }

  @Override
  public Spec updateSpec(Spec spec, long modifiedWatermark) throws IOException, SpecNotFoundException {
    return this.updateTimer.invokeMayThrowBoth(() -> updateSpecImpl(spec, modifiedWatermark));
  }

  @Override
  public Collection<Spec> getSpecs() throws IOException {
    return this.getAllTimer.invokeMayThrowIO(() -> getSpecsImpl());
  }

  @Override
  public Iterator<URI> getSpecURIs() throws IOException {
    return this.getURIsTimer.invokeMayThrowIO(() -> getSpecURIsImpl());
  }

  @Override
  public Iterator<URI> getSpecURIsWithTag(String tag) throws IOException {
    return this.getURIsWithTagTimer.invokeMayThrowIO(() -> getSpecURIsWithTagImpl(tag));
  }

  @Override
  public Collection<Spec> getSpecs(int start, int count) throws IOException {
    return this.getTimer.invokeMayThrowIO(() -> getSpecsImpl(start, count));
  }

  @Override
  public int getSize() throws IOException {
    return this.getSizeTimer.invokeMayThrowIO(() -> getSizeImpl());
  }

  public Spec updateSpecImpl(Spec spec, long modifiedWatermark) throws IOException, SpecNotFoundException{
    return updateSpecImpl(spec);
  }

  public abstract void addSpecImpl(Spec spec) throws IOException;
  public abstract Spec updateSpecImpl(Spec spec) throws IOException, SpecNotFoundException;
  public abstract boolean existsImpl(URI specUri) throws IOException;
  public abstract Spec getSpecImpl(URI specUri) throws IOException, SpecNotFoundException;
  public abstract boolean deleteSpecImpl(URI specUri) throws IOException;
  public abstract Collection<Spec> getSpecsImpl() throws IOException;
  public abstract Iterator<URI> getSpecURIsImpl() throws IOException;
  public abstract Iterator<URI> getSpecURIsWithTagImpl(String tag) throws IOException;
  public abstract int getSizeImpl() throws IOException;
  public abstract Collection<Spec> getSpecsImpl(int start, int count) throws IOException;

  /** child classes can implement this if they want to get specs using {@link SpecSearchObject} */
  public Collection<Spec> getSpecsImpl(SpecSearchObject specUri) throws IOException {
    throw new UnsupportedOperationException();
  }
}
