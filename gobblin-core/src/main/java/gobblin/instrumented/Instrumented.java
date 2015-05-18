/*
 * (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.instrumented;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import gobblin.Constructs;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.GobblinMetricsRegistry;
import gobblin.configuration.State;
import gobblin.converter.Converter;
import gobblin.fork.ForkOperator;
import gobblin.metrics.MetricContext;
import gobblin.metrics.Tag;
import gobblin.qualitychecker.row.RowLevelPolicy;
import gobblin.source.extractor.Extractor;
import gobblin.util.Decorator;
import gobblin.writer.DataWriter;


/**
 * Provides simple instrumentation for gobblin-core components.
 *
 * <p>
 * Creates {@link gobblin.metrics.MetricContext}. Tries to read the name of the parent context
 * from key "metrics.context.name" at state, and tries to get the parent context by name from
 * the {@link gobblin.metrics.MetricContext} registry (the parent context must be registered).
 * </p>
 *
 * <p>
 * Automatically adds two tags to the inner context:
 * <ul>
 * <li> component: attempts to determine which component type within gobblin-api generated this instance. </li>
 * <li> class: the specific class of the object that generated this instance of Instrumented </li>
 * </ul>
 * </p>
 *
 */
public class Instrumented implements Instrumentable, Closeable {

  public static final String METRIC_CONTEXT_NAME_KEY = "metrics.context.name";
  public static final Random RAND = new Random();

  private final boolean instrumentationEnabled;
  protected final MetricContext metricContext;
  protected final Closer closer;

  /**
   * Gets metric context with no additional tags.
   * See {@link #getMetricContext(State, Class, List)}
   */
  public static MetricContext getMetricContext(State state, Class<?> klazz) {
    return getMetricContext(state, klazz, new ArrayList<Tag<?>>());
  }

  /**
   * Get a {@link gobblin.metrics.MetricContext} to be used by an object needing instrumentation.
   *
   * <p>
   * This method will read the property "metrics.context.name" from the input State, and will attempt
   * to find a MetricContext with that name in the global instance of {@link gobblin.metrics.GobblinMetricsRegistry}.
   * If it succeeds, the generated MetricContext will be a child of the retrieved Context, otherwise it will
   * be a parent-less context.
   * </p>
   * <p>
   * The method will automatically add two tags to the context:
   * <ul>
   *  <li> construct will contain the name of the {@link gobblin.Constructs} that klazz represents. </li>
   *  <li> class will contain the canonical name of the input class. </li>
   * </ul>
   * </p>
   *
   * @param state {@link gobblin.configuration.State} used to find the parent MetricContext.
   * @param klazz Class of the object needing instrumentation.
   * @param tags Additional tags to add to the returned context.
   * @return A {@link gobblin.metrics.MetricContext} with the appropriate tags and parent.
   */
  public static MetricContext getMetricContext(State state, Class<?> klazz, List<Tag<?>> tags) {
    int randomId = RAND.nextInt();

    Constructs construct = null;
    if(Converter.class.isAssignableFrom(klazz)) {
      construct = Constructs.CONVERTER;
    } else if(ForkOperator.class.isAssignableFrom(klazz)) {
      construct = Constructs.FORK_OPERATOR;
    } else if(RowLevelPolicy.class.isAssignableFrom(klazz)) {
      construct = Constructs.ROW_QUALITY_CHECKER;
    } else if(Extractor.class.isAssignableFrom(klazz)) {
      construct = Constructs.EXTRACTOR;
    } else if(DataWriter.class.isAssignableFrom(klazz)) {
      construct = Constructs.WRITER;
    }

    List<Tag<?>> generatedTags = Lists.newArrayList();
    if (construct != null) {
      generatedTags.add(new Tag<String>("construct", construct.toString()));
    }
    generatedTags.add(new Tag<String>("class", klazz.isAnonymousClass() ? "anonymous" : klazz.getCanonicalName()));

    GobblinMetrics gobblinMetrics;
    MetricContext.Builder builder = state.contains(METRIC_CONTEXT_NAME_KEY) &&
        (gobblinMetrics = GobblinMetricsRegistry.getInstance().get(state.getProp(METRIC_CONTEXT_NAME_KEY))) != null ?
        gobblinMetrics.getMetricContext().childBuilder(klazz.getCanonicalName() + "." + randomId) :
        MetricContext.builder(klazz.getCanonicalName() + "." + randomId);
    return builder.
        addTags(generatedTags).
        addTags(tags).
        build();
  }

  /**
   * Determines whether an object or, if it is a decorator, any object on its lineage, is already instrumented.
   * @param obj Object to analyze.
   * @return Whether the object is instrumented.
   */
  public static boolean isLineageInstrumented(Object obj) {
    List<Object> lineage = Lists.newArrayList(obj);
    if(obj instanceof Decorator) {
      lineage = ((Decorator) obj).getDecoratorLineage();
    }

    for(Object node : lineage) {
      if(node instanceof Instrumentable) {
        return true;
      }
    }

    return false;
  }

  /**
   * Updates a timer only if it is defined.
   * @param timer an Optional&lt;{@link com.codahale.metrics.Timer}&gt;
   * @param duration
   * @param unit
   */
  public static void updateTimer(Optional<Timer> timer, final long duration, final TimeUnit unit) {
    timer.transform(new Function<Timer, Timer>() {
      @Override
      public Timer apply(Timer input) {
        input.update(duration, unit);
        return input;
      }
    });
  }

  /**
   * Marks a meter only if it is defined.
   * @param meter an Optional&lt;{@link com.codahale.metrics.Meter}&gt;
   */
  public static void markMeter(Optional<Meter> meter) {
    markMeter(meter, 1);
  }

  /**
   * Marks a meter only if it is defined.
   * @param meter an Optional&lt;{@link com.codahale.metrics.Meter}&gt;
   * @param value value to mark
   */
  public static void markMeter(Optional<Meter> meter, final int value) {
    meter.transform(new Function<Meter, Meter>() {
      @Override
      public Meter apply(Meter input) {
        input.mark(value);
        return input;
      }
    });
  }

  @SuppressWarnings("unchecked")
  public Instrumented(State state, Class<?> klazz) {
    this(state, klazz, ImmutableList.<Tag<?>>of());
  }

  @SuppressWarnings("unchecked")
  public Instrumented(State state, Class<?> klazz, List<Tag<?>> tags) {
    this.closer = Closer.create();
    this.instrumentationEnabled = GobblinMetrics.isEnabled(state);
    this.metricContext = closer.register(getMetricContext(state, klazz, tags));
  }

  @Override
  public List<Tag<?>> generateTags(State state) {
    return Lists.newArrayList();
  }

  @Override
  public boolean isInstrumentationEnabled() {
    return this.instrumentationEnabled;
  }

  @Override
  public MetricContext getMetricContext() {
    return this.metricContext;
  }

  @Override
  public void close()
      throws IOException {
    this.closer.close();
  }

}
