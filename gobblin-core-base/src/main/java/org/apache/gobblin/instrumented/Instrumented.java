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

package org.apache.gobblin.instrumented;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import org.apache.gobblin.Constructs;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.fork.ForkOperator;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.GobblinMetricsRegistry;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.qualitychecker.row.RowLevelPolicy;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.util.DecoratorUtils;
import org.apache.gobblin.writer.DataWriter;


/**
 * Provides simple instrumentation for gobblin-core components.
 *
 * <p>
 * Creates {@link org.apache.gobblin.metrics.MetricContext}. Tries to read the name of the parent context
 * from key "metrics.context.name" at state, and tries to get the parent context by name from
 * the {@link org.apache.gobblin.metrics.MetricContext} registry (the parent context must be registered).
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

  public static final String METRIC_CONTEXT_NAME_KEY = ConfigurationKeys.METRIC_CONTEXT_NAME_KEY;
  public static final Random RAND = new Random();

  private final boolean instrumentationEnabled;
  protected MetricContext metricContext;
  protected final Closer closer;

  /**
   * Gets metric context with no additional tags.
   * See {@link #getMetricContext(State, Class, List)}
   */
  public static MetricContext getMetricContext(State state, Class<?> klazz) {
    return getMetricContext(state, klazz, new ArrayList<Tag<?>>());
  }

  /**
   * Get a {@link org.apache.gobblin.metrics.MetricContext} to be used by an object needing instrumentation.
   *
   * <p>
   * This method will read the property "metrics.context.name" from the input State, and will attempt
   * to find a MetricContext with that name in the global instance of {@link org.apache.gobblin.metrics.GobblinMetricsRegistry}.
   * If it succeeds, the generated MetricContext will be a child of the retrieved Context, otherwise it will
   * be a parent-less context.
   * </p>
   * <p>
   * The method will automatically add two tags to the context:
   * <ul>
   *  <li> construct will contain the name of the {@link org.apache.gobblin.Constructs} that klazz represents. </li>
   *  <li> class will contain the canonical name of the input class. </li>
   * </ul>
   * </p>
   *
   * @param state {@link org.apache.gobblin.configuration.State} used to find the parent MetricContext.
   * @param klazz Class of the object needing instrumentation.
   * @param tags Additional tags to add to the returned context.
   * @return A {@link org.apache.gobblin.metrics.MetricContext} with the appropriate tags and parent.
   */
  public static MetricContext getMetricContext(State state, Class<?> klazz, List<Tag<?>> tags) {
    int randomId = RAND.nextInt(Integer.MAX_VALUE);

    List<Tag<?>> generatedTags = Lists.newArrayList();
    Constructs construct = null;
    if (Converter.class.isAssignableFrom(klazz)) {
      construct = Constructs.CONVERTER;
    } else if (ForkOperator.class.isAssignableFrom(klazz)) {
      construct = Constructs.FORK_OPERATOR;
    } else if (RowLevelPolicy.class.isAssignableFrom(klazz)) {
      construct = Constructs.ROW_QUALITY_CHECKER;
    } else if (Extractor.class.isAssignableFrom(klazz)) {
      construct = Constructs.EXTRACTOR;
    } else if (DataWriter.class.isAssignableFrom(klazz)) {
      construct = Constructs.WRITER;
    }

    if (construct != null) {
      generatedTags.add(new Tag<>(GobblinMetricsKeys.CONSTRUCT_META, construct.toString()));
    }
    if (!klazz.isAnonymousClass()) {
      generatedTags.add(new Tag<>(GobblinMetricsKeys.CLASS_META, klazz.getCanonicalName()));
    }

    Optional<GobblinMetrics> gobblinMetrics = state.contains(METRIC_CONTEXT_NAME_KEY)
        ? GobblinMetricsRegistry.getInstance().get(state.getProp(METRIC_CONTEXT_NAME_KEY))
        : Optional.<GobblinMetrics> absent();

    MetricContext.Builder builder = gobblinMetrics.isPresent()
        ? gobblinMetrics.get().getMetricContext().childBuilder(klazz.getCanonicalName() + "." + randomId)
        : MetricContext.builder(klazz.getCanonicalName() + "." + randomId);
    return builder.addTags(generatedTags).addTags(tags).build();
  }

  /**
   * Generates a new {@link org.apache.gobblin.metrics.MetricContext} with the parent and tags taken from the reference context.
   * Allows replacing {@link org.apache.gobblin.metrics.Tag} with new input tags.
   * This method will not copy any {@link org.apache.gobblin.metrics.Metric} contained in the reference {@link org.apache.gobblin.metrics.MetricContext}.
   *
   * @param context Reference {@link org.apache.gobblin.metrics.MetricContext}.
   * @param newTags New {@link org.apache.gobblin.metrics.Tag} to apply to context. Repeated keys will override old tags.
   * @param name Name of the new {@link org.apache.gobblin.metrics.MetricContext}.
   *             If absent or empty, will modify old name by adding a random integer at the end.
   * @return Generated {@link org.apache.gobblin.metrics.MetricContext}.
   */
  public static MetricContext newContextFromReferenceContext(MetricContext context, List<Tag<?>> newTags,
      Optional<String> name) {

    String newName = name.orNull();

    if (Strings.isNullOrEmpty(newName)) {
      UUID uuid = UUID.randomUUID();
      String randomIdPrefix = "uuid:";

      String oldName = context.getName();
      List<String> splitName = Strings.isNullOrEmpty(oldName) ? Lists.<String> newArrayList()
          : Lists.newArrayList(Splitter.on(".").splitToList(oldName));
      if (splitName.size() > 0 && StringUtils.startsWith(Iterables.getLast(splitName), randomIdPrefix)) {
        splitName.set(splitName.size() - 1, String.format("%s%s", randomIdPrefix, uuid.toString()));
      } else {
        splitName.add(String.format("%s%s", randomIdPrefix, uuid.toString()));
      }
      newName = Joiner.on(".").join(splitName);
    }

    MetricContext.Builder builder = context.getParent().isPresent() ? context.getParent().get().childBuilder(newName)
        : MetricContext.builder(newName);
    return builder.addTags(context.getTags()).addTags(newTags).build();
  }

  /**
   * Determines whether an object or, if it is a {@link org.apache.gobblin.util.Decorator}, any object on its lineage,
   * is of class {@link org.apache.gobblin.instrumented.Instrumentable}.
   * @param obj Object to analyze.
   * @return Whether the lineage is instrumented.
   */
  public static boolean isLineageInstrumented(Object obj) {

    List<Object> lineage = DecoratorUtils.getDecoratorLineage(obj);

    for (Object node : lineage) {
      if (node instanceof Instrumentable) {
        return true;
      }
    }

    return false;
  }

  /**
   * Returns a {@link com.codahale.metrics.Timer.Context} only if {@link org.apache.gobblin.metrics.MetricContext} is defined.
   * @param context an Optional&lt;{@link org.apache.gobblin.metrics.MetricContext}$gt;
   * @param name name of the timer.
   * @return an Optional&lt;{@link com.codahale.metrics.Timer.Context}$gt;
   */
  public static Optional<Timer.Context> timerContext(Optional<MetricContext> context, final String name) {
    return context.transform(new Function<MetricContext, Timer.Context>() {
      @Override
      public Timer.Context apply(@Nonnull MetricContext input) {
        return input.timer(name).time();
      }
    });
  }

  /**
   * Ends a {@link com.codahale.metrics.Timer.Context} only if it exists.
   * @param timer an Optional&lt;{@link com.codahale.metrics.Timer.Context}$gt;
   */
  public static void endTimer(Optional<Timer.Context> timer) {
    timer.transform(new Function<Timer.Context, Timer.Context>() {
      @Override
      public Timer.Context apply(@Nonnull Timer.Context input) {
        input.close();
        return input;
      }
    });
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
      public Timer apply(@Nonnull Timer input) {
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
  public static void markMeter(Optional<Meter> meter, final long value) {
    meter.transform(new Function<Meter, Meter>() {
      @Override
      public Meter apply(@Nonnull Meter input) {
        input.mark(value);
        return input;
      }
    });
  }

  /**
   * Sets the key {@link #METRIC_CONTEXT_NAME_KEY} to the specified name, in the given {@link State}.
   */
  public static void setMetricContextName(State state, String name) {
    state.setProp(Instrumented.METRIC_CONTEXT_NAME_KEY, name);
  }

  public Instrumented(State state, Class<?> klazz) {
    this(state, klazz, ImmutableList.<Tag<?>> of());
  }

  public Instrumented(State state, Class<?> klazz, List<Tag<?>> tags) {
    this.closer = Closer.create();
    this.instrumentationEnabled = GobblinMetrics.isEnabled(state);
    this.metricContext = this.closer.register(getMetricContext(state, klazz, tags));
  }

  /** Default with no additional tags */
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
  public void switchMetricContext(List<Tag<?>> tags) {
    this.metricContext = this.closer
        .register(Instrumented.newContextFromReferenceContext(this.metricContext, tags, Optional.<String> absent()));
  }

  @Override
  public void switchMetricContext(MetricContext context) {
    this.metricContext = context;
  }

  @Override
  public void close() throws IOException {
    this.closer.close();
  }

}
