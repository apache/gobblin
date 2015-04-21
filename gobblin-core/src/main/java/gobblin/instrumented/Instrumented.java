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
import java.util.Random;

import gobblin.configuration.State;
import gobblin.converter.Converter;
import gobblin.fork.ForkOperator;
import gobblin.metrics.MetricContext;
import gobblin.metrics.Tag;
import gobblin.qualitychecker.row.RowLevelPolicy;
import gobblin.source.extractor.Extractor;
import gobblin.writer.DataWriter;


/**
 * Provides simple instrumentation for gobblin-core components.
 *
 * Creates {@link gobblin.metrics.MetricContext}. Tries to read the name of the parent context
 * from key "metrics.context.name" at state, and tries to get the parent context by name from
 * the {@link gobblin.metrics.MetricContext} registry (the parent context must be registered).
 *
 * Automatically adds two tags to the inner context:
 * - component: attempts to determine which component type within gobblin-api generated this instace.
 * - class: the specific class of the object that generated this instace of Instrumented
 *
 */
public class Instrumented implements Closeable {

  public static final String METRIC_CONTEXT_NAME_KEY = "metrics.context.name";
  protected MetricContext metricContext;
  private int randomId;

  @SuppressWarnings("unchecked")
  public Instrumented(State state, Class klazz) {
    this.randomId = (new Random()).nextInt();

    String component = "unknown";
    if(Converter.class.isAssignableFrom(klazz)) {
      component = "converter";
    } else if(ForkOperator.class.isAssignableFrom(klazz)) {
      component = "forkOperator";
    } else if(RowLevelPolicy.class.isAssignableFrom(klazz)) {
      component = "rowLevelPolicy";
    } else if(Extractor.class.isAssignableFrom(klazz)) {
      component = "extractor";
    } else if(DataWriter.class.isAssignableFrom(klazz)) {
      component = "writer";
    }

    Tag<String> componentTag = new Tag("component", component);
    Tag<String> classTag = new Tag("class", klazz.getCanonicalName());

    MetricContext parentContext;
    MetricContext.Builder builder = state.contains(METRIC_CONTEXT_NAME_KEY) &&
        (parentContext = MetricContext.getContext(state.getProp(METRIC_CONTEXT_NAME_KEY))) != null ?
        parentContext.childBuilder(klazz.getCanonicalName() + "." + this.randomId) :
        MetricContext.builder(klazz.getCanonicalName() + "." + this.randomId);
    this.metricContext = builder.
        addTag(componentTag).
        addTag(classTag).
        build();
  }

  public MetricContext getContext() {
    return metricContext;
  }

  @Override
  public void close()
      throws IOException {
    metricContext.close();
  }
}
