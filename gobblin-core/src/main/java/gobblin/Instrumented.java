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

package gobblin;

import java.io.Closeable;
import java.io.IOException;

import gobblin.configuration.State;
import gobblin.metrics.MetricContext;
import gobblin.metrics.Tag;


public class Instrumented implements Closeable {

  public static final String METRIC_CONTEXT_NAME_KEY = "metrics.context.name";
  protected MetricContext metricContext;

  @SuppressWarnings("unchecked")
  public Instrumented(State state, Class klazz) {
    Tag<String> componentTag = new Tag("component", "converter");
    Tag<String> classTag = new Tag("class", this.getClass().getCanonicalName());

    MetricContext parentContext;
    MetricContext.Builder builder = state.contains(METRIC_CONTEXT_NAME_KEY) &&
        (parentContext = MetricContext.getContext(state.getProp(METRIC_CONTEXT_NAME_KEY))) != null ?
        parentContext.childBuilder(klazz.getCanonicalName()) :
        MetricContext.builder(klazz.getClass().getCanonicalName());
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
