/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metrics.reporter;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.typesafe.config.Config;

import lombok.Getter;

import gobblin.metrics.context.ReportableContext;


/**
 * Stores {@link ReportableContext} that is should report in a list.
 */
public class ContextStoreReporter extends ScheduledReporter {

  @Getter
  private final List<ReportableContext> reportedContexts;

  public ContextStoreReporter(String name, Config config) {
    super(name, config);
    this.reportedContexts = Lists.newArrayList();
  }

  @Override
  protected void report(ReportableContext context) {
    this.reportedContexts.add(context);
  }

  @Override
  public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers,
      Map<String, Object> tags) {
    // Noop
  }

  public Set<ReportableContext> getContextsToReport() {
    return Sets.newHashSet(this.getMetricContextsToReport());
  }
}
