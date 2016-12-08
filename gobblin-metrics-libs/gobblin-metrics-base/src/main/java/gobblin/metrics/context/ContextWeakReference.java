/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metrics.context;

import lombok.Getter;

import java.lang.ref.WeakReference;

import gobblin.metrics.InnerMetricContext;
import gobblin.metrics.MetricContext;
import gobblin.metrics.RootMetricContext;


/**
 * {@link WeakReference} to a {@link MetricContext} used to notify the {@link RootMetricContext} of garbage collection
 * of {@link MetricContext}s.
 */
@Getter
public class ContextWeakReference extends WeakReference<MetricContext> {

  private final InnerMetricContext innerContext;

  public ContextWeakReference(MetricContext referent, InnerMetricContext innerContext) {
    super(referent, RootMetricContext.get().getReferenceQueue());
    this.innerContext = innerContext;
  }

}
