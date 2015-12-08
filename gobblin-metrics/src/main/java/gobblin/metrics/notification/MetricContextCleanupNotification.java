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

package gobblin.metrics.notification;

import lombok.Data;

import gobblin.metrics.InnerMetricContext;


/**
 * {@link Notification} that a {@link gobblin.metrics.MetricContext} has been garbage collected. Contains the
 * {@link InnerMetricContext} associated which still includes {@link gobblin.metrics.Metric}s and
 * {@link gobblin.metrics.Tag}s.
 */
@Data
public class MetricContextCleanupNotification implements Notification {
  private final InnerMetricContext metricContext;
}
