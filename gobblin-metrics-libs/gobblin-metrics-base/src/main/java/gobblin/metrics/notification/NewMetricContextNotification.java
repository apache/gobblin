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

package gobblin.metrics.notification;

import lombok.AllArgsConstructor;
import lombok.Getter;

import gobblin.metrics.InnerMetricContext;
import gobblin.metrics.MetricContext;


/**
 * {@link Notification} that a new {@link gobblin.metrics.MetricContext} has been created. Contains the
 * new {@link MetricContext}.
 */
@AllArgsConstructor
@Getter
public class NewMetricContextNotification implements Notification {
  private final MetricContext metricContext;
  private final InnerMetricContext innerMetricContext;
}
