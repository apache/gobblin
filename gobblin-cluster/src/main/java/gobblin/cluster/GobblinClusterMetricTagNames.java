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

package gobblin.cluster;

import gobblin.annotation.Alpha;


/**
 * A central place for constants of {@link gobblin.metrics.MetricContext} tag names for a Gobblin cluster.
 *
 * @author Yinan Li
 */
@Alpha
public class GobblinClusterMetricTagNames {

  public static final String APPLICATION_NAME = "application.name";
  public static final String APPLICATION_ID = "application.id";
  public static final String HELIX_INSTANCE_NAME = "helix.instance.name";
  public static final String TASK_RUNNER_ID = "task.runner.id";
}
