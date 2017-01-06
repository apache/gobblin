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

package gobblin.yarn;

/**
 * A central place for constants of {@link gobblin.metrics.MetricContext} tag names for Gobblin on Yarn.
 *
 * @author Yinan Li
 */
public class GobblinYarnMetricTagNames {

  public static final String YARN_APPLICATION_ATTEMPT_ID = "yarn.application.attempt.id";
  public static final String CONTAINER_ID = "yarn.container.id";
}
