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

package org.apache.gobblin.cluster;

import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskFactory;

import com.typesafe.config.Config;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PathUtils;


/**
 * <p> A {@link TaskFactory} that creates {@link GobblinHelixJobTask}
 * to run task driver logic.
 */
@Slf4j
class GobblinHelixJobFactory implements TaskFactory {
  protected HelixJobsMapping jobsMapping;

  protected TaskRunnerSuiteBase.Builder builder;
  @Getter
  protected GobblinHelixJobLauncherMetrics launcherMetrics;
  @Getter
  protected GobblinHelixJobTask.GobblinHelixJobTaskMetrics jobTaskMetrics;
  @Getter
  protected GobblinHelixMetrics helixMetrics;

  private void initJobMapping(TaskRunnerSuiteBase.Builder builder) {
    Config sysConfig = builder.getConfig();
    Path appWorkDir = builder.getAppWorkPath();
    URI rootPathUri = PathUtils.getRootPath(appWorkDir).toUri();
    this.jobsMapping = new HelixJobsMapping(sysConfig,
                                            rootPathUri,
                                            appWorkDir.toString());
  }

  public GobblinHelixJobFactory(TaskRunnerSuiteBase.Builder builder, MetricContext metricContext) {

    this.builder = builder;
    initJobMapping(this.builder);

    // initialize job related metrics (planning jobs)
    int metricsWindowSizeInMin = ConfigUtils.getInt(builder.getConfig(),
        ConfigurationKeys.METRIC_TIMER_WINDOW_SIZE_IN_MINUTES,
        ConfigurationKeys.DEFAULT_METRIC_TIMER_WINDOW_SIZE_IN_MINUTES);

    this.launcherMetrics = new GobblinHelixJobLauncherMetrics("launcherInJobFactory",
        metricContext,
        metricsWindowSizeInMin);
    this.jobTaskMetrics = new GobblinHelixJobTask.GobblinHelixJobTaskMetrics(
        metricContext,
        metricsWindowSizeInMin);
    this.helixMetrics = new GobblinHelixMetrics("helixMetricsInJobFactory",
        metricContext,
        metricsWindowSizeInMin);
  }

  @Override
  public Task createNewTask(TaskCallbackContext context) {
    return new GobblinHelixJobTask(context,
        this.jobsMapping,
        this.builder,
        this.launcherMetrics,
        this.jobTaskMetrics,
        this.helixMetrics);
  }
}
