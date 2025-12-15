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
package org.apache.gobblin.temporal.ddm.activity;

import java.util.List;
import java.util.Properties;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.temporal.ddm.work.TimeBudget;
import org.apache.gobblin.temporal.ddm.work.WorkUnitsSizeSummary;
import org.apache.gobblin.temporal.dynamic.ScalingDirective;



/**
 * Activity to suggest the Dynamic Scaling warranted to complete processing of some amount of {@link org.apache.gobblin.source.workunit.WorkUnit}s
 * within {@link TimeBudget}, through a combination of Workforce auto-scaling and Worker right-sizing.
 *
 * As with all {@link ActivityInterface}s, this is stateless, so the {@link ScalingDirective}(s) returned "stand alone", presuming nothing of current
 * {@link org.apache.gobblin.temporal.dynamic.WorkforceStaffing}.  It thus falls to the caller to coordinate whether to apply the directive(s) as-is,
 * or first to adjust in light of scaling levels already in the current {@link org.apache.gobblin.temporal.dynamic.WorkforcePlan}.
 */
@ActivityInterface
public interface RecommendScalingForWorkUnits {

  /**
   * Recommend the {@link ScalingDirective}s to process the {@link WorkUnit}s of {@link WorkUnitsSizeSummary} within {@link TimeBudget}.
   *
   * @param remainingWork may characterize a newly-generated batch of `WorkUnit`s for which no processing has yet begun - or be the sub-portion
   *                      of an in-progress job that still awaits processing
   * @param sourceClass contextualizes the `WorkUnitsSizeSummary` and should name a {@link org.apache.gobblin.source.Source}
   * @param timeBudget the remaining target duration for processing the summarized `WorkUnit`s
   * @param jobProps all job props, to either guide the recommendation or better contextualize the nature of the `remainingWork`
   * @return the {@link ScalingDirective}s to process the summarized {@link WorkUnit}s within {@link TimeBudget}
   */
  @ActivityMethod
  List<ScalingDirective> recommendScaling(WorkUnitsSizeSummary remainingWork, String sourceClass, TimeBudget timeBudget, Properties jobProps);
}
