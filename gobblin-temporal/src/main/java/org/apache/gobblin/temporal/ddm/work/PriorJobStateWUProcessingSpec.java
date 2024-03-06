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

package org.apache.gobblin.temporal.ddm.work;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.hadoop.fs.FileSystem;

import org.apache.gobblin.instrumented.GobblinMetricsKeys;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.util.JobMetrics;
import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.temporal.ddm.work.assistance.Help;
import org.apache.gobblin.temporal.workflows.metrics.EventSubmitterContext;


/**
 * Same as {@link WUProcessingSpec}, but for a "Work Fulfillment-only" workflow that leverages the {@link JobState} and
 * {@link org.apache.gobblin.source.workunit.WorkUnit}s previously persisted by another separate job execution.
 * Accordingly we wish to adjust/"spoof" our {@link EventSubmitterContext} to carry identifiers from that original job,
 * and to indicate that the processing workflow ought to perform job-level timing.
 */
@Data
@EqualsAndHashCode(callSuper = true) // to prevent findbugs warning - "equals method overrides equals in superclass and may not be symmetric"
@NoArgsConstructor // IMPORTANT: for jackson (de)serialization
public class PriorJobStateWUProcessingSpec extends WUProcessingSpec {
  @NonNull
  private List<Tag<?>> tags = new ArrayList<>();
  @NonNull private String metricsSuffix = GobblinTemporalConfigurationKeys.DEFAULT_GOBBLIN_TEMPORAL_JOB_METRICS_SUFFIX;

  public PriorJobStateWUProcessingSpec(URI fileSystemUri, String workUnitsDir, EventSubmitterContext eventSubmitterContext) {
    super(fileSystemUri, workUnitsDir, eventSubmitterContext);
  }

  @JsonIgnore
  @Override
  public boolean isToDoJobLevelTiming() {
    return true;
  }

  @Override
  public @NonNull EventSubmitterContext getEventSubmitterContext() {
    // NOTE: We are using the metrics tags from Job Props to create the metric context for the timer and NOT
    // the deserialized jobState from HDFS that is created by the real distcp job. This is because the AZ runtime
    // settings we want are for the job launcher that launched this Yarn job.
    try {
      FileSystem fs = Help.loadFileSystemForce(this);
      JobState jobState = Help.loadJobStateUncached(this, fs);
      List<Tag<?>> tagsFromCurrentJob = this.getTags();
      String metricsSuffix = this.getMetricsSuffix();
      List<Tag<?>> tags = this.calcMergedTags(tagsFromCurrentJob, metricsSuffix, jobState);
      return new EventSubmitterContext(tags, JobMetrics.NAMESPACE);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  private List<Tag<?>> calcMergedTags(List<Tag<?>> tagsFromCurJob, String metricsSuffix, JobState jobStateFromHdfs) {
    // Construct new tags list by combining subset of tags on HDFS job state and the rest of the fields from the current job
    Map<String, Tag<?>> tagsMap = new HashMap<>();
    Set<String> tagKeysFromJobState = new HashSet<>(Arrays.asList(
        TimingEvent.FlowEventConstants.FLOW_NAME_FIELD,
        TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD,
        TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD,
        TimingEvent.FlowEventConstants.JOB_NAME_FIELD,
        TimingEvent.FlowEventConstants.JOB_GROUP_FIELD));

    // Step 1, Add tags from the AZ props using the original job (the one that launched this yarn app)
    tagsFromCurJob.forEach(tag -> tagsMap.put(tag.getKey(), tag));

    // Step 2. Add tags from the jobState (the original MR job on HDFS)
    List<String> targetKeysToAddSuffix = Arrays.asList(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD);
    GobblinMetrics.getCustomTagsFromState(jobStateFromHdfs).stream()
        .filter(tag -> tagKeysFromJobState.contains(tag.getKey()))
        .forEach(tag -> {
          // Step 2a (optional): Add a suffix to the FLOW_NAME_FIELD AND FLOW_GROUP_FIELDS to prevent collisions when testing
          String value = targetKeysToAddSuffix.contains(tag.getKey())
              ? tag.getValue() + metricsSuffix
              : String.valueOf(tag.getValue());
          tagsMap.put(tag.getKey(), new Tag<>(tag.getKey(), value));
        });

    // Step 3: Overwrite any pre-existing metadata with name of the current caller
    tagsMap.put(GobblinMetricsKeys.CLASS_META, new Tag<>(GobblinMetricsKeys.CLASS_META, getClass().getCanonicalName()));
    return new ArrayList<>(tagsMap.values());
  }
}
