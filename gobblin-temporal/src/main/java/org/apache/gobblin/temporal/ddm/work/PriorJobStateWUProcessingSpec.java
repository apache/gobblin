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
import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.hadoop.fs.FileSystem;

import org.apache.gobblin.metrics.Tag;
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
  @NonNull private List<Tag<?>> tags = new ArrayList<>();
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
      return new EventSubmitterContext.Builder()
          .addTags(this.getTags())
          .withGaaSJobProps(jobState.getProperties())
          .setNamespace(JobMetrics.NAMESPACE)
          .build();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}
