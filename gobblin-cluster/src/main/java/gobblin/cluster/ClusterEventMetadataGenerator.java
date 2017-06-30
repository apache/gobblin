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

package gobblin.cluster;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import gobblin.annotation.Alias;
import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.event.EventName;
import gobblin.runtime.EventMetadataUtils;
import gobblin.runtime.JobContext;
import gobblin.runtime.TaskState;
import gobblin.runtime.api.EventMetadataGenerator;


/**
 * {@link EventMetadataGenerator} that outputs the processed message count and error messages
 * used for job status tracking
 */
@Alias("cluster")
public class ClusterEventMetadataGenerator implements EventMetadataGenerator{
  public static final String PROCESSED_COUNT_KEY = "processedCount";
  public static final String MESSAGE_KEY = "message";

  public Map<String, String> getMetadata(JobContext jobContext, EventName eventName) {
    List<TaskState> taskStates = jobContext.getJobState().getTaskStates();

    switch (eventName) {
      case JOB_COMPLETE:
        return ImmutableMap.of(PROCESSED_COUNT_KEY, Long.toString(EventMetadataUtils.getProcessedCount(taskStates)));
      case JOB_FAILED:
        return ImmutableMap.of(MESSAGE_KEY, EventMetadataUtils.getTaskFailureExceptions(taskStates));
      default:
        break;
    }

    return ImmutableMap.of();
  }
}

