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

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;

import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.metrics.event.EventName;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.EventMetadataUtils;
import org.apache.gobblin.runtime.JobContext;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.runtime.api.EventMetadataGenerator;


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
    String taskException = EventMetadataUtils.getTaskFailureExceptions(taskStates);
    String jobException = EventMetadataUtils.getJobFailureExceptions(jobContext.getJobState());
    ImmutableMap.Builder<String,String> metadataBuilder = ImmutableMap.builder();
    if (jobContext.getJobState().contains(TimingEvent.FlowEventConstants.HIGH_WATERMARK_FIELD)) {
        metadataBuilder.put(TimingEvent.FlowEventConstants.HIGH_WATERMARK_FIELD,
            jobContext.getJobState().getProp(TimingEvent.FlowEventConstants.HIGH_WATERMARK_FIELD));
    }
    if (jobContext.getJobState().contains(TimingEvent.FlowEventConstants.LOW_WATERMARK_FIELD)) {
        metadataBuilder.put(TimingEvent.FlowEventConstants.LOW_WATERMARK_FIELD,
            jobContext.getJobState().getProp(TimingEvent.FlowEventConstants.LOW_WATERMARK_FIELD));
    }

    switch (eventName) {
        case JOB_COMPLETE:
            return metadataBuilder.put(PROCESSED_COUNT_KEY, Long.toString(EventMetadataUtils.getProcessedCount(taskStates))).build();
        case JOB_FAILED:
            return metadataBuilder.put(MESSAGE_KEY, taskException.length() != 0 ? taskException : jobException).build();
        default:
            break;
    }

    return metadataBuilder.build();
  }
}

