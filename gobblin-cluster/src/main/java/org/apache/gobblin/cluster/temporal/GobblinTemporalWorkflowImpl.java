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

// @@@SNIPSTART hello-world-project-template-java-workflow
package org.apache.gobblin.cluster.temporal;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Timestamp;

import io.temporal.activity.ActivityOptions;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.workflowservice.v1.GetWorkflowExecutionHistoryRequest;
import io.temporal.api.workflowservice.v1.GetWorkflowExecutionHistoryResponse;
import io.temporal.common.RetryOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.workflow.Workflow;

import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.GobblinEventBuilder;

import static org.apache.gobblin.cluster.GobblinTemporalClusterManager.createServiceStubs;


public class GobblinTemporalWorkflowImpl implements GobblinTemporalWorkflow {

    /*
     * At least one of the following options needs to be defined:
     * - setStartToCloseTimeout
     * - setScheduleToCloseTimeout
     */

    private final RetryOptions retryoptions = RetryOptions.newBuilder()
        .setMaximumAttempts(1)
        .build();

    int yearPeriod = 365 * 24 * 60 * 60;
    private final ActivityOptions options = ActivityOptions.newBuilder()
            .setStartToCloseTimeout(Duration.ofSeconds(yearPeriod))
            .setRetryOptions(retryoptions)
            .build();

    /*
     * Define the GobblinTemporalActivity stub. Activity stubs are proxies for activity invocations that
     * are executed outside of the workflow thread on the activity worker, that can be on a
     * different host. Temporal is going to dispatch the activity results back to the workflow and
     * unblock the stub as soon as activity is completed on the activity worker.
     *
     * The activity options that were defined above are passed in as a parameter.
     */
    private final GobblinTemporalActivity activity = Workflow.newActivityStub(GobblinTemporalActivity.class, options);
    private static final Logger LOGGER = LoggerFactory.getLogger(GobblinTemporalWorkflowImpl.class);
    // This is the entry point to the Workflow.
    @Override
    public String getGreeting(String name) {
        /**
         * If there were other Activity methods they would be orchestrated here or from within other Activities.
         * This is a blocking call that returns only after the activity has completed.
         */
        LOGGER.info("Workflow triggered");
        return activity.composeGreeting(name);
    }

    @Override
    public void runTask(Properties jobProps, String appWorkDir, String jobId, String workUnitFilePath, String jobStateFilePath)
        throws Exception{
        String workflowId = Workflow.getInfo().getWorkflowId();
        String runId = Workflow.getInfo().getRunId();
        WorkflowExecution execution = WorkflowExecution.newBuilder()
            .setWorkflowId(workflowId)
            .setRunId(runId)
            .build();

        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

        MetricContext metricContext = MetricContext.builder("TemporalWorkflowContext").build();
        EventSubmitter eventSubmitter = new EventSubmitter.Builder(metricContext, "gobblin.temporal").build();

        final long[] lastLoggedEventId = {0};
        executorService.scheduleAtFixedRate(() -> {
            try {
                GetWorkflowExecutionHistoryRequest request =
                    GetWorkflowExecutionHistoryRequest.newBuilder().setNamespace("gobblin-fastingest-internpoc").setExecution(execution).build();

                WorkflowServiceStubs workflowServiceStubs = createServiceStubs();
                GetWorkflowExecutionHistoryResponse response =
                    workflowServiceStubs.blockingStub().getWorkflowExecutionHistory(request);

                for (HistoryEvent event : response.getHistory().getEventsList()) {
                    // Only log events that are newer than the last one we logged
                    if (event.getEventId() > lastLoggedEventId[0]) {
                        Timestamp timestamp = event.getEventTime();
                        Instant instant = Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                            .withZone(ZoneId.systemDefault());
                        String formattedDateTime = formatter.format(instant);

                        GobblinEventBuilder eventBuilder = new GobblinEventBuilder("TemporalEvent");
                        eventBuilder.addMetadata("WorkflowId", workflowId);
                        eventBuilder.addMetadata("EventType", event.getEventType().name());
                        eventBuilder.addMetadata("EventTime", formattedDateTime);
                        // add metadata of workflow topic
                        eventSubmitter.submit(eventBuilder);

                        lastLoggedEventId[0] = event.getEventId();
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Error retrieving workflow history", e);
            }
        }, 0, 10, TimeUnit.SECONDS);

        activity.run(jobProps, appWorkDir, jobId, workUnitFilePath, jobStateFilePath, workflowId);

        executorService.shutdown();
    }
}