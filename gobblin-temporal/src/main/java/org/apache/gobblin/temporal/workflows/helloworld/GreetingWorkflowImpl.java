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

package org.apache.gobblin.temporal.workflows.helloworld;

import java.time.Duration;

import org.slf4j.Logger;

import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.Workflow;

import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.temporal.workflows.timing.TemporalEventTimer;
import org.apache.gobblin.temporal.workflows.trackingevent.activity.GobblinTrackingEventActivity;


public class GreetingWorkflowImpl implements GreetingWorkflow {

    private final Logger LOG = Workflow.getLogger(GreetingWorkflowImpl.class);

    /*
     * At least one of the following options needs to be defined:
     * - setStartToCloseTimeout
     * - setScheduleToCloseTimeout
     */
    ActivityOptions options = ActivityOptions.newBuilder()
            .setStartToCloseTimeout(Duration.ofSeconds(60))
            .build();

    /*
     * Define the HelloWorldActivity stub. Activity stubs are proxies for activity invocations that
     * are executed outside of the workflow thread on the activity worker, that can be on a
     * different host. Temporal is going to dispatch the activity results back to the workflow and
     * unblock the stub as soon as activity is completed on the activity worker.
     *
     * The activity options that were defined above are passed in as a parameter.
     */
    private final FormatActivity formatActivity = Workflow.newActivityStub(FormatActivity.class, options);
    private final GobblinTrackingEventActivity timerActivity = Workflow.newActivityStub(GobblinTrackingEventActivity.class, options);

    // This is the entry point to the Workflow.
    @Override
    public String getGreeting(String name, EventSubmitter eventSubmitter) {
        /**
         * Example of the {@link TemporalEventTimer.Factory} invoking child activity for instrumentation.
         */
        TemporalEventTimer.Factory timerFactory = new TemporalEventTimer.Factory(timerActivity, eventSubmitter);
        try (TemporalEventTimer timer = timerFactory.get("getGreetingTime")) {
            LOG.info("Executing getGreeting");
            return formatActivity.composeGreeting(name);
        }
    }
}
