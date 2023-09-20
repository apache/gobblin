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

package org.apache.gobblin.temporal.workflows;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Async;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;
import java.time.Duration;

/** {@link com.linkedin.temporal.app.workflow.nesting.NestingExecWorkflow} for {@link IllustrationTask} */
public class NestingExecWorkflowImpl
        extends AbstractNestingExecWorkflowImpl<IllustrationTask, String> {

    // RetryOptions specify how to automatically handle retries when Activities fail.
    private static final RetryOptions ACTIVITY_RETRY_OPTS = RetryOptions.newBuilder()
            .setInitialInterval(Duration.ofSeconds(1))
            .setMaximumInterval(Duration.ofSeconds(100))
            .setBackoffCoefficient(2)
            .setMaximumAttempts(3)
            .build();

    private static final ActivityOptions ACTIVITY_OPTS = ActivityOptions.newBuilder()
            .setStartToCloseTimeout(Duration.ofSeconds(10))
            .setRetryOptions(ACTIVITY_RETRY_OPTS)
            .build();

    private final IllustrationTaskActivity activityStub =
            Workflow.newActivityStub(IllustrationTaskActivity.class, ACTIVITY_OPTS);

    @Override
    protected Promise<String> launchAsyncActivity(final IllustrationTask t) {
        return Async.function(activityStub::doTask, t);
    }
}
