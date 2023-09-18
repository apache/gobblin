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
