package org.apache.gobblin.temporal.ddm.workflow.impl;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Async;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;

import java.time.Duration;

import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.temporal.ddm.activity.CommitActivity;
import org.apache.gobblin.temporal.ddm.work.WUProcessingSpec;
import org.apache.gobblin.temporal.ddm.work.WorkUnitClaimCheck;
import org.apache.gobblin.temporal.ddm.workflow.CommitStepWorkflow;


@Slf4j
public class CommitStepWorkflowImpl implements CommitStepWorkflow {

  private static final RetryOptions ACTIVITY_RETRY_OPTS = RetryOptions.newBuilder()
      .setInitialInterval(Duration.ofSeconds(3))
      .setMaximumInterval(Duration.ofSeconds(100))
      .setBackoffCoefficient(2)
      .setMaximumAttempts(4)
      .build();

  private static final ActivityOptions ACTIVITY_OPTS = ActivityOptions.newBuilder()
      .setStartToCloseTimeout(Duration.ofSeconds(999))
      .setRetryOptions(ACTIVITY_RETRY_OPTS)
      .build();

  private final CommitActivity activityStub = Workflow.newActivityStub(CommitActivity.class, ACTIVITY_OPTS);

  @Override
  public int commit(WUProcessingSpec workSpec) {
    Promise<Integer> result = Async.function(activityStub::commit, workSpec);
    return result.get();
  }
}
