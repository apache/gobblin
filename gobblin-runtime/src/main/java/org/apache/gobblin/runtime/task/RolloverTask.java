package org.apache.gobblin.runtime.task;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.publisher.DataPublisher;
import org.apache.gobblin.publisher.NoopPublisher;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.source.workunit.WorkUnit;

/**
 * Mostly used in the case when failedTasks from previous persisted state store is exceeding the maximum number
 * of workunits that can be handled by a single execution.
 * In such case, failed-but-to-retry workunits needs to be rolled over to next execution.
 */
public class RolloverTask extends BaseAbstractTask {
  private RolloverTask(TaskContext taskContext) {
    super(taskContext);
  }

  /**
   * @return A {@link WorkUnit} that will run a {@link RolloverTask}.
   */
  public static WorkUnit rolloverWorkunit() {
    WorkUnit workUnit = new WorkUnit();
    TaskUtils.setTaskFactoryClass(workUnit, RolloverTask.Factory.class);
    return workUnit;
  }

  /**
   * ROLLOVERED represents that current task won't be executed while it will be resumed in the next execution
   * if quota permitted.
   */
  public void commit() {
    this.workingState = WorkUnitState.WorkingState.ROLLOVERED;
  }

  @Override
  public void run() {
    this.workingState = WorkUnitState.WorkingState.ROLLOVERED;
  }

  /**
   * The factory for a RolloverTask.
   */
  public static class Factory implements TaskFactory {
    @Override
    public TaskIFace createTask(TaskContext taskContext) {
      return new RolloverTask(taskContext);
    }

    @Override
    public DataPublisher createDataPublisher(JobState.DatasetState datasetState) {
      return new NoopPublisher(datasetState);
    }
  }
}

