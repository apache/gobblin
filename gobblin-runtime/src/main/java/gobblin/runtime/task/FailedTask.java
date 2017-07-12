package gobblin.runtime.task;

import gobblin.configuration.WorkUnitState;
import gobblin.publisher.DataPublisher;
import gobblin.publisher.NoopPublisher;
import gobblin.runtime.JobState;
import gobblin.runtime.TaskContext;
import gobblin.source.workunit.WorkUnit;

/**
 * A task which raise an exception when run
 */
public class FailedTask extends BaseAbstractTask {
  public FailedTask (TaskContext taskContext) {
    super(taskContext);
  }

  public void commit() {
    this.workingState = WorkUnitState.WorkingState.FAILED;
  }

  @Override
  public void run() {
    this.workingState = WorkUnitState.WorkingState.FAILED;
  }

  public static class FailedWorkUnit extends WorkUnit {

    public FailedWorkUnit () {
      super ();
      TaskUtils.setTaskFactoryClass(this, FailedTaskFactory.class);
    }
  }

  public static class FailedTaskFactory implements TaskFactory {

    @Override
    public TaskIFace createTask(TaskContext taskContext) {
      return new FailedTask(taskContext);
    }

    @Override
    public DataPublisher createDataPublisher(JobState.DatasetState datasetState) {
      return new NoopPublisher(datasetState);
    }
  }
}
