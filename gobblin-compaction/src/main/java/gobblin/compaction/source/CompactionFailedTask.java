package gobblin.compaction.source;

import gobblin.compaction.suite.CompactionSuite;
import gobblin.compaction.suite.CompactionSuiteUtils;
import gobblin.dataset.Dataset;
import gobblin.runtime.TaskContext;
import gobblin.runtime.task.FailedTask;
import gobblin.runtime.task.TaskIFace;

/**
 * A task which throws an exception when executed
 * The exception contains dataset information
 */
public class CompactionFailedTask extends FailedTask {
  protected final CompactionSuite suite;
  protected final Dataset dataset;

  public CompactionFailedTask (TaskContext taskContext) {
    super(taskContext);
    this.suite = CompactionSuiteUtils.getCompactionSuiteFactory (taskContext.getTaskState()).
        createSuite(taskContext.getTaskState());
    this.dataset = this.suite.load(taskContext.getTaskState());
  }

  @Override
  public void run() {
    throw new RuntimeException("Dataset " + dataset.datasetURN() + " failed");
  }

  public static class CompactionFailedTaskFactory extends FailedTaskFactory {

    @Override
    public TaskIFace createTask(TaskContext taskContext) {
      return new CompactionFailedTask (taskContext);
    }
  }
}
