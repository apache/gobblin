package org.apache.gobblin.compaction.mapreduce;

import java.io.IOException;


import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.runtime.mapreduce.MRTaskFactory;
import org.apache.gobblin.runtime.task.TaskIFace;

/**
 * A subclass of {@link MRTaskFactory} which provides a customized {@link MRCompactionTask} instance
 */
public class MRCompactionTaskFactory extends MRTaskFactory {
  @Override
  public TaskIFace createTask(TaskContext taskContext) {
    try {
      return new MRCompactionTask(taskContext);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
