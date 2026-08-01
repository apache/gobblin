package org.apache.gobblin.runtime.mapreduce.kubernetes;

import org.apache.gobblin.runtime.AbstractTaskStateTracker;
import org.apache.gobblin.runtime.Task;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KuberTaskStateTracker extends AbstractTaskStateTracker {

    private static final Logger LOG = LoggerFactory.getLogger(KuberTaskStateTracker.class);


    public KuberTaskStateTracker(final Configuration configuration,
                                 final Logger logger) {
        super(configuration, logger);
    }


    @Override
    public void registerNewTask(final Task task) {
    }

    @Override
    public void onTaskRunCompletion(final Task task) {
        task.markTaskCompletion();
    }

    @Override
    public void onTaskCommitCompletion(final Task task) {
        LOG.info("Task {} completed running in {}ms with state {}",
                task.getTaskId(),
                task.getTaskState().getTaskDuration(),
                task.getTaskState().getWorkingState()
        );
    }

}
