package com.linkedin.uif.publisher;

import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.qualitychecker.task.TaskLevelPolicyCheckResults;

public class TaskPublisherBuilder
{
    private final TaskLevelPolicyCheckResults results;
    private final WorkUnitState workUnitState;
    private final int index;

    public TaskPublisherBuilder(WorkUnitState workUnitState, TaskLevelPolicyCheckResults results, int index) {
        this.results = results;
        this.workUnitState = workUnitState;
        this.index = index;
    }
    
    public static TaskPublisherBuilder newBuilder(WorkUnitState taskState,
                                                  TaskLevelPolicyCheckResults results,
                                                  int index) {

        return new TaskPublisherBuilder(taskState, results, index);
    }

    public TaskPublisher build() throws Exception {
        return new TaskPublisher(this.workUnitState, this.results);
    }
}
