package com.linkedin.uif.publisher;

import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.qualitychecker.task.TaskLevelPolicyCheckResults;

public class TaskPublisherBuilder
{
    private final TaskLevelPolicyCheckResults results;
    private final WorkUnitState workUnitState;
        
    public TaskPublisherBuilder(WorkUnitState workUnitState, TaskLevelPolicyCheckResults results) {
        this.results = results;
        this.workUnitState = workUnitState;
    }
    
    public static TaskPublisherBuilder newBuilder(WorkUnitState taskState, TaskLevelPolicyCheckResults results) {
        return new TaskPublisherBuilder(taskState, results);
    }

    public TaskPublisher build() throws Exception {
        return new TaskPublisher(this.workUnitState, this.results);
    }
}
