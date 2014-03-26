package com.linkedin.uif.publisher;

import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.qualitychecker.PolicyCheckResults;

public class TaskPublisherBuilder
{
    private final PolicyCheckResults results;
    private final WorkUnitState workUnitState;
        
    public TaskPublisherBuilder(WorkUnitState workUnitState, PolicyCheckResults results) {
        this.results = results;
        this.workUnitState = workUnitState;
    }
    
    public static TaskPublisherBuilder newBuilder(WorkUnitState taskState, PolicyCheckResults results) {
        return new TaskPublisherBuilder(taskState, results);
    }

    public TaskPublisher build() throws Exception {
        return new TaskPublisher(this.workUnitState, this.results);
    }
}
