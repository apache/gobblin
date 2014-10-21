package com.linkedin.uif.publisher;

import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.qualitychecker.task.TaskLevelPolicyCheckResults;

public class TaskPublisherBuilderFactory
{
    public TaskPublisherBuilder newTaskPublisherBuilder(WorkUnitState workUnitState,
                                                        TaskLevelPolicyCheckResults results,
                                                        int index) {

        return TaskPublisherBuilder.newBuilder(workUnitState, results, index);
    }
}
