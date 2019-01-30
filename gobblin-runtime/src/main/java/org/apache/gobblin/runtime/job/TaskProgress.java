package org.apache.gobblin.runtime.job;

import org.apache.gobblin.configuration.WorkUnitState;


public interface TaskProgress {

	String getJobId();
	String getTaskId();
	WorkUnitState.WorkingState getWorkingState();
	boolean isCompleted();

}
