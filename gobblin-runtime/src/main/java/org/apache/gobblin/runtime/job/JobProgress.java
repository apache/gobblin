package org.apache.gobblin.runtime.job;

import java.util.List;

import org.apache.gobblin.runtime.JobState;


public interface JobProgress {

	String getJobId();
	int getTaskCount();
	int getCompletedTasks();
	long getElapsedTime();
	JobState.RunningState getState();

	List<? extends TaskProgress> getTaskProgress();

}
