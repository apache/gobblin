/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.runtime.job;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.runtime.JobState;
import org.junit.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

import lombok.AllArgsConstructor;
import lombok.Getter;

public class JobInterruptionPredicateTest {

	@Test
	public void testJobPredicate() {
		SettableJobProgress jobProgress = new SettableJobProgress("job123", 10, 0, 0, JobState.RunningState.RUNNING, new ArrayList<>());
		AtomicBoolean atomicBoolean = new AtomicBoolean(false);

		JobInterruptionPredicate predicate =
				new JobInterruptionPredicate(jobProgress, "SELECT completedTasks > 5 FROM jobProgress", () -> atomicBoolean.set(true), false);

		predicate.runOneIteration();
		Assert.assertFalse(atomicBoolean.get());

		jobProgress.completedTasks = 6;

		predicate.runOneIteration();
		Assert.assertTrue(atomicBoolean.get());
	}

	@Test
	public void testTaskPredicate() {
		SettableTaskProgress t1 = new SettableTaskProgress("j1", "t1", WorkUnitState.WorkingState.RUNNING, false);
		SettableTaskProgress t2 = new SettableTaskProgress("j1", "t1", WorkUnitState.WorkingState.RUNNING, false);

		SettableJobProgress jobProgress = new SettableJobProgress("job123", 10, 0, 0, JobState.RunningState.RUNNING,
				Lists.newArrayList(t1, t2));
		AtomicBoolean atomicBoolean = new AtomicBoolean(false);

		JobInterruptionPredicate predicate =
				new JobInterruptionPredicate(jobProgress, "SELECT count(*) > 0 FROM taskProgress WHERE workingState = 'FAILED'", () -> atomicBoolean.set(true), false);

		predicate.runOneIteration();
		Assert.assertFalse(atomicBoolean.get());

		t2.workingState = WorkUnitState.WorkingState.FAILED;

		predicate.runOneIteration();
		Assert.assertTrue(atomicBoolean.get());
	}

	@Test
	public void testTaskAndJobPredicate() {
		SettableTaskProgress t1 = new SettableTaskProgress("j1", "t1", WorkUnitState.WorkingState.RUNNING, false);
		SettableTaskProgress t2 = new SettableTaskProgress("j1", "t1", WorkUnitState.WorkingState.RUNNING, false);

		SettableJobProgress jobProgress = new SettableJobProgress("job123", 10, 0, 0, JobState.RunningState.RUNNING,
				Lists.newArrayList(t1, t2));
		AtomicBoolean atomicBoolean = new AtomicBoolean(false);

		JobInterruptionPredicate predicate =
				new JobInterruptionPredicate(jobProgress,
						"SELECT EXISTS(SELECT * FROM (SELECT completedTasks > 5 AS pred FROM jobProgress UNION SELECT count(*) > 0 AS pred FROM taskProgress WHERE workingState = 'FAILED') WHERE pred)",
						() -> atomicBoolean.set(true), false);

		predicate.runOneIteration();
		Assert.assertFalse(atomicBoolean.get());

		t2.workingState = WorkUnitState.WorkingState.FAILED;

		predicate.runOneIteration();
		Assert.assertTrue(atomicBoolean.get());
		atomicBoolean.set(false);

		t2.workingState = WorkUnitState.WorkingState.RUNNING;

		predicate.runOneIteration();
		Assert.assertFalse(atomicBoolean.get());

		jobProgress.completedTasks = 6;

		predicate.runOneIteration();
		Assert.assertTrue(atomicBoolean.get());
	}

	@Getter
	@AllArgsConstructor
	public static class SettableJobProgress implements JobProgress {
		private final String jobId;
		private int taskCount;
		private int completedTasks;
		private long elapsedTime;
		private JobState.RunningState runningState;
		private List<TaskProgress> taskProgress;

		@Override
		public JobState.RunningState getState() {
			return this.runningState;
		}
	}

	@Getter
	@AllArgsConstructor
	public static class SettableTaskProgress implements TaskProgress {
		private final String jobId;
		private final String taskId;
		private WorkUnitState.WorkingState workingState;
		private boolean isCompleted;
	}

}
