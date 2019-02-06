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

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.util.ReflectivePredicateEvaluator;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractScheduledService;

import lombok.extern.slf4j.Slf4j;


/**
 * This class evaluates a predicate on the {@link JobProgress} of a job and calls a job interruption hook when the
 * predicate is satisfied.
 *
 * It is used to preemptively stop jobs after they satisfy some completion predicate (e.g. more than 15 minutes have
 * elapsed and at least 75% of tasks have finished).
 */
@Slf4j
public class JobInterruptionPredicate extends AbstractScheduledService {

	public static final String INTERRUPTION_SQL = "org.apache.gobblin.jobInterruptionPredicate.sql";

	private final String sql;
	private final ReflectivePredicateEvaluator evaluator;
	private final JobProgress jobProgress;
	private final Runnable jobInterruptionHook;

	public JobInterruptionPredicate(JobState jobState, Runnable jobInterruptionHook, boolean autoStart) {
		this(jobState, jobState.getProp(INTERRUPTION_SQL), jobInterruptionHook, autoStart);
	}

	protected JobInterruptionPredicate(JobProgress jobProgress, String predicate,
			Runnable jobInterruptionHook, boolean autoStart) {
		this.sql = predicate;

		ReflectivePredicateEvaluator tmpEval = null;
		if (this.sql != null) {
			try {
				tmpEval = new ReflectivePredicateEvaluator(this.sql, JobProgress.class, TaskProgress.class);
			} catch (SQLException exc) {
				log.warn("Job interruption predicate is invalid, will not preemptively interrupt job.", exc);
			}
		}
		this.evaluator = tmpEval;
		this.jobProgress = jobProgress;
		this.jobInterruptionHook = jobInterruptionHook;

		if (autoStart && this.sql != null) {
			startAsync();
		}
	}

	@Override
	protected void runOneIteration() {
		if (this.evaluator == null) {
			stopAsync();
			return;
		}
		switch (this.jobProgress.getState()) {
			case PENDING:
				return;
			case RUNNING:
				try {
					List<Object> objects = Stream.concat(Stream.<Object>of(this.jobProgress), this.jobProgress.getTaskProgress().stream()).collect(
							Collectors.toList());
					if (this.evaluator.evaluate(objects)) {
						log.info("Interrupting job due to satisfied job interruption predicate. Predicate: " + this.sql);
						this.jobInterruptionHook.run();
						stopAsync();
					}
				} catch (Throwable exc) {
					log.warn("Failed to evaluate job interruption predicate. Will not preemptively interrupt job.", exc);
					throw Throwables.propagate(exc);
				}
				break;
			default:
				log.info(String.format("Detected job finished with state %s. Stopping job interruption predicate.",
						this.jobProgress.getState()));
				stopAsync();
		}
	}

	@Override
	protected Scheduler scheduler() {
		return Scheduler.newFixedDelaySchedule(30, 30, TimeUnit.SECONDS);
	}
}
