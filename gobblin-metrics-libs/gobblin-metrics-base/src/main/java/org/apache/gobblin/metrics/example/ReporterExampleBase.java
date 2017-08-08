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

package org.apache.gobblin.metrics.example;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import org.apache.gobblin.metrics.event.JobEvent;
import org.apache.gobblin.metrics.reporter.ContextAwareScheduledReporter;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;


/**
 * A base class for those that exemplifies the usage of {@link MetricContext} and a given
 * {@link ContextAwareScheduledReporter}.
 *
 * <p>
 *   This class simulating a record processing job that spawns a configurable number of tasks
 *   running in a thread pool for processing incoming data records in parallel. Each task will
 *   simulate processing the same configurable number of records. The job creates a job-level
 *   {@link MetricContext}, from which a child {@link MetricContext} for each task is created.
 *   Each task uses four metrics in its {@link MetricContext} to keep track of the total number
 *   of records processed, record processing rate, record size distribution, and record processing
 *   times. Since the job-level {@link MetricContext} is the parent of the {@link MetricContext}s
 *   of the tasks, updates to a metric of a task will be automatically applied to the metric with
 *   the same name in the job-level {@link MetricContext}. The job and task {@link MetricContext}s
 *   use the same given {@link ContextAwareScheduledReporter} to report metrics.
 * </p>
 *
 * @author Yinan Li
 */
public class ReporterExampleBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReporterExampleBase.class);

  private static final String JOB_NAME = "ExampleJob";
  private static final String TASK_ID_KEY = "task.id";
  private static final String TASK_ID_PREFIX = "ExampleTask_";

  private static final String TOTAL_RECORDS = "totalRecords";
  private static final String RECORD_PROCESS_RATE = "recordProcessRate";
  private static final String RECORD_PROCESS_TIME = "recordProcessTime";
  private static final String RECORD_SIZES = "recordSizes";

  private final ExecutorService executor;
  private final MetricContext context;
  private final ContextAwareScheduledReporter.Builder reporterBuilder;

  private final int tasks;
  private final long totalRecords;

  public ReporterExampleBase(ContextAwareScheduledReporter.Builder reporterBuilder, int tasks, long totalRecords) {
    this.executor = Executors.newFixedThreadPool(10);

    this.context = MetricContext.builder("Job")
        .addTag(new Tag<String>(JobEvent.METADATA_JOB_NAME, "ExampleJob"))
        .addTag(new Tag<String>(JobEvent.METADATA_JOB_ID, JOB_NAME + "_" + System.currentTimeMillis()))
        .build();

    this.reporterBuilder = reporterBuilder;

    this.tasks = tasks;
    this.totalRecords = totalRecords;
  }

  /**
   * Run the example.
   */
  public void run() throws Exception {
    try {

      CountDownLatch countDownLatch = new CountDownLatch(this.tasks);
      for (int i = 0; i < this.tasks; i++) {
        addTask(i, countDownLatch);
      }
      // Wait for the tasks to finish
      countDownLatch.await();
    } finally {
      try {
        // Calling close() will stop metric reporting
        this.context.close();
      } finally {
        this.executor.shutdownNow();
      }
    }
  }

  private void addTask(int taskIndex, CountDownLatch countDownLatch) {
    // Build the context of this task, which is a child of the job's context.
    // Tags of the job (parent) context will be inherited automatically.
    MetricContext taskContext = this.context.childBuilder("Task" + taskIndex)
        .addTag(new Tag<String>(TASK_ID_KEY, TASK_ID_PREFIX + taskIndex))
        .build();
    Task task = new Task(taskContext, taskIndex, this.totalRecords, countDownLatch);
    this.executor.execute(task);
  }

  private static class Task implements Runnable {

    private final MetricContext context;
    private final int taskIndex;
    private final long totalRecords;
    private final CountDownLatch countDownLatch;
    private final Random rand = new Random();

    public Task(MetricContext context, int taskIndex, long totalRecords, CountDownLatch countDownLatch) {
      this.context = context;
      this.taskIndex = taskIndex;
      this.totalRecords = totalRecords;
      this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
      Counter totalRecordsCounter = this.context.contextAwareCounter(TOTAL_RECORDS);
      Meter recordProcessRateMeter = this.context.contextAwareMeter(RECORD_PROCESS_RATE);
      Timer recordProcessTimeTimer = this.context.contextAwareTimer(RECORD_PROCESS_TIME);
      Histogram recordSizesHistogram = this.context.contextAwareHistogram(RECORD_SIZES);

      try {

        for (int i = 0; i < this.totalRecords; i++) {
          totalRecordsCounter.inc();
          recordProcessRateMeter.mark();
          recordSizesHistogram.update((this.rand.nextLong() & Long.MAX_VALUE) % 5000l);

          if (i % 100 == 0) {
            LOGGER.info(String.format("Task %d has processed %d records so far", this.taskIndex, i));
          }

          long processTime = (this.rand.nextLong() & Long.MAX_VALUE) % 10;
          // Simulate record processing by sleeping for a random amount of time
          try {
            Thread.sleep(processTime);
          } catch (InterruptedException ie) {
            LOGGER.warn(String.format("Task %d has been interrupted", this.taskIndex));
            Thread.currentThread().interrupt();
            return;
          }

          recordProcessTimeTimer.update(processTime, TimeUnit.MILLISECONDS);
        }

        LOGGER.info(String.format("Task %d has processed all %d records", this.taskIndex, this.totalRecords));
      } finally{
        try {
          this.context.close();
        } catch (IOException ioe) {
          LOGGER.error("Failed to close context: " + this.context.getName(), ioe);
        } finally {
          this.countDownLatch.countDown();
        }
      }
    }
  }
}
