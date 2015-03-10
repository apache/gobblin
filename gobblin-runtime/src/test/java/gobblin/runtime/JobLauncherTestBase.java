/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime;

import java.io.File;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.inject.Guice;
import com.google.inject.Injector;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.metastore.MetaStoreModule;
import gobblin.metastore.StateStore;


/**
 * Base class for {@link JobLauncher} unit tests.
 */
public abstract class JobLauncherTestBase {

  protected static final String SOURCE_FILE_LIST_KEY = "source.files";

  protected Properties properties;
  protected StateStore jobStateStore;

  @SuppressWarnings("unchecked")
  protected void runTest(Properties jobProps)
      throws Exception {
    JobLauncher jobLauncher = JobLauncherFactory.newJobLauncher(this.properties);
    jobLauncher.launchJob(jobProps, null);
    String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
    String jobId = jobProps.getProperty(ConfigurationKeys.JOB_ID_KEY);
    List<JobState> jobStateList = (List<JobState>) this.jobStateStore.getAll(jobName, jobId + ".jst");
    JobState jobState = jobStateList.get(0);

    Assert.assertEquals(jobState.getState(), JobState.RunningState.COMMITTED);
    Assert.assertEquals(jobState.getCompletedTasks(), 4);
    Assert.assertEquals(jobState.getPropAsInt(ConfigurationKeys.JOB_FAILURES_KEY), 0);
    for (TaskState taskState : jobState.getTaskStates()) {
      Assert.assertEquals(taskState.getWorkingState(), WorkUnitState.WorkingState.COMMITTED);
    }
  }

  @SuppressWarnings("unchecked")
  protected void runTestWithPullLimit(Properties jobProps)
      throws Exception {
    JobLauncher jobLauncher = JobLauncherFactory.newJobLauncher(this.properties);
    jobLauncher.launchJob(jobProps, null);
    String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
    String jobId = jobProps.getProperty(ConfigurationKeys.JOB_ID_KEY);
    List<JobState> jobStateList = (List<JobState>) this.jobStateStore.getAll(jobName, jobId + ".jst");
    JobState jobState = jobStateList.get(0);

    Assert.assertEquals(jobState.getState(), JobState.RunningState.COMMITTED);
    Assert.assertEquals(jobState.getCompletedTasks(), 4);
    Assert.assertEquals(jobState.getPropAsInt(ConfigurationKeys.JOB_FAILURES_KEY), 0);

    for (TaskState taskState : jobState.getTaskStates()) {
      Assert.assertEquals(taskState.getWorkingState(), WorkUnitState.WorkingState.COMMITTED);
      Assert.assertEquals(taskState.getPropAsLong(ConfigurationKeys.EXTRACTOR_ROWS_EXPECTED),
          taskState.getPropAsLong(ConfigurationKeys.EXTRACT_PULL_LIMIT));
      Assert.assertEquals(taskState.getPropAsLong(ConfigurationKeys.WRITER_ROWS_WRITTEN),
          taskState.getPropAsLong(ConfigurationKeys.EXTRACT_PULL_LIMIT));
    }
  }

  @SuppressWarnings("unchecked")
  protected void runTestWithCancellation(final Properties jobProps)
      throws Exception {
    final JobLauncher jobLauncher = JobLauncherFactory.newJobLauncher(this.properties);

    final AtomicBoolean isCancelled = new AtomicBoolean(false);
    // This thread will cancel the job after some time
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(500);
          jobLauncher.cancelJob(jobProps);
          isCancelled.set(true);
        } catch (Exception je) {
          // Ignored
        }
      }
    });
    thread.start();

    jobLauncher.launchJob(jobProps, null);

    Assert.assertTrue(isCancelled.get());

    String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
    String jobId = jobProps.getProperty(ConfigurationKeys.JOB_ID_KEY);
    List<JobState> jobStateList = (List<JobState>) this.jobStateStore.getAll(jobName, jobId + ".jst");
    Assert.assertTrue(jobStateList.isEmpty());

    FileSystem lfs = FileSystem.getLocal(new Configuration());
    Path jobLockFile = new Path(jobProps.getProperty(ConfigurationKeys.JOB_LOCK_DIR_KEY),
        jobName + FileBasedJobLock.LOCK_FILE_EXTENSION);
    Assert.assertFalse(lfs.exists(jobLockFile));
  }

  @SuppressWarnings("unchecked")
  protected void runTestWithFork(Properties jobProps)
      throws Exception {
    JobLauncher jobLauncher = JobLauncherFactory.newJobLauncher(this.properties);
    jobLauncher.launchJob(jobProps, null);
    String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
    String jobId = jobProps.getProperty(ConfigurationKeys.JOB_ID_KEY);
    List<JobState> jobStateList = (List<JobState>) this.jobStateStore.getAll(jobName, jobId + ".jst");
    JobState jobState = jobStateList.get(0);

    Assert.assertEquals(jobState.getState(), JobState.RunningState.COMMITTED);
    Assert.assertEquals(jobState.getCompletedTasks(), 4);
    Assert.assertEquals(jobState.getPropAsInt(ConfigurationKeys.JOB_FAILURES_KEY), 0);

    FileSystem lfs = FileSystem.getLocal(new Configuration());
    for (TaskState taskState : jobState.getTaskStates()) {
      Assert.assertEquals(taskState.getWorkingState(), WorkUnitState.WorkingState.COMMITTED);
      Path path = new Path(taskState.getProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR),
          new Path(taskState.getExtract().getOutputFilePath(), "fork_0"));
      Assert.assertTrue(lfs.exists(path));
      Assert.assertEquals(lfs.listStatus(path).length, 2);
      path = new Path(taskState.getProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR),
          new Path(taskState.getExtract().getOutputFilePath(), "fork_1"));
      Assert.assertTrue(lfs.exists(path));
      Assert.assertEquals(lfs.listStatus(path).length, 2);
    }
  }

  protected void prepareJobHistoryStoreDatabase(Properties properties)
      throws Exception {
    // Read the DDL statements
    List<String> statementLines = Lists.newArrayList();
    List<String> lines = Files.readLines(new File("gobblin-metastore/src/test/resources/gobblin_job_history_store.sql"),
        Charset.forName(ConfigurationKeys.DEFAULT_CHARSET_ENCODING));
    for (String line : lines) {
      // Skip a comment line
      if (line.startsWith("--")) {
        continue;
      }
      statementLines.add(line);
    }
    String statements = Joiner.on("\n").skipNulls().join(statementLines);

    Optional<Connection> connectionOptional = Optional.absent();
    try {
      Injector injector = Guice.createInjector(new MetaStoreModule(properties));
      DataSource dataSource = injector.getInstance(DataSource.class);
      connectionOptional = Optional.of(dataSource.getConnection());
      Connection connection = connectionOptional.get();
      for (String statement : Splitter.on(";").omitEmptyStrings().trimResults().split(statements)) {
        PreparedStatement preparedStatement = connection.prepareStatement(statement);
        preparedStatement.execute();
      }
    } finally {
      if (connectionOptional.isPresent()) {
        connectionOptional.get().close();
      }
    }
  }
}
