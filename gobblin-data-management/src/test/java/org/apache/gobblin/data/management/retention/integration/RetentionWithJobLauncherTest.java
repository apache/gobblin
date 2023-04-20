
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

package org.apache.gobblin.data.management.retention.integration;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.io.Closer;
import com.google.common.io.Files;
import com.google.gson.reflect.TypeToken;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.data.management.policy.NewestKSelectionPolicy;
import org.apache.gobblin.data.management.retention.dataset.ConfigurableCleanableDataset;
import org.apache.gobblin.data.management.retention.profile.GlobCleanableDatasetFinder;
import org.apache.gobblin.data.management.retention.profile.MultiCleanableDatasetFinder;
import org.apache.gobblin.data.management.retention.source.DatasetCleanerSource;
import org.apache.gobblin.data.management.retention.version.DatasetRetentionSummary;
import org.apache.gobblin.data.management.version.finder.GlobModTimeDatasetVersionFinder;
import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.test.MetricsAssert;
import org.apache.gobblin.runtime.AbstractJobLauncher;
import org.apache.gobblin.runtime.JobContext;
import org.apache.gobblin.runtime.JobLauncher;
import org.apache.gobblin.runtime.JobLauncherFactory;
import org.apache.gobblin.runtime.util.GsonUtils;
import org.apache.gobblin.util.JobLauncherUtils;


public class RetentionWithJobLauncherTest {

  private Properties launcherProps;
  private Path testDataPath;
  private FileSystem fs;

  @BeforeClass
  public void startup() throws Exception {
    this.fs = FileSystem.get(new Configuration());
    this.launcherProps = new Properties();
    this.launcherProps.load(new FileReader("gobblin-test/resource/gobblin.test.properties"));
    this.launcherProps.setProperty(ConfigurationKeys.JOB_HISTORY_STORE_ENABLED_KEY, "false");
    this.launcherProps.setProperty(ConfigurationKeys.METRICS_ENABLED_KEY, "true");
    this.launcherProps.setProperty(ConfigurationKeys.METRICS_REPORTING_FILE_ENABLED_KEY, "false");
    // Create mock test data
    this.testDataPath = new Path(Files.createTempDir().getAbsolutePath(), "testRetentionData");
    fs.mkdirs(this.testDataPath);
    for (int i = 0; i < 3; i++) {
      File testFile = new File(testDataPath.toString(), "file" + i + ".txt");
      testFile.createNewFile();
    }
  }

  @AfterClass
  public void cleanup() throws IOException {
    if (this.fs.exists(this.testDataPath)){
      this.fs.delete(this.testDataPath, true);
    }
  }

  @Test
  void runRetentionJobNewestK() {
    Properties jobProps = new Properties();
    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY, "testRetentionJob");
    jobProps.setProperty(ConfigurationKeys.JOB_GROUP_KEY, "testGroup");
    jobProps.setProperty(ConfigurationKeys.SOURCE_CLASS_KEY, DatasetCleanerSource.class.getCanonicalName());
    jobProps.setProperty(MultiCleanableDatasetFinder.DATASET_FINDER_CLASS_KEY, GlobCleanableDatasetFinder.class.getCanonicalName());
    jobProps.setProperty(ConfigurableCleanableDataset.VERSION_FINDER_CLASS_KEY, GlobModTimeDatasetVersionFinder.class.getCanonicalName());
    jobProps.setProperty(ConfigurableCleanableDataset.SELECTION_POLICY_CLASS_KEY, NewestKSelectionPolicy.class.getCanonicalName());
    jobProps.setProperty(GlobCleanableDatasetFinder.DATASET_FINDER_PATTERN_KEY, this.testDataPath.toString());
    jobProps.setProperty(ConfigurableCleanableDataset.RETENTION_CONFIGURATION_KEY + NewestKSelectionPolicy.NEWEST_K_VERSIONS_SELECTED_KEY, "2");

    try {
      List<GobblinTrackingEvent> jobTrackingEvents = runTest(jobProps);
      // Validate that 2 files are deleted
      Assert.assertEquals(this.fs.listStatus(this.testDataPath).length, 1);

      Optional<GobblinTrackingEvent>
          summaryEvent = jobTrackingEvents.stream().filter(e -> e.getName().equals("RetentionJobSummary")).findFirst();
      Assert.assertNotNull(summaryEvent);
      Type retentionSummaryType = new TypeToken<ArrayList<DatasetRetentionSummary>>(){}.getType();
      Assert.assertTrue(summaryEvent.get().getMetadata().containsKey("datasetsMarkedForCleaning"));
      List<DatasetRetentionSummary> retentionSummaries =
          GsonUtils.GSON_WITH_DATE_HANDLING.fromJson(summaryEvent.get().getMetadata().get("datasetsMarkedForCleaning"), retentionSummaryType);
      Assert.assertEquals(retentionSummaries.size(), 1);
      Assert.assertEquals(retentionSummaries.get(0).getVersionsSelectedForDeletion(), 2);
      Assert.assertEquals(retentionSummaries.get(0).isSuccessfullyDeleted(), true);
    } catch (Exception e) {
      Assert.fail("Retention job failed with exception: " + e.getMessage());
    }
  }


  public List<GobblinTrackingEvent> runTest(Properties jobProps) throws Exception {
    String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
    String jobId = JobLauncherUtils.newJobId(jobName);
    jobProps.setProperty(ConfigurationKeys.JOB_ID_KEY, jobId);

    JobContext jobContext;
    MetricsAssert metricsAssert;
    Closer closer = Closer.create();
    JobLauncher jobLauncher = closer.register(JobLauncherFactory.newJobLauncher(this.launcherProps, jobProps));
    jobContext = ((AbstractJobLauncher) jobLauncher).getJobContext();
    metricsAssert = new MetricsAssert(jobContext.getJobMetricsOptional().get().getMetricContext());
    try {
      jobLauncher.launchJob(null);
      List<GobblinTrackingEvent> events = metricsAssert.getEvents();
      Assert.assertTrue(events.stream().anyMatch(e -> e.getName().equals("JobStartTimer")));
      Assert.assertTrue(jobContext.getJobMetricsOptional().isPresent());
      // Should ignore the summary event since Retention jobs do not follow task-writer semantics
      Optional<GobblinTrackingEvent>
          summaryEvent = events.stream().filter(e -> e.getName().equals("JobSummaryTimer")).findFirst();
      Assert.assertFalse(summaryEvent.isPresent());
    } finally {
      closer.close();
    }
    return metricsAssert.getEvents();
  }
}
