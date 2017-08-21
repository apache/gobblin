package org.apache.gobblin;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.JobException;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


@Test
public class TaskSkipErrRecordsIntegrationTest {
  private static final String SAMPLE_FILE = "test.avro";
  public static final String TASK_SKIP_ERROR_RECORDS = "task.skip.error.records";
  public static final String ONE = "1";
  public static final String ZERO = "0";

  @BeforeTest
  @AfterTest
  public void cleanDir()
      throws IOException {
    GobblinLocalJobLauncherUtils.cleanDir();
  }

  /**
   * Converter will throw DataConversionException while trying to convert
   * first record. Since task.skip.error.records is set to 0, this job should fail.
   */
  @Test(expectedExceptions = JobException.class)
  public void skipZeroErrorRecordTest()
      throws Exception {
    Properties jobProperties = getProperties();
    jobProperties.setProperty(TASK_SKIP_ERROR_RECORDS, ZERO);
    GobblinLocalJobLauncherUtils.invokeLocalJobLauncher(jobProperties);
  }

  /**
   * Converter will throw DataConversionException while trying to convert
   * first record. Since task.skip.error.records is set to 1, this job should succeed
   */
  @Test
  public void skipOneErrorRecordTest()
      throws Exception {
    Properties jobProperties = getProperties();
    jobProperties.setProperty(TASK_SKIP_ERROR_RECORDS, ONE);
    GobblinLocalJobLauncherUtils.invokeLocalJobLauncher(jobProperties);
  }

  private Properties getProperties()
      throws IOException {
    Properties jobProperties =
        GobblinLocalJobLauncherUtils.getJobProperties("runtime_test/task_skip_err_records.properties");
    FileUtils.copyFile(new File(GobblinLocalJobLauncherUtils.RESOURCE_DIR + SAMPLE_FILE),
        new File(GobblinLocalJobLauncherUtils.RESOURCE_DIR + GobblinLocalJobLauncherUtils.SAMPLE_DIR + SAMPLE_FILE));
    jobProperties.setProperty(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL,
        GobblinLocalJobLauncherUtils.RESOURCE_DIR + GobblinLocalJobLauncherUtils.SAMPLE_DIR + SAMPLE_FILE);
    return jobProperties;
  }
}
