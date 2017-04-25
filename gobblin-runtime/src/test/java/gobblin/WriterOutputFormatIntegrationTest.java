package gobblin;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import gobblin.configuration.ConfigurationKeys;
import gobblin.hive.HiveSerDeWrapper;


public class WriterOutputFormatIntegrationTest {
  private static final String SAMPLE_FILE = "test.avro";

  @BeforeTest
  @AfterTest
  public void cleanDir()
      throws IOException {
    GobblinLocalJobLauncherUtils.cleanDir();
  }

  @Test
  public void parquetOutputFormatTest()
      throws Exception {
    Properties jobProperties = getProperties();
    jobProperties.setProperty(HiveSerDeWrapper.SERDE_SERIALIZER_TYPE, "PARQUET");
    jobProperties.setProperty(ConfigurationKeys.WRITER_OUTPUT_FORMAT_KEY, "PARQUET");
    GobblinLocalJobLauncherUtils.invokeLocalJobLauncher(jobProperties);
  }

  @Test
  public void orcOutputFormatTest()
      throws Exception {
    Properties jobProperties = getProperties();
    jobProperties.setProperty(HiveSerDeWrapper.SERDE_SERIALIZER_TYPE, "ORC");
    jobProperties.setProperty(ConfigurationKeys.WRITER_OUTPUT_FORMAT_KEY, "ORC");
    GobblinLocalJobLauncherUtils.invokeLocalJobLauncher(jobProperties);
  }

  @Test
  public void textfileOutputFormatTest()
      throws Exception {
    Properties jobProperties = getProperties();
    jobProperties.setProperty(HiveSerDeWrapper.SERDE_SERIALIZER_TYPE, "TEXTFILE");
    jobProperties.setProperty(ConfigurationKeys.WRITER_OUTPUT_FORMAT_KEY, "TEXTFILE");
    GobblinLocalJobLauncherUtils.invokeLocalJobLauncher(jobProperties);
  }

  private Properties getProperties()
      throws IOException {
    Properties jobProperties =
        GobblinLocalJobLauncherUtils.getJobProperties("runtime_test/writer_output_format_test.properties");
    FileUtils.copyFile(new File(GobblinLocalJobLauncherUtils.RESOURCE_DIR + SAMPLE_FILE),
        new File(GobblinLocalJobLauncherUtils.RESOURCE_DIR + GobblinLocalJobLauncherUtils.SAMPLE_DIR + SAMPLE_FILE));
    jobProperties.setProperty(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL,
        GobblinLocalJobLauncherUtils.RESOURCE_DIR + GobblinLocalJobLauncherUtils.SAMPLE_DIR + SAMPLE_FILE);
    return jobProperties;
  }
}
