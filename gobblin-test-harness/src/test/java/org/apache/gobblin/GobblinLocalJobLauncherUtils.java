package gobblin;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.io.FileUtils;

import gobblin.runtime.app.ApplicationLauncher;
import gobblin.runtime.app.ServiceBasedAppLauncher;
import gobblin.runtime.local.LocalJobLauncher;


public class GobblinLocalJobLauncherUtils {
  public static final String RESOURCE_DIR = "./gobblin-test-harness/src/test/resources/runtime_test/";
  public static final String SAMPLE_DIR = "test_data/daily/2016/10/01/";
  public static final String DATA_PURGER_COMMIT_DATA = "data.purger.commit.data";
  public static final String STATE_STORE = "state_store";
  public static final String WRITER_STAGING = "writer_staging";
  public static final String WRITER_OUTPUT = "writer_output";
  public static final String TMP = "tmp";
  public static final String METRICS = "metrics";
  public static final String FINAL_DIR = "final_dir";

  public static void invokeLocalJobLauncher(Properties properties)
      throws Exception {
    try (ApplicationLauncher applicationLauncher = new ServiceBasedAppLauncher(properties,
        properties.getProperty(ServiceBasedAppLauncher.APP_NAME, "CliLocalJob-" + UUID.randomUUID()))) {
      applicationLauncher.start();
      try (LocalJobLauncher localJobLauncher = new LocalJobLauncher(properties)) {
        localJobLauncher.launchJob(null);
      }
      applicationLauncher.stop();
    }
  }

  public static void cleanDir()
      throws IOException {
    FileUtils.forceMkdir(new File(RESOURCE_DIR + STATE_STORE));
    FileUtils.forceMkdir(new File(RESOURCE_DIR + WRITER_STAGING));
    FileUtils.forceMkdir(new File(RESOURCE_DIR + WRITER_OUTPUT));
    FileUtils.forceMkdir(new File(RESOURCE_DIR + TMP));
    FileUtils.forceMkdir(new File(RESOURCE_DIR + METRICS));
    FileUtils.forceMkdir(new File(RESOURCE_DIR + SAMPLE_DIR));
    FileUtils.forceMkdir(new File(RESOURCE_DIR + FINAL_DIR));

    FileUtils.cleanDirectory(new File(RESOURCE_DIR + STATE_STORE));
    FileUtils.cleanDirectory(new File(RESOURCE_DIR + WRITER_STAGING));
    FileUtils.cleanDirectory(new File(RESOURCE_DIR + WRITER_OUTPUT));
    FileUtils.cleanDirectory(new File(RESOURCE_DIR + TMP));
    FileUtils.cleanDirectory(new File(RESOURCE_DIR + METRICS));
    FileUtils.cleanDirectory(new File(RESOURCE_DIR + SAMPLE_DIR));
    FileUtils.cleanDirectory(new File(RESOURCE_DIR + FINAL_DIR));
  }

  public static Properties getJobProperties(Properties jobProperties, String fileProperties)
      throws IOException {
    jobProperties.load(GobblinLocalJobLauncherUtils.class.getClassLoader().getResourceAsStream(fileProperties));
    return jobProperties;
  }

  public static Properties getJobProperties(String fileProperties)
      throws IOException {
    return getJobProperties(new Properties(), fileProperties);
  }
}
