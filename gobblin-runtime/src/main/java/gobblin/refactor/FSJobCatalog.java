package gobblin.refactor;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.ConfigurationKeys;


/**
 * The job Catalog for file system to persist the job configuration information.
 * For stateful storage like file system,
 */
public class FSJobCatalog implements JobCatalog {

  private Path jobConfDirPath;
  private final List<JobCatalogListener> listeners = new CopyOnWriteArrayList<>();
  private HashMap<URI, JobSpec> persistedJobs = new HashMap<>();
  private static final Logger LOGGER = LoggerFactory.getLogger(FSJobCatalog.class);
  private Configuration conf = new Configuration();
  private FileSystem fs;

  /**
   * Initialize the JobCatalog, fetch all jobs in jobConfDirPath.
   * @param jobConfDirPath
   * @throws Exception
   */
  public FSJobCatalog(Path jobConfDirPath)
      throws Exception {
    try {
      fs = jobConfDirPath.getFileSystem(conf);
      // initialization of target folder.
      if (fs.exists(this.jobConfDirPath)) {
        LOGGER.info("Loading job configurations from " + jobConfDirPath);
        Properties properties = new Properties();
        properties.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY, jobConfDirPath.toString());
        persistedJobs = JobCatalogUtils.jobSpecListToMap(JobCatalogUtils.loadGenericJobConfigs(properties));
        LOGGER.info("Loaded " + persistedJobs.size() + " job configuration(s)");
      } else {
        LOGGER.warn("Job configuration directory " + jobConfDirPath + " not found");
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public Path getPath() {
    return this.jobConfDirPath;
  }

  /**
   * Fetch all the job files under the jobConfDirPath
   * @return A collection of JobSpec
   */
  @Override
  public List<JobSpec> getJobs() {
    return new ArrayList<JobSpec>(persistedJobs.values());
  }

  @Override
  public JobSpec getJob(URI uri) {
    if (!persistedJobs.containsKey(uri)) {
      throw new RuntimeException("The key " + uri + " doesn't exist in FSJobCatalog");
    }
    return persistedJobs.get(uri);
  }

  @Override
  public void addListener(JobCatalogListener jobListener) {
    if (jobListener != null) {
      listeners.add(jobListener);
    }
  }

  @Override
  public void removeListener(JobCatalogListener jobListener) {
    if (jobListener != null) {
      listeners.add(jobListener);
    }
  }
}
