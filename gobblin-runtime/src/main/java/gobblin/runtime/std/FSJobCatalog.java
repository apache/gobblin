package gobblin.runtime.std;

import java.net.URI;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import gobblin.runtime.util.JobCatalogUtils;
import gobblin.runtime.api.JobCatalog;
import gobblin.runtime.api.JobCatalogListener;
import gobblin.runtime.api.JobSpec;


/**
 * The job Catalog for file system to persist the job configuration information.
 * For stateful storage like file system,
 */
public class FSJobCatalog implements JobCatalog {
  private Path jobConfDirPath;
  private final JobCatalogListenersList listeners;
  private static final Logger LOGGER = LoggerFactory.getLogger(FSJobCatalog.class);

  public enum Action {
    SINGLEJOB, BATCHJOB
  }

  /**
   * Initialize the JobCatalog, fetch all jobs in jobConfDirPath.
   * @param jobConfDirPath
   * @throws Exception
   */
  public FSJobCatalog(Path jobConfDirPath)
      throws Exception {
    this.jobConfDirPath = jobConfDirPath;
    this.listeners = new JobCatalogListenersList(Optional.of(LOGGER));
  }

  /**
   * Fetch all the job files under the jobConfDirPath
   * @return A collection of JobSpec
   */
  @Override
  public synchronized List<JobSpec> getJobs() {
    return JobCatalogUtils.loadJobConfigHelper(this.jobConfDirPath, Action.BATCHJOB, new Path("dummy Path"));
  }

  /**
   * Fetch single job file based on its URI,
   * return null requested URI not existed
   * requires null checking
   * @param uri
   * @return
   */
  @Override
  public synchronized JobSpec getJobSpec(URI uri) {
    Path targetJobSpecPath = new Path(uri);

    List<JobSpec> resultJobSpecList =
        JobCatalogUtils.loadJobConfigHelper(this.jobConfDirPath, Action.SINGLEJOB, targetJobSpecPath);
    if (resultJobSpecList == null || resultJobSpecList.size() == 0) {
      LOGGER.warn("No JobSpec with URI:" + uri + " is found.");
      return null;
    } else {
      return resultJobSpecList.get(0);
    }
  }

  @Override
  public synchronized void addListener(JobCatalogListener jobListener) {
    Preconditions.checkNotNull(jobListener);

    this.listeners.addListener(jobListener);

    List<JobSpec> currentJobSpecList = this.getJobs();
    if (currentJobSpecList == null || currentJobSpecList.size() == 0) {
      return;
    } else {
      for (JobSpec jobSpecEntry : currentJobSpecList) {
        JobCatalogListenersList.AddJobCallback addJobCallback =
            new JobCatalogListenersList.AddJobCallback(jobSpecEntry);
        this.listeners.callbackOneListener(addJobCallback, jobListener);
      }
    }
  }

  @Override
  public synchronized void removeListener(JobCatalogListener jobListener) {
    this.listeners.removeListener(jobListener);
  }

  /**
   * return the path that it controls.
   * @return
   */
  public Path getPath() {
    return this.jobConfDirPath;
  }
}
