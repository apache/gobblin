package gobblin.runtime.job_catalog;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.Path;

import com.typesafe.config.Config;

import gobblin.runtime.api.JobSpec;
import gobblin.runtime.util.FSJobCatalogHelper;
import gobblin.util.PathUtils;
import gobblin.util.PullFileLoader;
import gobblin.util.filesystem.PathAlterationListenerAdaptor;


public class FSPathAlterationListenerAdaptor extends PathAlterationListenerAdaptor {
  private final Path jobConfDirPath;
  private final PullFileLoader loader;
  private final Config sysConfig;
  private final JobCatalogListenersList listeners;
  private final FSJobCatalogHelper.JobSpecConverter converter;

  FSPathAlterationListenerAdaptor(Path jobConfDirPath, PullFileLoader loader, Config sysConfig,
      JobCatalogListenersList listeners, FSJobCatalogHelper.JobSpecConverter converter) {
    this.jobConfDirPath = jobConfDirPath;
    this.loader = loader;
    this.sysConfig = sysConfig;
    this.listeners = listeners;
    this.converter = converter;
  }

  /**
   * Transform the event triggered by file creation into JobSpec Creation for Driver (One of the JobCatalogListener )
   * Create a new JobSpec object and notify each of member inside JobCatalogListenersList
   * @param rawPath This could be complete path to the newly-created configuration file.
   */
  @Override
  public void onFileCreate(Path rawPath) {
    try {
      JobSpec newJobSpec =
          this.converter.apply(loader.loadPullFile(rawPath, sysConfig, false));
      listeners.onAddJob(newJobSpec);
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  /**
   * For already deleted job configuration file, the only identifier is path
   * it doesn't make sense to loadJobConfig Here.
   * @param rawPath This could be the complete path to the newly-deleted configuration file.
   */
  @Override
  public void onFileDelete(Path rawPath) {
    URI jobSpecUri = this.converter.computeURI(rawPath);
    // TODO: fix version
    listeners.onDeleteJob(jobSpecUri, null);
  }

  @Override
  public void onFileChange(Path rawPath) {
    try {
      JobSpec updatedJobSpec =
          this.converter.apply(loader.loadPullFile(rawPath, sysConfig, false));
      listeners.onUpdateJob(updatedJobSpec);
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }
  }
}