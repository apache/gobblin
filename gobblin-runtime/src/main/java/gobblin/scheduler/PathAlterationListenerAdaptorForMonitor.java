package gobblin.scheduler;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.JobException;
import gobblin.runtime.listeners.EmailNotificationJobListener;
import gobblin.runtime.listeners.RunOnceJobListener;
import gobblin.util.SchedulerUtils;
import gobblin.util.filesystem.PathAlterationListenerAdaptor;



/**
 * Inner subclass of PathAlterationListenerAdaptor for implementation of Listen's methods,
 * avoiding anonymous class
 */
public class PathAlterationListenerAdaptorForMonitor extends PathAlterationListenerAdaptor {

  private static final Logger LOG = LoggerFactory.getLogger(JobScheduler.class);

  Path jobConfigFileDirPath;
  JobScheduler jobScheduler ;

  PathAlterationListenerAdaptorForMonitor(Path jobConfigFileDirPath, JobScheduler jobScheduler) {
    this.jobConfigFileDirPath = jobConfigFileDirPath;
    this.jobScheduler = jobScheduler ;
  }

  public void loadNewJobConfigAndHandleNewJob(Path path, JobScheduler.Action action) {
    // Load the new job configuration and schedule the new job

    String customizedInfo = "";
    try {
      Properties jobProps =
          SchedulerUtils.loadGenericJobConfig(this.jobScheduler.properties, path, jobConfigFileDirPath);

      switch (action) {
        case SCHEDULE:
          boolean runOnce = Boolean.valueOf(jobProps.getProperty(ConfigurationKeys.JOB_RUN_ONCE_KEY, "false"));
          customizedInfo = "schedule";
          jobScheduler.scheduleJob(jobProps, runOnce ? new RunOnceJobListener() : new EmailNotificationJobListener());
          break;
        case UNSCHEDULE:
          String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
          customizedInfo = "unschedule";
          jobScheduler.unscheduleJob(jobName);
          break;
        case RESCHEDULE:
          customizedInfo = "reschedule";
          rescheduleJob(jobProps);
          break;
        default:
          break;
      }
    } catch (ConfigurationException | IOException e) {
      LOG.error("Failed to load from job configuration file " + path.toString(), e);
    } catch (JobException je) {
      LOG.error("Failed to " + customizedInfo + " new job loaded from job configuration file " + path.toString(), je);
    }
  }

  public void loadNewCommonConfigAndHandleNewJob(Path path, JobScheduler.Action action) {
    String customizedInfoAction = "";
    String customizedInfoResult = "";
    try {
      for (Properties jobProps : SchedulerUtils.loadGenericJobConfigs(jobScheduler.properties, path,
          jobConfigFileDirPath)) {
        try {
          switch (action) {
            case SCHEDULE:
              boolean runOnce = Boolean.valueOf(jobProps.getProperty(ConfigurationKeys.JOB_RUN_ONCE_KEY, "false"));
              customizedInfoAction = "schedule";
              customizedInfoResult = "creation or equivalent action";
              jobScheduler.scheduleJob(jobProps, runOnce ? new RunOnceJobListener() : new EmailNotificationJobListener());
              break;
            case RESCHEDULE:
              customizedInfoAction = "reschedule";
              customizedInfoResult = "change";
              rescheduleJob(jobProps);
              break;
            case UNSCHEDULE:
              String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
              customizedInfoAction = "unschedule";
              customizedInfoResult = "deletion";
              jobScheduler.unscheduleJob(jobName);
              break;
            default:
              break;
          }
        } catch (JobException je) {
          LOG.error("Failed to " + customizedInfoAction + " job reloaded from job configuration file "
              + jobProps.getProperty(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY), je);
        }
      }
    } catch (ConfigurationException | IOException e) {
      LOG.error(
          "Failed to reload job configuration files affected by " + customizedInfoResult + " to " + path.toString(),
          e);
    }
  }

  @Override
  public void onFileCreate(Path path) {
    String fileExtension = path.getName().substring(path.getName().lastIndexOf('.') + 1);
    String noExtFileName = path.getName().substring(0, path.getName().lastIndexOf('.'));
    if (fileExtension.equalsIgnoreCase(SchedulerUtils.JOB_PROPS_FILE_EXTENSION)) {
      //check no other properties pre-existed
      try {
        if (checkCommonPropExistance(path.getParent(), noExtFileName)) {
          return;
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
      LOG.info("Detected cration to common properties file" + path.toString());
      // New .properties file founded with some new attributes, reschedule jobs.
      loadNewCommonConfigAndHandleNewJob(path, JobScheduler.Action.RESCHEDULE);
      return ;
    }
    if (!jobScheduler.jobConfigFileExtensions.contains(fileExtension)) {
      return;
    }
    LOG.info("Detected new job configuration file " + path.toString());

    loadNewJobConfigAndHandleNewJob(path, JobScheduler.Action.SCHEDULE);
  }

  /**
   * Called when a job configuration file is changed.
   */
  @Override
  public void onFileChange(Path path) {
    String fileExtension = path.getName().substring(path.getName().lastIndexOf('.') + 1);
    if (fileExtension.equalsIgnoreCase(SchedulerUtils.JOB_PROPS_FILE_EXTENSION)) {
      LOG.info("Detected change to common properties file " + path.toString());
      loadNewCommonConfigAndHandleNewJob(path, JobScheduler.Action.RESCHEDULE);
      return;
    }

    if (!jobScheduler.jobConfigFileExtensions.contains(fileExtension)) {
      // Not a job configuration file, ignore.
      return;
    }

    LOG.info("Detected change to job configuration file " + path.toString());
    loadNewJobConfigAndHandleNewJob(path, JobScheduler.Action.RESCHEDULE);
  }

  /**
   * Called when a job configuration file is deleted.
   */
  @Override
  public void onFileDelete(Path path) {
    String fileExtension = path.getName().substring(path.getName().lastIndexOf('.') + 1);
    if (fileExtension.equalsIgnoreCase(SchedulerUtils.JOB_PROPS_FILE_EXTENSION)) {
      LOG.info("Detected deletion to common properties file " + path.toString());
      // For JobProps, deletion in local folder means inheritance from ancestor folder and reschedule.
      loadNewCommonConfigAndHandleNewJob(path, JobScheduler.Action.RESCHEDULE);
      return;
    }

    if (!jobScheduler.jobConfigFileExtensions.contains(fileExtension)) {
      // Not a job configuration file, ignore.
      return;
    }

    LOG.info("Detected deletion to job configuration file " + path.toString());
    // As for normal job file, deletion means unschedule
    loadNewJobConfigAndHandleNewJob(path, JobScheduler.Action.UNSCHEDULE);
  }

  private void rescheduleJob(Properties jobProps)
      throws JobException {
    String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
    // First unschedule and delete the old job
    jobScheduler.unscheduleJob(jobName);
    boolean runOnce = Boolean.valueOf(jobProps.getProperty(ConfigurationKeys.JOB_RUN_ONCE_KEY, "false"));
    // Reschedule the job with the new job configuration
    jobScheduler.scheduleJob(jobProps, runOnce ? new RunOnceJobListener() : new EmailNotificationJobListener());
  }

  /**
   * Given the target rootPath, check if there's common properties existed. Return false if so.
   * @param rootPath
   * @return
   */
  private boolean checkCommonPropExistance(Path rootPath, String noExtFileName)
      throws IOException {
    Configuration conf = new Configuration();
    FileStatus[] children = rootPath.getFileSystem(conf).listStatus(rootPath);

    for (FileStatus aChild : children) {
      if (aChild.getPath().getName().contains(noExtFileName)) {
        return false;
      }
    }

    return true;
  }
}
