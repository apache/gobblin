package gobblin.runtime.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.typesafe.config.Config;

import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.JobSpecNotFoundException;
import gobblin.util.ConfigUtils;
import gobblin.util.PathUtils;


public class FSJobCatalogJobSpecHelper {

  // Key used in the metadata of JobSpec.
  private static final String DESCRIPTION_KEY_IN_JOBSPEC = "description";
  private static final String VERSION_KEY_IN_JOBSPEC = "version";

  /**
   * On receiving a JobSpec object, materialized it into a real file into non-volatile storage layer.
   * @param jobConfDirPath The configuration root directory path.
   * @param jobSpec a {@link JobSpec}
   */
  public static void materializeJobSpec(Path jobConfDirPath, JobSpec jobSpec)
      throws IOException {
    Path path = new Path(jobConfDirPath, jobSpec.getUri().toString());
    try (FileSystem fs = path.getFileSystem(new Configuration())) {
      Properties props = ConfigUtils.configToProperties(jobSpec.getConfig());

      // Persist the metadata info as well.
      props.setProperty(DESCRIPTION_KEY_IN_JOBSPEC, jobSpec.getDescription());
      props.setProperty(VERSION_KEY_IN_JOBSPEC, jobSpec.getVersion());

      try (OutputStream os = fs.create(path)) {
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(os, Charsets.UTF_8));
        props.store(bufferedWriter, "");
        bufferedWriter.close();
      }
    }
  }

  /**
   * Used for shadow copying in the process of updating a existing job configuration file,
   * which requires deletion of the pre-existed copy of file and create a new one with the same name.
   * Steps:
   *  Create a new one in /tmp.
   *  Safely deletion of old one.
   *  copy the newly createad configuration file to jobConfigDir.
   *  Delete the shadow file.
   *
   * @param srcFileName The target file to be replaced,
   *               Noted that only the file name is provided, to guarantee in /tmp they are flat.
   */
  public static void updateExistedConfigFile(Path srcFileName, Path jobConfigDirPath, JobSpec jobSpec)
      throws IOException, JobSpecNotFoundException {
    Path shadowDirectoryPath = new Path("/tmp");
    Configuration conf = new Configuration();
    try (FileSystem shadow_fs = shadowDirectoryPath.getFileSystem(conf)) {
      Path shadowFilePath = PathUtils.mergePaths(shadowDirectoryPath, srcFileName);
      /* If previously existed, should delete anyway */
      if (shadow_fs.exists(shadowFilePath)) {
        shadow_fs.delete(shadowFilePath, false);
      }
      /*Write new file in /tmp folder*/
      materializeJobSpec(shadowDirectoryPath, jobSpec);

      /* Delete oldSpec and replace it with new one. */
      try (FileSystem configFs = jobConfigDirPath.getFileSystem(conf)) {
        Path toBeReplacedPath = new Path(jobConfigDirPath, jobSpec.getUri().toString());

        synchronized (FSJobCatalogLoadingHelper.class) {
          if (!configFs.exists(toBeReplacedPath)) {
            throw new JobSpecNotFoundException(jobSpec.getUri());
          }
          configFs.delete(toBeReplacedPath, false);
          FileUtil.copy(shadow_fs, shadowFilePath, configFs, toBeReplacedPath, true, true, conf);
        }
      }
    }
  }

  /**
   * Convert a raw jobProp that loaded from configuration file into JobSpec Object.
   * May remove some metadata inside, if the configuration file is originally
   * materialized from a JobSpec.
   *
   * @param rawProp The properties object that directly loaded from configuration file.
   * @param jobConfigURI The uri of target job configuration file.
   *                     Relative path to the root Configuration folder.
   * @return
   */
  public static JobSpec jobSpecConverter(Properties rawProp, URI jobConfigURI) {
    Optional<String> description = Optional.absent();
    Optional<Config> config;
    Optional<Properties> optionalProp;
    String version = "";

    if (rawProp != null) {
      // Points to noted:
      // 1.To ensure the transparency of JobCatalog, need to remove the addtional
      //   options that added through the materialization process.
      // 2. For files that created by user directly in the file system, there might not be
      //   version and description provided. Set them to default then, according to JobSpec constructor.

      if (rawProp.containsKey(VERSION_KEY_IN_JOBSPEC)) {
        version = rawProp.getProperty(VERSION_KEY_IN_JOBSPEC);
        rawProp.remove(VERSION_KEY_IN_JOBSPEC);
      } else {
        // Set the version as default.
        version = "1";
      }

      if (rawProp.containsKey(DESCRIPTION_KEY_IN_JOBSPEC)) {
        description = Optional.fromNullable(rawProp.getProperty(DESCRIPTION_KEY_IN_JOBSPEC));
        rawProp.remove(DESCRIPTION_KEY_IN_JOBSPEC);
      } else {
        // Set description as default.
        description = Optional.of("Gobblin job " + jobConfigURI);
      }

      // This on-the-fly added attribute is no longer needed when *.properties is not considered.
      // For the system configuration they should be kept, like ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY
      if (rawProp.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY)) {
        rawProp.remove(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY);
      }
      if (rawProp.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY)) {
        rawProp.remove(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY);
      }
      if (rawProp.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY)) {
        rawProp.remove(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY);
      }
    }
    optionalProp = Optional.fromNullable(rawProp);
    config = Optional.fromNullable(ConfigUtils.propertiesToConfig(rawProp));

    // The builder has null-checker. Leave the checking there.
    return JobSpec.builder(jobConfigURI)
        .withConfigAsProperties(optionalProp.get())
        .withConfig(config.get())
        .withDescription(description.get())
        .withVersion(version)
        .build();
  }
}
