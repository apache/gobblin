package gobblin.runtime.util;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;

import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.JobSpecNotFoundException;
import gobblin.runtime.job_catalog.FSJobCatalog;
import gobblin.util.PathUtils;
import gobblin.util.filesystem.PathAlterationDetector;
import gobblin.util.filesystem.PathAlterationListener;
import gobblin.util.filesystem.PathAlterationObserver;

import javax.annotation.Nullable;
import lombok.AllArgsConstructor;


public class FSJobCatalogHelper {

  // Key used in the metadata of JobSpec.
  private static final String DESCRIPTION_KEY_IN_JOBSPEC = "gobblin.fsJobCatalog.description";
  private static final String VERSION_KEY_IN_JOBSPEC = "gobblin.fsJobCatalog.version";

  /**
   * Used for shadow copying in the process of updating a existing job configuration file,
   * which requires deletion of the pre-existed copy of file and create a new one with the same name.
   * Steps:
   *  Create a new one in /tmp.
   *  Safely deletion of old one.
   *  copy the newly createad configuration file to jobConfigDir.
   *  Delete the shadow file.
   */
  public static void materializedJobSpec(Path jobSpecPath, JobSpec jobSpec, FileSystem fs)
      throws IOException, JobSpecNotFoundException {
    Path shadowDirectoryPath = new Path("/tmp");
    Path shadowFilePath = new Path(shadowDirectoryPath, UUID.randomUUID().toString());
    /* If previously existed, should delete anyway */
    if (fs.exists(shadowFilePath)) {
      fs.delete(shadowFilePath, false);
    }

    Map<String, String> injectedKeys = ImmutableMap.of(DESCRIPTION_KEY_IN_JOBSPEC, jobSpec.getDescription(),
        VERSION_KEY_IN_JOBSPEC, jobSpec.getVersion());
    String renderedConfig = ConfigFactory.parseMap(injectedKeys).withFallback(jobSpec.getConfig())
        .root().render(ConfigRenderOptions.defaults());
    try (DataOutputStream os = fs.create(shadowFilePath);
        Writer writer = new OutputStreamWriter(os, Charsets.UTF_8)) {
      writer.write(renderedConfig);
    }

    /* (Optionally:Delete oldSpec) and copy the new one in. */
    synchronized (FSJobCatalog.class) {
      if (fs.exists(jobSpecPath)) {
        fs.delete(jobSpecPath, false);
      }
      fs.rename(shadowFilePath, jobSpecPath);
    }
  }

  /**
   * Instance of this function is passed as a parameter into other transform function,
   * for converting Properties object into JobSpec object.
   */
  @AllArgsConstructor
  public static class JobSpecConverter implements Function<Config, JobSpec> {
    private final Path jobConfigDirPath;
    private final Optional<String> extensionToStrip;

    public URI computeURI(Path filePath) {
      // Make sure this is relative
      URI uri = PathUtils.relativizePath(filePath,
          jobConfigDirPath).toUri();
      if (this.extensionToStrip.isPresent()) {
        uri = PathUtils.removeExtension(new Path(uri), this.extensionToStrip.get()).toUri();
      }
      return uri;
    }

    @Nullable
    @Override
    public JobSpec apply(Config rawConfig) {

      URI jobConfigURI = computeURI(new Path(rawConfig.getString(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY)));

      // Points to noted:
      // 1.To ensure the transparency of JobCatalog, need to remove the addtional
      //   options that added through the materialization process.
      // 2. For files that created by user directly in the file system, there might not be
      //   version and description provided. Set them to default then, according to JobSpec constructor.

      String version;
      if (rawConfig.hasPath(VERSION_KEY_IN_JOBSPEC)) {
        version = rawConfig.getString(VERSION_KEY_IN_JOBSPEC);
      } else {
        // Set the version as default.
        version = "1";
      }

      String description;
      if (rawConfig.hasPath(DESCRIPTION_KEY_IN_JOBSPEC)) {
        description = rawConfig.getString(DESCRIPTION_KEY_IN_JOBSPEC);
      } else {
        // Set description as default.
        description = "Gobblin job " + jobConfigURI;
      }

      // The builder has null-checker. Leave the checking there.
      return JobSpec.builder(jobConfigURI)
          .withConfig(rawConfig)
          .withDescription(description)
          .withVersion(version)
          .build();
    }
  }

  public static Set<String> getJobConfigurationFileExtensions(Properties properties) {
    Iterable<String> jobConfigFileExtensionsIterable = Splitter.on(",")
        .omitEmptyStrings()
        .trimResults()
        .split(properties.getProperty(ConfigurationKeys.JOB_CONFIG_FILE_EXTENSIONS_KEY,
            ConfigurationKeys.DEFAULT_JOB_CONFIG_FILE_EXTENSIONS));
    return ImmutableSet.copyOf(Iterables.transform(jobConfigFileExtensionsIterable, new Function<String, String>() {
      @Override
      public String apply(String input) {
        return null != input ? input.toLowerCase() : "";
      }
    }));
  }

  /**
   * Create and attach {@link PathAlterationDetector}s for the given
   * root directory and any nested subdirectories under the root directory to the given
   * {@link PathAlterationDetector}.
   * @param detector  a {@link PathAlterationDetector}
   * @param listener a {@link gobblin.util.filesystem.PathAlterationListener}
   * @param observerOptional Optional observer object. For testing routine, this has been initialized by user.
   *                         But for general usage, the observer object is created inside this method.
   * @param rootDirPath root directory
   * @throws IOException
   */
  public static void addPathAlterationObserver(PathAlterationDetector detector, PathAlterationListener listener,
      Optional<PathAlterationObserver> observerOptional, Path rootDirPath)
      throws IOException {
    PathAlterationObserver observer;
    if (observerOptional.isPresent()) {
      observer = observerOptional.get();
    } else {
      observer = new PathAlterationObserver(rootDirPath);
    }
    observer.addListener(listener);
    detector.addObserver(observer);
  }
}
