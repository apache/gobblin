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

package gobblin.runtime.template;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.api.client.util.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigResolveOptions;

import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.api.JobTemplate;
import gobblin.runtime.api.SpecNotFoundException;
import gobblin.runtime.job_catalog.PackagedTemplatesJobCatalogDecorator;
import gobblin.util.ConfigUtils;
import gobblin.util.PathUtils;
import gobblin.util.PullFileLoader;

import lombok.Data;


/**
 * A simple script to convert old style pull files into *.conf files using templates.
 *
 * This script will load *.pull files and a template, and will attempt to create a *.conf file using that template
 * that resolves to the same job configuration as the pull file resolves. To do this, it compares all of the config
 * key-value pairs in the pull file against those in the template, and writes out only those key-value pairs that
 * are different from the template. It then writes the *.conf file to the output directory.
 *
 * The template can include a key {@link #DO_NOT_OVERRIDE_KEY} with an array of strings representing keys in the template
 * that should not appear in the output *.conf even if they resolve to different values.
 *
 * Note the template must be a resource-style template and it must be in the classpath.
 *
 * Usage:
 * PullFileToConfigConverter <pullFilesRootPath> <pullFilesToConvertGlob> resource:///<templatePath> <sysConfigPath> <outputDir>
 *
 */
@Data
public class PullFileToConfigConverter {

  public static final String DO_NOT_OVERRIDE_KEY = "pullFileToConfigConverter.doNotOverride";

  private static final Set<String> FILTER_KEYS = ImmutableSet.of("job.config.path", "jobconf.dir", "jobconf.fullyQualifiedPath");

  private final Path pullFileRootPath;
  private final Path fileGlobToConvert;
  private final Path templateURI;
  private final File sysConfigPath;
  private final Path outputPath;

  public static void main(String[] args) throws Exception {
    if (args.length != 5) {
      System.out.println("Usage: PullFileToConfigConverter <pullFilesRootPath> <pullFilesToConvertGlob> resource:///<templatePath> <sysConfigPath> <outputDir>");
      System.exit(1);
    }

    new PullFileToConfigConverter(new Path(args[0]), new Path(args[1]), new Path(args[2]), new File(args[3]), new Path(args[4]))
        .convert();
  }

  public void convert() throws IOException {
    Config baseConfig = ConfigFactory.parseString(DO_NOT_OVERRIDE_KEY + ": []");

    FileSystem pullFileFs = pullFileRootPath.getFileSystem(new Configuration());
    FileSystem outputFs = this.outputPath.getFileSystem(new Configuration());

    Config sysConfig = ConfigFactory.parseFile(this.sysConfigPath);

    PullFileLoader pullFileLoader = new PullFileLoader(this.pullFileRootPath, pullFileFs,
        PullFileLoader.DEFAULT_JAVA_PROPS_PULL_FILE_EXTENSIONS, PullFileLoader.DEFAULT_HOCON_PULL_FILE_EXTENSIONS);

    PackagedTemplatesJobCatalogDecorator catalog = new PackagedTemplatesJobCatalogDecorator();

    ConfigResolveOptions configResolveOptions = ConfigResolveOptions.defaults();
    configResolveOptions = configResolveOptions.setAllowUnresolved(true);

    ResourceBasedJobTemplate template;
    Config templateConfig;
    try {
      template = (ResourceBasedJobTemplate) catalog.getTemplate(templateURI.toUri());

      templateConfig = sysConfig.withFallback(template.getRawTemplateConfig()).withFallback(baseConfig).resolve(configResolveOptions);
    } catch (SpecNotFoundException|JobTemplate.TemplateException exc) {
      throw new IOException(exc);
    }

    Set<String> doNotOverride = templateConfig.hasPath(DO_NOT_OVERRIDE_KEY) ?
        Sets.newHashSet(templateConfig.getStringList(DO_NOT_OVERRIDE_KEY)) : Sets.<String>newHashSet();

    ConfigRenderOptions configRenderOptions = ConfigRenderOptions.defaults();
    configRenderOptions = configRenderOptions.setComments(false);
    configRenderOptions = configRenderOptions.setOriginComments(false);
    configRenderOptions = configRenderOptions.setFormatted(true);
    configRenderOptions = configRenderOptions.setJson(false);

    for (FileStatus pullFile : pullFileFs.globStatus(this.fileGlobToConvert)) {
      Config pullFileConfig = pullFileLoader.loadPullFile(pullFile.getPath(), ConfigFactory.empty(), true).resolve();
      Map<String, String> outputConfigMap = Maps.newHashMap();

      outputConfigMap.put(ConfigurationKeys.JOB_TEMPLATE_PATH, this.templateURI.toString());

      boolean somethingChanged;
      do {
        somethingChanged = false;

        Config currentOutputConfig = ConfigFactory.parseMap(outputConfigMap);
        Config currentResolvedConfig = currentOutputConfig.withFallback(templateConfig).resolve(configResolveOptions);

        for (Map.Entry<Object, Object> entry : ConfigUtils.configToProperties(pullFileConfig).entrySet()) {
          String key = (String) entry.getKey();
          String value = (String) entry.getValue();

          try {
            if ((!currentResolvedConfig.hasPath(key)) ||
                (!currentResolvedConfig.getString(key).equals(value) && !doNotOverride.contains(key))) {
              if (!FILTER_KEYS.contains(key)) {
                somethingChanged = true;
                outputConfigMap.put(key, value);
              }
            }
          } catch (ConfigException.NotResolved nre) {
            // path is unresolved in config, will try again next iteration
          }
        }

      } while (somethingChanged);

      try {
        Config outputConfig = ConfigFactory.parseMap(outputConfigMap);
        Config currentResolvedConfig = outputConfig.withFallback(templateConfig).resolve();

        String rendered = outputConfig.root().render(configRenderOptions);

        Path newPath = PathUtils.removeExtension(pullFile.getPath(),
            PullFileLoader.DEFAULT_JAVA_PROPS_PULL_FILE_EXTENSIONS.toArray(new String[]{}));
        newPath = PathUtils.addExtension(newPath, "conf");
        newPath = new Path(this.outputPath, newPath.getName());

        FSDataOutputStream os = outputFs.create(newPath);
        os.write(rendered.getBytes(Charsets.UTF_8));
        os.close();
      } catch (ConfigException.NotResolved nre) {
        throw new IOException("Not all configuration keys were resolved in pull file " + pullFile.getPath(), nre);
      }

    }
  }
}
