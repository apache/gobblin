/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.uif.util;

import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;

import com.linkedin.uif.configuration.ConfigurationKeys;


/**
 * A utility class used by the scheduler.
 *
 * @author ynli
 */
public class SchedulerUtils {

  // Extension of properties files
  private static final String JOB_PROPS_FILE_EXTENSION = ".properties";

  /**
   * Load job configurations from job configuration files stored under the
   * root job configuration directory.
   *
   * @param properties Gobblin framework configuration properties
   * @return list of job configuration properties
   */
  public static List<Properties> loadJobConfigs(Properties properties)
      throws IOException {
    Set<String> jobConfigFileExtensions = Sets.newHashSet(Splitter.on(",").omitEmptyStrings().split(properties
                .getProperty(ConfigurationKeys.JOB_CONFIG_FILE_EXTENSIONS_KEY,
                    ConfigurationKeys.DEFAULT_JOB_CONFIG_FILE_EXTENSIONS)));
    List<Properties> jobConfigs = Lists.newArrayList();
    loadJobConfigsRecursive(jobConfigs, properties, jobConfigFileExtensions,
        new File(properties.getProperty(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY)));
    return jobConfigs;
  }

  /**
   * Recursively load job configuration files under the given directory.
   */
  private static void loadJobConfigsRecursive(List<Properties> jobConfigs, Properties rootProps,
      Set<String> jobConfigFileExtensions, File jobConfigDir)
      throws IOException {

    // Get the properties file that ends with .properties if any
    String[] propertiesFiles = jobConfigDir.list(new FilenameFilter() {
      @Override
      public boolean accept(File file, String name) {
        return name.toLowerCase().endsWith(JOB_PROPS_FILE_EXTENSION);
      }
    });

    Closer closer = Closer.create();
    try {
      if (propertiesFiles != null && propertiesFiles.length > 0) {
        // There should be a single properties file in each directory (or sub directory)
        if (propertiesFiles.length != 1) {
          throw new RuntimeException("Found more than one .properties file in directory: " + jobConfigDir);
        }

        // Load the properties, which may overwrite the same properties defined
        // in the parent or ancestor directories.
        rootProps.load(closer.register(new FileReader(new File(jobConfigDir, propertiesFiles[0]))));
      }

      String[] names = jobConfigDir.list();
      if (names == null || names.length == 0) {
        return;
      }

      for (String name : names) {
        File file = new File(jobConfigDir, name);
        if (file.isDirectory()) {
          Properties rootPropsCopy = new Properties();
          rootPropsCopy.putAll(rootProps);
          loadJobConfigsRecursive(jobConfigs, rootPropsCopy, jobConfigFileExtensions, file);
        } else {
          int pos = file.getName().lastIndexOf(".");
          String fileExtension = pos >= 0 ? file.getName().substring(pos + 1) : "";
          if (!jobConfigFileExtensions.contains(fileExtension)) {
            // Not a job configuration file, ignore.
            continue;
          }

          File doneFile = new File(file + ".done");
          if (doneFile.exists()) {
            // Skip the job configuration file when a .done file with the same name exists,
            // which means the job configuration file is for a one-time job and the job has
            // already run and finished.
            continue;
          }

          Properties jobProps = new Properties();
          // Put all parent/ancestor properties first
          jobProps.putAll(rootProps);
          // Then load the job configuration properties defined in the pull file
          jobProps.load(closer.register(new FileReader(file)));
          jobProps.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY, file.getAbsolutePath());
          jobConfigs.add(jobProps);
        }
      }
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }
}
