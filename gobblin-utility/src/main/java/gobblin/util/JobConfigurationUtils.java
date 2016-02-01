/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;

import gobblin.configuration.State;
import org.apache.hadoop.fs.Path;


/**
 * A utility class for working with job configurations.
 *
 * @author Yinan Li
 */
public class JobConfigurationUtils {

  /**
   * Get a new {@link Properties} instance by combining a given system configuration {@link Properties}
   * object (first) and a job configuration {@link Properties} object (second).
   *
   * @param sysProps the given system configuration {@link Properties} object
   * @param jobProps the given job configuration {@link Properties} object
   * @return a new {@link Properties} instance
   */
  public static Properties combineSysAndJobProperties(Properties sysProps, Properties jobProps) {
    Properties combinedJobProps = new Properties();
    combinedJobProps.putAll(sysProps);
    combinedJobProps.putAll(jobProps);
    return combinedJobProps;
  }

  /**
   * Put all configuration properties in a given {@link Properties} object into a given
   * {@link Configuration} object.
   *
   * @param properties the given {@link Properties} object
   * @param configuration the given {@link Configuration} object
   */
  public static void putPropertiesIntoConfiguration(Properties properties, Configuration configuration) {
    for (String name : properties.stringPropertyNames()) {
      configuration.set(name, properties.getProperty(name));
    }
  }

  /**
   * Put all configuration properties in a given {@link State} object into a given
   * {@link Configuration} object.
   *
   * @param state the given {@link State} object
   * @param configuration the given {@link Configuration} object
   */
  public static void putStateIntoConfiguration(State state, Configuration configuration) {
    for (String key : state.getPropertyNames()) {
      configuration.set(key, state.getProp(key));
    }
  }

  /**
   * Load the properties from the specified file into a {@link Properties} object.
   *
   * @param fileName the name of the file to load properties from
   * @return a new {@link Properties} instance
   */
  public static Properties fileToProperties(String fileName) throws IOException, ConfigurationException {
    Path filePath = new Path(fileName);
    PropertiesConfiguration propsConfig = new PropertiesConfiguration();
    propsConfig.load(filePath.getFileSystem(new Configuration()).open(filePath));
    return ConfigurationConverter.getProperties(propsConfig);
  }
}
