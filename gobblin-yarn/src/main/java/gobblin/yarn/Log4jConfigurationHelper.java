/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.yarn;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;

import com.google.common.io.Closer;


/**
 * A helper class for programmatically configuring log4j.
 *
 * @author Yinan Li
 */
public class Log4jConfigurationHelper {

  static final String LOG4J_CONFIGURATION_FILE_NAME = "log4j-yarn.properties";

  /**
   * Update the log4j configuration.
   *
   * @param targetClass the target class used to get the original log4j configuration file as a resource
   * @param log4jPath the custom log4j configuration properties file path
   * @throws IOException if there's something wrong with updating the log4j configuration
   */
  public static void updateLog4jConfiguration(Class<?> targetClass, String log4jPath) throws IOException {
    Closer closer = Closer.create();
    try {
      InputStream fileInputStream = closer.register(new FileInputStream(log4jPath));
      InputStream inputStream = closer.register(targetClass.getResourceAsStream("/" + LOG4J_CONFIGURATION_FILE_NAME));
      Properties customProperties = new Properties();
      customProperties.load(fileInputStream);
      Properties originalProperties = new Properties();
      originalProperties.load(inputStream);

      for (Entry<Object, Object> entry : customProperties.entrySet()) {
        originalProperties.setProperty(entry.getKey().toString(), entry.getValue().toString());
      }

      LogManager.resetConfiguration();
      PropertyConfigurator.configure(originalProperties);
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }
}
