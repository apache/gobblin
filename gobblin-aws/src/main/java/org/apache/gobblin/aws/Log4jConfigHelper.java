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

package org.apache.gobblin.aws;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;

import com.google.common.io.Closer;

import org.apache.gobblin.annotation.Alpha;


/**
 * A helper class for programmatically configuring log4j.
 *
 * @author Abhishek Tiwari
 */
@Alpha
public class Log4jConfigHelper {
  /**
   * Update the log4j configuration.
   *
   * @param targetClass the target class used to get the original log4j configuration file as a resource
   * @param log4jFileName the custom log4j configuration properties file name
   * @throws IOException if there's something wrong with updating the log4j configuration
   */
  public static void updateLog4jConfiguration(Class<?> targetClass, String log4jFileName)
      throws IOException {
    final Closer closer = Closer.create();
    try {
      final InputStream inputStream = closer.register(targetClass.getResourceAsStream("/" + log4jFileName));
      final Properties originalProperties = new Properties();
      originalProperties.load(inputStream);

      LogManager.resetConfiguration();
      PropertyConfigurator.configure(originalProperties);
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }
}
