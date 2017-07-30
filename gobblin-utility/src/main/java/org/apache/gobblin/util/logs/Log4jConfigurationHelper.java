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

package gobblin.util.logs;

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

  /**
   * Update the log4j configuration.
   *
   * @param targetClass the target class used to get the original log4j configuration file as a resource
   * @param log4jPath the custom log4j configuration properties file path
   * @param log4jFileName the custom log4j configuration properties file name
   * @throws IOException if there's something wrong with updating the log4j configuration
   */
  public static void updateLog4jConfiguration(Class<?> targetClass, String log4jPath, String log4jFileName)
      throws IOException {
    Closer closer = Closer.create();
    try {
      InputStream fileInputStream = closer.register(new FileInputStream(log4jPath));
      InputStream inputStream = closer.register(targetClass.getResourceAsStream("/" + log4jFileName));
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
