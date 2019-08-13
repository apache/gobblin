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

package org.apache.gobblin.scheduler;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.gobblin.runtime.app.ServiceBasedAppLauncher;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PropertiesUtils;
import lombok.extern.slf4j.Slf4j;

/**
 * A class that runs the {@link JobScheduler} in a daemon process for standalone deployment.
 *
 * @author Yinan Li
 */
@Slf4j
public class SchedulerDaemon extends ServiceBasedAppLauncher {

  private SchedulerDaemon(Properties defaultProperties, Properties customProperties) throws Exception {
    this(PropertiesUtils.combineProperties(defaultProperties, customProperties));
  }

  public SchedulerDaemon(Properties properties) throws Exception {
    super(properties, getAppName(properties));
    SchedulerService schedulerService = new SchedulerService(properties);
    addService(schedulerService);
    addService(new JobScheduler(properties, schedulerService));
  }

  private static String getAppName(Properties properties) {
    return properties.getProperty(ServiceBasedAppLauncher.APP_NAME, "SchedulerDaemon-" + UUID.randomUUID());
  }

  public static void main(String[] args)
          throws Exception {
    if (args.length < 1 || args.length > 2) {
      System.err.println("Usage: SchedulerDaemon <default configuration properties file> [custom configuration properties file]");
      System.exit(1);
    }

    // Load default framework configuration properties
    Path path = Paths.get(args[0]);
    File configFile = path.toFile();

    Config config = null;
    if (configFile.exists()){
      config = ConfigFactory.parseFile(configFile);
    }else{
      System.err.println("invalid config file path: "+args[0]);
      System.exit(1);
    }

    // Start the scheduler daemon
    new SchedulerDaemon(ConfigUtils.configToProperties(config)).start();
  }
}
