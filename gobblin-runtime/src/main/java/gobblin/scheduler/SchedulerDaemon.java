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

package gobblin.scheduler;

import java.util.Properties;
import java.util.UUID;

import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.PropertiesConfiguration;

import gobblin.runtime.app.ServiceBasedAppLauncher;
import gobblin.util.PropertiesUtils;


/**
 * A class that runs the {@link JobScheduler} in a daemon process for standalone deployment.
 *
 * @author Yinan Li
 */
public class SchedulerDaemon extends ServiceBasedAppLauncher {

  private SchedulerDaemon(Properties defaultProperties, Properties customProperties) throws Exception {
    this(PropertiesUtils.combineProperties(defaultProperties, customProperties));
  }

  private SchedulerDaemon(Properties properties) throws Exception {
    super(properties, getAppName(properties));
    addService(new JobScheduler(properties));
  }

  private static String getAppName(Properties properties) {
    return properties.getProperty(ServiceBasedAppLauncher.APP_NAME, "SchedulerDaemon-" + UUID.randomUUID());
  }

  public static void main(String[] args)
      throws Exception {
    if (args.length < 1 || args.length > 2) {
      System.err.println(
          "Usage: SchedulerDaemon <default configuration properties file> [custom configuration properties file]");
      System.exit(1);
    }

    // Load default framework configuration properties
    Properties defaultProperties = ConfigurationConverter.getProperties(new PropertiesConfiguration(args[0]));

    // Load custom framework configuration properties (if any)
    Properties customProperties = new Properties();
    if (args.length == 2) {
      customProperties.putAll(ConfigurationConverter.getProperties(new PropertiesConfiguration(args[1])));
    }

    // Start the scheduler daemon
    new SchedulerDaemon(defaultProperties, customProperties).start();
  }
}
