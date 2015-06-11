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

package gobblin.azkaban;

import java.util.Properties;

import org.apache.log4j.Logger;

import azkaban.jobExecutor.AbstractJob;

import gobblin.test.setup.config.TestHarnessLauncher;

/**
 * This class launches the TestHarness framework using Azkaban
 *
 * Created by spyne on 6/8/15.
 */
public class AzkabanIntegrationTestLauncher extends AbstractJob {
  private static final Logger LOG = Logger.getLogger(AzkabanIntegrationTestLauncher.class);

  private final Properties properties;

  private TestHarnessLauncher launcher;

  public AzkabanIntegrationTestLauncher(String id, Properties properties) {
    super(id, LOG);
    this.properties = properties;
  }

  @Override
  public void run() throws Exception {
    // Get the test harness launcher instance

    // Read the properties file
    launcher.configure(properties);

    // Prepare the tests to be run
    launcher.prepareTest();

    // Execute them
    launcher.launchTest();
  }
}
