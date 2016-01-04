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

package gobblin.azkaban;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.Logger;

import azkaban.jobExecutor.AbstractJob;

import com.typesafe.config.ConfigFactory;

import gobblin.yarn.GobblinYarnAppLauncher;


/**
 * A utility class for launching a Gobblin application on Yarn through Azkaban.
 *
 * <p>
 *   This class starts the driver of the Gobblin application on Yarn, which will be up running until the
 *   Azkaban job is killed/cancelled or the shutdown hook gets called and causes the driver to stop.
 * </p>
 *
 * <p>
 *   See {@link GobblinYarnAppLauncher} for details information on the launcher/driver of the Gobblin
 *   application on Yarn.
 * </p>
 *
 * @author Yinan Li
 */
@SuppressWarnings("unused")
public class AzkabanGobblinYarnAppLauncher extends AbstractJob {

  private static final Logger LOGGER = Logger.getLogger(AzkabanJobLauncher.class);

  private final GobblinYarnAppLauncher gobblinYarnAppLauncher;

  public AzkabanGobblinYarnAppLauncher(String jobId, Properties props) throws IOException {
    super(jobId, LOGGER);
    this.gobblinYarnAppLauncher = new GobblinYarnAppLauncher(ConfigFactory.parseProperties(props),
        new YarnConfiguration());
  }

  @Override
  public void run() throws Exception {
    this.gobblinYarnAppLauncher.launch();

    Runtime.getRuntime().addShutdownHook(new Thread() {

      @Override
      public void run() {
        try {
          gobblinYarnAppLauncher.stop();
        } catch (IOException ioe) {
          LOGGER.error("Failed to shutdown the " + GobblinYarnAppLauncher.class.getSimpleName(), ioe);
        } catch (TimeoutException te) {
          LOGGER.error("Timed out in shutting down the " + GobblinYarnAppLauncher.class.getSimpleName(), te);
        }
      }

    });
  }

  @Override
  public void cancel() throws Exception {
    try {
      this.gobblinYarnAppLauncher.stop();
    } finally {
      super.cancel();
    }
  }
}
