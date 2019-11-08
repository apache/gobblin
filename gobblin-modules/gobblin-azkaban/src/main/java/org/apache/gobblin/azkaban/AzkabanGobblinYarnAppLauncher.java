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

package org.apache.gobblin.azkaban;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;

import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.yarn.GobblinYarnAppLauncher;

import azkaban.jobExecutor.AbstractJob;
import lombok.Getter;


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
public class AzkabanGobblinYarnAppLauncher extends AbstractJob {
  // if this is set then the Azkaban config will be written to the specified file path
  public static final String AZKABAN_CONFIG_OUTPUT_PATH = "gobblin.yarn.akabanConfigOutputPath";

  private static final Logger LOGGER = Logger.getLogger(AzkabanJobLauncher.class);

  private final GobblinYarnAppLauncher gobblinYarnAppLauncher;

  @Getter
  private final YarnConfiguration yarnConfiguration;

  public AzkabanGobblinYarnAppLauncher(String jobId, Properties gobblinProps) throws IOException {
    super(jobId, LOGGER);
    Config gobblinConfig = ConfigUtils.propertiesToConfig(gobblinProps);

    outputConfigToFile(gobblinConfig);

    yarnConfiguration = initYarnConf(gobblinProps);

    this.gobblinYarnAppLauncher = new GobblinYarnAppLauncher(gobblinConfig, this.yarnConfiguration);
  }

  /**
   * Extended class can override this method by providing their own Yarn configuration.
   */
  protected YarnConfiguration initYarnConf(Properties gobblinProps) {
    YarnConfiguration yarnConfiguration = new YarnConfiguration();

    if (gobblinProps.containsKey("yarn-site-address")) {
      yarnConfiguration.addResource(new Path(gobblinProps.getProperty("yarn-site-address")));
    } else {
      yarnConfiguration.set("yarn.resourcemanager.connect.max-wait.ms", "10000");
      yarnConfiguration.set("yarn.nodemanager.resource.memory-mb", "1024");
      yarnConfiguration.set("yarn.scheduler.maximum-allocation-mb", "2048");
    }
    return yarnConfiguration;
  }

  @Override
  public void run() throws Exception {
    this.gobblinYarnAppLauncher.launch();

    Runtime.getRuntime().addShutdownHook(new Thread() {

      @Override
      public void run() {
        try {
          AzkabanGobblinYarnAppLauncher.this.gobblinYarnAppLauncher.stop();
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

  /**
   * Write the config to the file specified with the config key {@value AZKABAN_CONFIG_OUTPUT_PATH} if it
   * is configured.
   * @param config the config to output
   * @throws IOException
   */
  @VisibleForTesting
  static void outputConfigToFile(Config config) throws IOException {
    // If a file path is specified then write the Azkaban config to that path in HOCON format.
    // This can be used to generate an application.conf file to pass to the yarn app master and containers.
    if (config.hasPath(AZKABAN_CONFIG_OUTPUT_PATH)) {
      File configFile = new File(config.getString(AZKABAN_CONFIG_OUTPUT_PATH));
      File parentDir = configFile.getParentFile();

      if (parentDir != null && !parentDir.exists()) {
        if (!parentDir.mkdirs()) {
          throw new IOException("Error creating directories for " + parentDir);
        }
      }

      ConfigRenderOptions configRenderOptions = ConfigRenderOptions.defaults();
      configRenderOptions = configRenderOptions.setComments(false);
      configRenderOptions = configRenderOptions.setOriginComments(false);
      configRenderOptions = configRenderOptions.setFormatted(true);
      configRenderOptions = configRenderOptions.setJson(false);

      String renderedConfig = config.root().render(configRenderOptions);

      FileUtils.writeStringToFile(configFile, renderedConfig, Charsets.UTF_8);
    }
  }
}
