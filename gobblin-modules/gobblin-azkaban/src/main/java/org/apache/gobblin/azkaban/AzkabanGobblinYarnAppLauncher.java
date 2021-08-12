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

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import azkaban.jobExecutor.AbstractJob;
import lombok.Getter;

import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.yarn.GobblinYarnAppLauncher;
import org.apache.gobblin.yarn.GobblinYarnConfigurationKeys;


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
  private static final Logger LOGGER = Logger.getLogger(AzkabanGobblinYarnAppLauncher.class);

  private final GobblinYarnAppLauncher gobblinYarnAppLauncher;

  @Getter
  private final YarnConfiguration yarnConfiguration;

  public AzkabanGobblinYarnAppLauncher(String jobId, Properties gobblinProps)
      throws IOException {
    super(jobId, LOGGER);

    addRuntimeProperties(gobblinProps);

    Config gobblinConfig = ConfigUtils.propertiesToConfig(gobblinProps);

    //Suppress logs from classes that emit Yarn application Id that Azkaban uses to kill the application.
    setLogLevelForClasses(gobblinConfig);

    yarnConfiguration = initYarnConf(gobblinProps);

    gobblinConfig = gobblinConfig.withValue(GobblinYarnAppLauncher.GOBBLIN_YARN_APP_LAUNCHER_MODE,
        ConfigValueFactory.fromAnyRef(GobblinYarnAppLauncher.AZKABAN_APP_LAUNCHER_MODE_KEY));
    this.gobblinYarnAppLauncher = new GobblinYarnAppLauncher(gobblinConfig, this.yarnConfiguration);
  }

  /**
   * Set Log Level for each class specified in the config. Class name and the corresponding log level can be specified
   * as "a:INFO,b:ERROR", where logs of class "a" are set to INFO and logs from class "b" are set to ERROR.
   * @param config
   */
  private void setLogLevelForClasses(Config config) {
    List<String> classLogLevels = ConfigUtils.getStringList(config, GobblinYarnConfigurationKeys.GOBBLIN_YARN_AZKABAN_CLASS_LOG_LEVELS);

    for (String classLogLevel: classLogLevels) {
      String className = classLogLevel.split(":")[0];
      Level level = Level.toLevel(classLogLevel.split(":")[1], Level.INFO);
      Logger.getLogger(className).setLevel(level);
    }
  }

  /**
   * Extended class can override this method by providing their own YARN configuration.
   */
  protected YarnConfiguration initYarnConf(Properties gobblinProps) {
    return new YarnConfiguration();
  }

  /**
   * Extended class can override this method to add some runtime properties.
   */
  protected void addRuntimeProperties(Properties gobblinProps) {
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
}
