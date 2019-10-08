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
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;

import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.yarn.GobblinYarnAppLauncher;

import azkaban.jobExecutor.AbstractJob;


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
  public static final String GOBBLIN_YARN_APP_LAUNCHER_CLASS = "gobblin.yarn.app.launcher.class";
  public static final String DEFAULT_GOBBLIN_YARN_APP_LAUNCHER_CLASS = "org.apache.gobblin.yarn.GobblinYarnAppLauncher";

  private static final Logger LOGGER = Logger.getLogger(AzkabanJobLauncher.class);

  private final GobblinYarnAppLauncher gobblinYarnAppLauncher;

  public AzkabanGobblinYarnAppLauncher(String jobId, Properties props) throws IOException {
    super(jobId, LOGGER);
    Config gobblinConfig = ConfigUtils.propertiesToConfig(props);

    outputConfigToFile(gobblinConfig);

    ClassAliasResolver<GobblinYarnAppLauncher> aliasResolver = new ClassAliasResolver<>(GobblinYarnAppLauncher.class);
    try {
      this.gobblinYarnAppLauncher = (GobblinYarnAppLauncher) ConstructorUtils.invokeConstructor(Class.forName(aliasResolver.resolve(
          props.getProperty(GOBBLIN_YARN_APP_LAUNCHER_CLASS, DEFAULT_GOBBLIN_YARN_APP_LAUNCHER_CLASS))), gobblinConfig, new YarnConfiguration());
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
        | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
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
