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

package org.apache.gobblin.yarn;

import java.util.Collections;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

import org.apache.helix.NotificationContext;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.Service;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import lombok.Getter;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.GobblinClusterManager;
import org.apache.gobblin.cluster.GobblinClusterUtils;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.JvmUtils;
import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.logs.Log4jConfigurationHelper;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;
import org.apache.gobblin.yarn.event.DelegationTokenUpdatedEvent;


/**
 * The Yarn ApplicationMaster class for Gobblin.
 *
 * <p>
 *   This class runs the {@link YarnService} for all Yarn-related stuffs like ApplicationMaster registration
 *   and un-registration and Yarn container provisioning.
 * </p>
 *
 * @author Yinan Li
 */
@Alpha
public class GobblinApplicationMaster extends GobblinClusterManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinApplicationMaster.class);

  @Getter
  private final YarnService yarnService;

  public GobblinApplicationMaster(String applicationName, ContainerId containerId, Config config,
      YarnConfiguration yarnConfiguration) throws Exception {
    super(applicationName, containerId.getApplicationAttemptId().getApplicationId().toString(),
        GobblinClusterUtils.addDynamicConfig(config.withValue(GobblinYarnConfigurationKeys.CONTAINER_NUM_KEY,
            ConfigValueFactory.fromAnyRef(YarnHelixUtils.getContainerNum(containerId.toString())))),
        Optional.<Path>absent());

    String containerLogDir = config.getString(GobblinYarnConfigurationKeys.LOGS_SINK_ROOT_DIR_KEY);
    GobblinYarnLogSource gobblinYarnLogSource = new GobblinYarnLogSource();
    if (gobblinYarnLogSource.isLogSourcePresent()) {
      Path appWorkDir = PathUtils.combinePaths(containerLogDir, GobblinClusterUtils.getAppWorkDirPath(this.clusterName, this.applicationId), "AppMaster");
      this.applicationLauncher
          .addService(gobblinYarnLogSource.buildLogCopier(this.config, containerId.toString(), this.fs, appWorkDir));
    }

    this.yarnService = buildYarnService(this.config, applicationName, this.applicationId, yarnConfiguration, this.fs);
    this.applicationLauncher.addService(this.yarnService);

    if (UserGroupInformation.isSecurityEnabled()) {
      LOGGER.info("Adding YarnContainerSecurityManager since security is enabled");
      this.applicationLauncher.addService(buildYarnContainerSecurityManager(this.config, this.fs));
    }

    // Add additional services
    List<String> serviceClassNames = ConfigUtils.getStringList(this.config,
        GobblinYarnConfigurationKeys.APP_MASTER_SERVICE_CLASSES);

    for (String serviceClassName : serviceClassNames) {
      Class<?> serviceClass = Class.forName(serviceClassName);
      this.applicationLauncher.addService((Service) GobblinConstructorUtils.invokeLongestConstructor(serviceClass, this));
    }
  }

  /**
   * Build the {@link YarnService} for the Application Master.
   */
  protected YarnService buildYarnService(Config config, String applicationName, String applicationId,
      YarnConfiguration yarnConfiguration, FileSystem fs)
      throws Exception {
    return new YarnService(config, applicationName, applicationId, yarnConfiguration, fs, this.eventBus);
  }

  /**
   * Build the {@link YarnContainerSecurityManager} for the Application Master.
   */
  private YarnContainerSecurityManager buildYarnContainerSecurityManager(Config config, FileSystem fs) {
    return new YarnContainerSecurityManager(config, fs, this.eventBus);
  }

  @Override
  protected MessageHandlerFactory getUserDefinedMessageHandlerFactory() {
    return new ControllerUserDefinedMessageHandlerFactory();
  }

  /**
   * A custom {@link MessageHandlerFactory} for {@link ControllerUserDefinedMessageHandler}s that
   * handle messages of type {@link org.apache.helix.model.Message.MessageType#USER_DEFINE_MSG}.
   */
  private class ControllerUserDefinedMessageHandlerFactory implements MessageHandlerFactory {

    @Override
    public MessageHandler createHandler(Message message, NotificationContext context) {
      return new ControllerUserDefinedMessageHandler(message, context);
    }

    @Override
    public String getMessageType() {
      return Message.MessageType.USER_DEFINE_MSG.toString();
    }

    public List<String> getMessageTypes() {
      return Collections.singletonList(getMessageType());
    }

    @Override
    public void reset() {

    }

    /**
     * A custom {@link MessageHandler} for handling user-defined messages to the controller.
     *
     * <p>
     *   Currently it handles the following sub types of messages:
     *
     *   <ul>
     *     <li>{@link HelixMessageSubTypes#TOKEN_FILE_UPDATED}</li>
     *   </ul>
     * </p>
     */
    private class ControllerUserDefinedMessageHandler extends MessageHandler {

      public ControllerUserDefinedMessageHandler(Message message, NotificationContext context) {
        super(message, context);
      }

      @Override
      public HelixTaskResult handleMessage() throws InterruptedException {
        String messageSubType = this._message.getMsgSubType();

        if (messageSubType.equalsIgnoreCase(HelixMessageSubTypes.TOKEN_FILE_UPDATED.toString())) {
          LOGGER.info("Handling message " + HelixMessageSubTypes.TOKEN_FILE_UPDATED.toString());

          eventBus.post(new DelegationTokenUpdatedEvent());
          HelixTaskResult helixTaskResult = new HelixTaskResult();
          helixTaskResult.setSuccess(true);
          return helixTaskResult;
        }

        throw new IllegalArgumentException(String.format("Unknown %s message subtype: %s",
            Message.MessageType.USER_DEFINE_MSG.toString(), messageSubType));
      }

      @Override
      public void onError(Exception e, ErrorCode code, ErrorType type) {
        LOGGER.error(
            String.format("Failed to handle message with exception %s, error code %s, error type %s", e, code, type));
      }
    }
  }

  private static Options buildOptions() {
    Options options = new Options();
    options.addOption("a", GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME, true, "Yarn application name");
    return options;
  }

  private static void printUsage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(GobblinApplicationMaster.class.getSimpleName(), options);
  }

  public static void main(String[] args) throws Exception {
    Options options = buildOptions();
    try {
      CommandLine cmd = new DefaultParser().parse(options, args);
      if (!cmd.hasOption(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME)) {
        printUsage(options);
        System.exit(1);
      }

      Log4jConfigurationHelper.updateLog4jConfiguration(GobblinApplicationMaster.class,
          GobblinYarnConfigurationKeys.GOBBLIN_YARN_LOG4J_CONFIGURATION_FILE,
          GobblinYarnConfigurationKeys.GOBBLIN_YARN_LOG4J_CONFIGURATION_FILE);

      LOGGER.info(JvmUtils.getJvmInputArguments());

      ContainerId containerId =
          ConverterUtils.toContainerId(System.getenv().get(ApplicationConstants.Environment.CONTAINER_ID.key()));

      try (GobblinApplicationMaster applicationMaster = new GobblinApplicationMaster(
          cmd.getOptionValue(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME), containerId,
          ConfigFactory.load(), new YarnConfiguration())) {

        applicationMaster.start();
      }
    } catch (ParseException pe) {
      printUsage(options);
      System.exit(1);
    }
  }
}
