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

import java.util.Collections;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.Path;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.GobblinClusterManager;
import org.apache.gobblin.cluster.GobblinHelixJobScheduler;
import org.apache.gobblin.cluster.HelixMessageSubTypes;
import org.apache.gobblin.cluster.JobConfigurationManager;
import org.apache.gobblin.util.JvmUtils;


/**
 * The AWS Cluster master class for Gobblin.
 *
 * <p>
 *   This class makes use of super class {@link GobblinClusterManager} to run:
 *   1. {@link GobblinHelixJobScheduler} for scheduling and running Gobblin jobs.
 *   2. {@link HelixManager} to work with Helix and act as Helix controller.
 *   3. {@link JobConfigurationManager} to discover new job configurations and updates to
 *   existing job configurations.
 *
 *   More AWS specific services can be added in future to this class that are required to be
 *   run on Gobblin cluster master.
 * </p>
 *
 * <p>
 *   Note: Shutdown initiated by {@link GobblinAWSClusterLauncher} via a Helix message of subtype
 *   {@link HelixMessageSubTypes#APPLICATION_MASTER_SHUTDOWN} is handled by super class {@link GobblinClusterManager}
 * </p>
 *
 * @author Abhishek Tiwari
 */
@Alpha
public class GobblinAWSClusterManager extends GobblinClusterManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinAWSClusterManager.class);

  public GobblinAWSClusterManager(String clusterName, String applicationId, Config config,
      Optional<Path> appWorkDirOptional)
      throws Exception {
    super(clusterName, applicationId, config, appWorkDirOptional);

    // Note: JobConfigurationManager and HelixJobScheduler are initialized in {@link GobblinClusterManager}
  }

  /**
   * A custom {@link MessageHandlerFactory} for {@link ControllerUserDefinedMessageHandler}s that
   * handle messages of type {@link org.apache.helix.model.Message.MessageType#USER_DEFINE_MSG}.
   */
  private static class ControllerUserDefinedMessageHandlerFactory implements MessageHandlerFactory {

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
     *   Currently does not handle any user-defined messages. If this class is passed a custom message, it will simply
     *   print out a warning and return successfully.
     * </p>
     */
    private static class ControllerUserDefinedMessageHandler extends MessageHandler {

      public ControllerUserDefinedMessageHandler(Message message, NotificationContext context) {
        super(message, context);
      }

      @Override
      public HelixTaskResult handleMessage() throws InterruptedException {
        LOGGER.warn(String
            .format("No handling setup for %s message of subtype: %s", Message.MessageType.USER_DEFINE_MSG.toString(),
                this._message.getMsgSubType()));

        final HelixTaskResult helixTaskResult = new HelixTaskResult();
        helixTaskResult.setSuccess(true);
        return helixTaskResult;
      }

      @Override
      public void onError(Exception e, ErrorCode code, ErrorType type) {
        LOGGER.error(
            String.format("Failed to handle message with exception %s, error code %s, error type %s", e, code, type));
      }
    }
  }

  private static Options buildOptions() {
    final Options options = new Options();
    options.addOption("a", GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME, true, "AWS application name");
    options.addOption("d", GobblinAWSConfigurationKeys.APP_WORK_DIR, true, "Application work directory");

    return options;
  }

  private static void printUsage(Options options) {
    final HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(GobblinAWSClusterManager.class.getSimpleName(), options);
  }

  public static void main(String[] args) throws Exception {
    final Options options = buildOptions();
    try {
      final CommandLine cmd = new DefaultParser().parse(options, args);
      if (!cmd.hasOption(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME) ||
          !cmd.hasOption(GobblinAWSConfigurationKeys.APP_WORK_DIR)) {
        printUsage(options);
        System.exit(1);
      }

      if (System.getProperty("log4j.configuration") == null) {
        Log4jConfigHelper.updateLog4jConfiguration(GobblinAWSClusterManager.class,
                GobblinAWSConfigurationKeys.GOBBLIN_AWS_LOG4J_CONFIGURATION_FILE);
      }

      LOGGER.info(JvmUtils.getJvmInputArguments());

      // Note: Application id is required param for {@link GobblinClusterManager} super class
      // .. but has not meaning in AWS cluster context, so defaulting to a fixed value
      final String applicationId = "1";
      final String appWorkDir = cmd.getOptionValue(GobblinAWSConfigurationKeys.APP_WORK_DIR);

      try (GobblinAWSClusterManager clusterMaster = new GobblinAWSClusterManager(
          cmd.getOptionValue(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME), applicationId,
          ConfigFactory.load(), Optional.of(new Path(appWorkDir)))) {

        clusterMaster.start();
      }
    } catch (ParseException pe) {
      printUsage(options);
      System.exit(1);
    }
  }
}
