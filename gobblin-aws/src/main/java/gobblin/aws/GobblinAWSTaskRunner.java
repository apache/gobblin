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

package gobblin.aws;

import java.util.Collections;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
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

import com.amazonaws.util.EC2MetadataUtils;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.Service;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.annotation.Alpha;
import gobblin.cluster.GobblinClusterConfigurationKeys;
import gobblin.cluster.GobblinClusterManager;
import gobblin.cluster.GobblinHelixTask;
import gobblin.cluster.GobblinHelixTaskFactory;
import gobblin.cluster.GobblinTaskRunner;
import gobblin.cluster.HelixMessageSubTypes;


/**
 * Class running on worker nodes managing services for executing Gobblin
 * {@link gobblin.source.workunit.WorkUnit}s.
 *
 * <p>
 *   This class makes use of super class {@link GobblinTaskRunner} to run:
 *   1. {@link GobblinHelixTaskFactory} for creating {@link GobblinHelixTask}s that Helix manages
 *      to run Gobblin data ingestion tasks.
 *   2. {@link HelixManager} to work with Helix and act as Helix participant to execute tasks.
 *
 *   More AWS specific services can be added in future to this class that are required to be
 *   run on Gobblin cluster worker.
 * </p>
 *
 * <p>
 *   Note: Shutdown initiated by {@link GobblinClusterManager} via a Helix message of subtype
 *   {@link HelixMessageSubTypes#WORK_UNIT_RUNNER_SHUTDOWN} is handled by super class {@link GobblinTaskRunner}
 * </p>
 *
 * @author Abhishek Tiwari
 */
@Alpha
public class GobblinAWSTaskRunner extends GobblinTaskRunner {
  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinTaskRunner.class);

  public GobblinAWSTaskRunner(String applicationName, String helixInstanceName, Config config,
      Optional<Path> appWorkDirOptional)
      throws Exception {
    super(applicationName, helixInstanceName, getApplicationId(), getTaskRunnerId(), config,
        appWorkDirOptional);
  }

  @Override
  public List<Service> getServices() {
    return Collections.emptyList();
  }

  @Override
  public MessageHandlerFactory getUserDefinedMessageHandlerFactory() {
    return new ParticipantUserDefinedMessageHandlerFactory();
  }

  /**
   * A custom {@link MessageHandlerFactory} for {@link ParticipantUserDefinedMessageHandler}s that
   * handle messages of type {@link org.apache.helix.model.Message.MessageType#USER_DEFINE_MSG}.
   */
  private static class ParticipantUserDefinedMessageHandlerFactory implements MessageHandlerFactory {

    @Override
    public MessageHandler createHandler(Message message, NotificationContext context) {
      return new ParticipantUserDefinedMessageHandler(message, context);
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
    private static class ParticipantUserDefinedMessageHandler extends MessageHandler {

      public ParticipantUserDefinedMessageHandler(Message message, NotificationContext context) {
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

  private static String getApplicationId() {
    return "1";
  }

  private static String getTaskRunnerId() {
    return EC2MetadataUtils.getNetworkInterfaces().get(0).getPublicIPv4s().get(0);
  }

  public static Options buildOptions() {
    final Options options = new Options();
    options.addOption("a", GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME, true, "Application name");
    options.addOption("i", GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_OPTION_NAME, true, "Helix instance name");
    options.addOption("d", GobblinAWSConfigurationKeys.APP_WORK_DIR, true, "Application work directory");
    return options;
  }

  public static void main(String[] args) throws Exception {
    final Options options = buildOptions();

    try {
      final CommandLine cmd = new DefaultParser().parse(options, args);
      if (!cmd.hasOption(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME) ||
          !cmd.hasOption(GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_OPTION_NAME) ||
          !cmd.hasOption(GobblinAWSConfigurationKeys.APP_WORK_DIR)) {
        printUsage(options);
        System.exit(1);
      }

      Log4jConfigHelper.updateLog4jConfiguration(GobblinTaskRunner.class,
          GobblinAWSConfigurationKeys.GOBBLIN_AWS_LOG4J_CONFIGURATION_FILE);

      final String applicationName = cmd.getOptionValue(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME);
      final String helixInstanceName = cmd.getOptionValue(GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_OPTION_NAME);
      final String appWorkDir = cmd.getOptionValue(GobblinAWSConfigurationKeys.APP_WORK_DIR);

      final GobblinTaskRunner gobblinTaskRunner =
          new GobblinAWSTaskRunner(applicationName, helixInstanceName, ConfigFactory.load(),
              Optional.of(new Path(appWorkDir)));
      gobblinTaskRunner.start();
    } catch (ParseException pe) {
      printUsage(options);
      System.exit(1);
    }
  }
}
