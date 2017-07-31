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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
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

import org.apache.gobblin.cluster.GobblinTaskRunner;
import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.util.JvmUtils;
import org.apache.gobblin.util.logs.Log4jConfigurationHelper;
import org.apache.gobblin.yarn.event.DelegationTokenUpdatedEvent;


public class GobblinYarnTaskRunner extends GobblinTaskRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinTaskRunner.class);

  public GobblinYarnTaskRunner(String applicationName, String helixInstanceName, ContainerId containerId, Config config,
      Optional<Path> appWorkDirOptional) throws Exception {
    super(applicationName, helixInstanceName, getApplicationId(containerId), getTaskRunnerId(containerId), config,
        appWorkDirOptional);
  }

  @Override
  public List<Service> getServices() {
    List<Service> services = new ArrayList<>();
    if (this.config.hasPath(GobblinYarnConfigurationKeys.KEYTAB_FILE_PATH)) {
      LOGGER.info("Adding YarnContainerSecurityManager since login is keytab based");
      services.add(new YarnContainerSecurityManager(this.config, this.fs, this.eventBus));
    }
    return services;
  }

  @Override
  public MessageHandlerFactory getUserDefinedMessageHandlerFactory() {
    return new ParticipantUserDefinedMessageHandlerFactory();
  }

  /**
   * A custom {@link MessageHandlerFactory} for {@link ParticipantUserDefinedMessageHandler}s that
   * handle messages of type {@link org.apache.helix.model.Message.MessageType#USER_DEFINE_MSG}.
   */
  private class ParticipantUserDefinedMessageHandlerFactory implements MessageHandlerFactory {

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
     * A custom {@link MessageHandler} for handling user-defined messages to the participants.
     *
     * <p>
     *   Currently it handles the following sub types of messages:
     *
     *   <ul>
     *     <li>{@link org.apache.gobblin.cluster.HelixMessageSubTypes#TOKEN_FILE_UPDATED}</li>
     *   </ul>
     * </p>
     */
    private class ParticipantUserDefinedMessageHandler extends MessageHandler {

      public ParticipantUserDefinedMessageHandler(Message message, NotificationContext context) {
        super(message, context);
      }

      @Override
      public HelixTaskResult handleMessage() throws InterruptedException {
        String messageSubType = this._message.getMsgSubType();

        if (messageSubType.equalsIgnoreCase(org.apache.gobblin.cluster.HelixMessageSubTypes.TOKEN_FILE_UPDATED.toString())) {
          LOGGER.info("Handling message " + org.apache.gobblin.cluster.HelixMessageSubTypes.TOKEN_FILE_UPDATED.toString());

          eventBus.post(new DelegationTokenUpdatedEvent());
          HelixTaskResult helixTaskResult = new HelixTaskResult();
          helixTaskResult.setSuccess(true);
          return helixTaskResult;
        }

        throw new IllegalArgumentException(String
            .format("Unknown %s message subtype: %s", Message.MessageType.USER_DEFINE_MSG.toString(), messageSubType));
      }

      @Override
      public void onError(Exception e, ErrorCode code, ErrorType type) {
        LOGGER.error(
            String.format("Failed to handle message with exception %s, error code %s, error type %s", e, code, type));
      }
    }
  }

  private static String getApplicationId(ContainerId containerId) {
    return containerId.getApplicationAttemptId().getApplicationId().toString();
  }

  private static String getTaskRunnerId(ContainerId containerId) {
    return containerId.toString();
  }

  public static void main(String[] args) throws Exception {
    Options options = buildOptions();
    try {
      CommandLine cmd = new DefaultParser().parse(options, args);
      if (!cmd.hasOption(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME) || !cmd
          .hasOption(GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_OPTION_NAME)) {
        printUsage(options);
        System.exit(1);
      }

      Log4jConfigurationHelper.updateLog4jConfiguration(GobblinTaskRunner.class,
          GobblinYarnConfigurationKeys.GOBBLIN_YARN_LOG4J_CONFIGURATION_FILE,
          GobblinYarnConfigurationKeys.GOBBLIN_YARN_LOG4J_CONFIGURATION_FILE);

      LOGGER.info(JvmUtils.getJvmInputArguments());

      ContainerId containerId =
          ConverterUtils.toContainerId(System.getenv().get(ApplicationConstants.Environment.CONTAINER_ID.key()));
      String applicationName = cmd.getOptionValue(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME);
      String helixInstanceName = cmd.getOptionValue(GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_OPTION_NAME);

      GobblinTaskRunner gobblinTaskRunner =
          new GobblinYarnTaskRunner(applicationName, helixInstanceName, containerId, ConfigFactory.load(),
              Optional.<Path>absent());
      gobblinTaskRunner.start();
    } catch (ParseException pe) {
      printUsage(options);
      System.exit(1);
    }
  }
}
