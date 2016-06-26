package gobblin.aws;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.helix.NotificationContext;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.annotation.Alpha;
import gobblin.cluster.GobblinClusterConfigurationKeys;
import gobblin.cluster.GobblinClusterManager;
import gobblin.util.logs.Log4jConfigurationHelper;

/**
 * The AWS Cluster Master class for Gobblin.
 *
 * @author Abhishek Tiwari
 */
@Alpha
public class GobblinAWSClusterMaster extends GobblinClusterManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinAWSClusterMaster.class);

  public GobblinAWSClusterMaster(String clusterName, String applicationId, Config config)
      throws Exception {
    super(clusterName, applicationId, config);

    // Note: JobConfigurationManager and HelixJobScheduler are initializedin GobblinClusterManager
    // TODO: Add log handler
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
    private class ControllerUserDefinedMessageHandler extends MessageHandler {

      public ControllerUserDefinedMessageHandler(Message message, NotificationContext context) {
        super(message, context);
      }

      @Override
      public HelixTaskResult handleMessage() throws InterruptedException {
        LOGGER.warn(String
            .format("No handling setup for %s message of subtype: %s", Message.MessageType.USER_DEFINE_MSG.toString(),
                this._message.getMsgSubType()));

        HelixTaskResult helixTaskResult = new HelixTaskResult();
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
    Options options = new Options();
    options.addOption("a", GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME, true, "AWS application name");
    return options;
  }

  private static void printUsage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(GobblinAWSClusterMaster.class.getSimpleName(), options);
  }

  public static void main(String[] args) throws Exception {
    Options options = buildOptions();
    try {
      CommandLine cmd = new DefaultParser().parse(options, args);
      if (!cmd.hasOption(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME)) {
        printUsage(options);
        System.exit(1);
      }

      Log4jConfigurationHelper.updateLog4jConfiguration(GobblinAWSClusterMaster.class,
          GobblinAWSConfigurationKeys.GOBBLIN_AWS_LOG4J_CONFIGURATION_FILE,
          GobblinAWSConfigurationKeys.GOBBLIN_AWS_LOG4J_CONFIGURATION_FILE);

      // TODO: If required, change logic to fetch application id
      String applicationId = "1";

      try (GobblinAWSClusterMaster clusterMaster = new GobblinAWSClusterMaster(
          cmd.getOptionValue(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME), applicationId,
          ConfigFactory.load())) {

        clusterMaster.start();
      }
    } catch (ParseException pe) {
      printUsage(options);
      System.exit(1);
    }
  }
}
