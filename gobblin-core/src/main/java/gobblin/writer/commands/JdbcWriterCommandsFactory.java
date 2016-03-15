package gobblin.writer.commands;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.writer.Destination.DestinationType;

import java.util.Objects;

public class JdbcWriterCommandsFactory {

  public static JdbcWriterCommands newInstance(DestinationType destinationType) {
    switch (destinationType) {
      case MYSQL:
        return new MySqlWriterCommands();
      default:
        throw new IllegalArgumentException(destinationType + " is not supported");
    }
  }

  public static JdbcWriterCommands newInstance(State state) {
    String destType = state.getProp(ConfigurationKeys.WRITER_DESTINATION_TYPE_KEY);
    Objects.requireNonNull(destType, ConfigurationKeys.WRITER_DESTINATION_TYPE_KEY + " is required for underlying JDBC product name");
    return newInstance(DestinationType.valueOf(destType.toUpperCase()));
  }
}