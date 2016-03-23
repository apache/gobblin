/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.writer.commands;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.writer.Destination;
import gobblin.writer.Destination.DestinationType;

import java.util.Objects;

/**
 * Factory method pattern class. It's not a static class mainly for TDD -- so that it can be mocked for testing purpose.
 */
public class JdbcWriterCommandsFactory {

  /**
   * @param destination
   * @return Provides JdbcWriterCommands bases on destination.
   */
  public JdbcWriterCommands newInstance(Destination destination) {
    switch (destination.getType()) {
      case MYSQL:
        return new MySqlWriterCommands(destination.getProperties());
      default:
        throw new IllegalArgumentException(destination.getType() + " is not supported");
    }
  }

  /**
   * @param state
   * @return Provides JdbcWriterCommands based on ConfigurationKeys.WRITER_DESTINATION_TYPE_KEY
   */
  public JdbcWriterCommands newInstance(State state) {
    String destType = state.getProp(ConfigurationKeys.WRITER_DESTINATION_TYPE_KEY);
    Objects.requireNonNull(destType, ConfigurationKeys.WRITER_DESTINATION_TYPE_KEY + " is required for underlying JDBC product name");
    return newInstance(Destination.of(DestinationType.valueOf(destType.toUpperCase()), state));
  }
}