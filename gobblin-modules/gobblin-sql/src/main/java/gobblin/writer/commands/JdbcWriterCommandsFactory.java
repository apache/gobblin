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

package gobblin.writer.commands;

import java.sql.Connection;

import com.google.common.base.Preconditions;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.ForkOperatorUtils;
import gobblin.writer.Destination;
import gobblin.writer.Destination.DestinationType;


/**
 * Factory method pattern class. It's not a static class mainly for TDD -- so that it can be mocked for testing purpose.
 */
public class JdbcWriterCommandsFactory {

  /**
   * @param destination
   * @return Provides JdbcWriterCommands bases on destination.
   */
  public JdbcWriterCommands newInstance(Destination destination, Connection conn) {
    switch (destination.getType()) {
      case MYSQL:
        return new MySqlWriterCommands(destination.getProperties(), conn);
      case TERADATA:
        return new TeradataWriterCommands(destination.getProperties(), conn);
      default:
        throw new IllegalArgumentException(destination.getType() + " is not supported");
    }
  }

  /**
   * @param state
   * @return Provides JdbcWriterCommands based on ConfigurationKeys.WRITER_DESTINATION_TYPE_KEY
   */
  public JdbcWriterCommands newInstance(State state, Connection conn) {
    String destKey = ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_DESTINATION_TYPE_KEY,
        state.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1),
        state.getPropAsInt(ConfigurationKeys.FORK_BRANCH_ID_KEY, 0));
    String destType = state.getProp(destKey);
    Preconditions.checkNotNull(destType, destKey + " is required for underlying JDBC product name");
    return newInstance(Destination.of(DestinationType.valueOf(destType.toUpperCase()), state), conn);
  }
}
