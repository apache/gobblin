/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor.extract.jdbc;

import gobblin.source.extractor.extract.CommandType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Joiner;
import gobblin.source.extractor.extract.Command;


/**
 * JDBC command with command type and parameters to execute a command
 *
 * @author nveeramr
 */
public class JdbcCommand implements Command {

  /**
   * JDBC command types
   */
  public enum JdbcCommandType implements CommandType {
    QUERY, QUERYPARAMS, FETCHSIZE, DELETE, UPDATE, SELECT
  }

  private List<String> params;
  private JdbcCommandType cmd;

  public JdbcCommand() {
    this.params = new ArrayList<String>();
  }

  @Override
  public List<String> getParams() {
    return this.params;
  }

  @Override
  public CommandType getCommandType() {
    return this.cmd;
  }

  @Override
  public String toString() {
    Joiner joiner = Joiner.on(":").skipNulls();
    return cmd.toString() + ":" + joiner.join(params);
  }

  @Override
  public Command build(Collection<String> params, CommandType cmd) {
    this.params.addAll(params);
    this.cmd = (JdbcCommandType) cmd;
    return this;
  }
}