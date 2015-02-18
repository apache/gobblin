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

package gobblin.source.extractor.extract;

import java.util.Collection;
import java.util.List;


/**
 * Interface for a source command (e.g. REST, SFTP, etc.)
 * Specifies a Command along with a CommandType and a list of parameters
 * @author stakiar
 */
public interface Command {
  public List<String> getParams();

  public CommandType getCommandType();

  public Command build(Collection<String> params, CommandType cmd);
}