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

package org.apache.gobblin.multistage.connection;

import org.apache.gobblin.multistage.exception.RetriableAuthenticationException;
import org.apache.gobblin.multistage.util.WorkUnitStatus;


public interface Connection {
  /**
   * The common method among all connections, read or write, is the execute(). This
   * method expects a work unit status object as input parameter, and it gives out
   * a new work unit object as output.
   * @param status the input WorkUnitStatus object
   * @return the output of the execution in a WorkUnitStatus object
   * @throws RetriableAuthenticationException exception to allow retry at higher level
   */
  WorkUnitStatus execute(final WorkUnitStatus status) throws RetriableAuthenticationException;
  /**
   * Close the connection and pool of connections if applicable
   * @param message the message to send to the other end of connection upon closing
   * @return true if connections are successfully closed, or false if connections are not
   * closed successfully
   */
  boolean closeAll(final String message);
  /**
   * Close the current cursor or stream if applicable
   * @return true if closeStream was successful, or false if not able to close the stream
   */
  boolean closeStream();
}
