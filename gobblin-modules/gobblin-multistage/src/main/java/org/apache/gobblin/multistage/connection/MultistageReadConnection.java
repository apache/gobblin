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

import com.google.gson.JsonObject;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.multistage.exception.RetriableAuthenticationException;
import org.apache.gobblin.multistage.keys.ExtractorKeys;
import org.apache.gobblin.multistage.keys.JobKeys;
import org.apache.gobblin.multistage.util.VariableUtils;
import org.apache.gobblin.multistage.util.WorkUnitStatus;

@Slf4j
public class MultistageReadConnection implements Connection {
  @Getter @Setter private State state = null;
  @Getter @Setter private JobKeys jobKeys = null;
  @Getter @Setter private ExtractorKeys extractorKeys = null;

  public MultistageReadConnection(State state, JobKeys jobKeys, ExtractorKeys extractorKeys) {
    this.setJobKeys(jobKeys);
    this.setState(state);
    this.setExtractorKeys(extractorKeys);
  }

  /**
   * Default execute methods
   * @param status prior work unit status
   * @return new work unit status
   */
  @Override
  public WorkUnitStatus execute(final WorkUnitStatus status) throws RetriableAuthenticationException {
    return status.toBuilder().build();
  }

  /**
   * Close the connection and pool of connections if applicable, default
   * implementation does nothing.
   * @param message the message to send to the other end of connection upon closing
   * @return true (default)
   */
  @Override
  public boolean closeAll(final String message) {
    return true;
  }

  /**
   * Close the current cursor or stream if applicable, default
   * implementation do nothing.
   * @return true (default)
   */
  @Override
  public boolean closeStream() {
    return true;
  }

  public JsonObject getWorkUnitParameters() {
    return null;
  }

  /**
   * Default implementation of a multistage read connection
   * @param workUnitStatus prior work unit status
   * @return new work unit status
   */
  @SneakyThrows
  public WorkUnitStatus getFirst(final WorkUnitStatus workUnitStatus) throws RetriableAuthenticationException {
    return WorkUnitStatus.builder().build();
  }

  public WorkUnitStatus getNext(final WorkUnitStatus workUnitStatus) throws RetriableAuthenticationException {
    try {
      Thread.sleep(jobKeys.getCallInterval());
    } catch (Exception e) {
      log.warn(e.getMessage());
    }
    log.info("Starting a new request to the source, work unit = {}", extractorKeys.getSignature());
    log.debug("Prior parameters: {}", extractorKeys.getDynamicParameters().toString());
    log.debug("Prior work unit status: {}", workUnitStatus.toString());
    return workUnitStatus;
  }

  /**
   * This method applies the work unit parameters to string template, and
   * then return a work unit specific string
   *
   * @param template the template string
   * @param parameters the parameters with all variables substituted
   * @return work unit specific string
   */
  protected String getWorkUnitSpecificString(String template, JsonObject parameters) {
    String finalString = template;
    try {
      // substitute with parameters defined in ms.parameters and activation parameters
      finalString = VariableUtils.replaceWithTracking(
          finalString,
          parameters,
          false).getKey();
    } catch (Exception e) {
      log.error("Error getting work unit specific string " + e);
    }
    log.info("Final work unit specific string: {}", finalString);
    return finalString;
  }
}
