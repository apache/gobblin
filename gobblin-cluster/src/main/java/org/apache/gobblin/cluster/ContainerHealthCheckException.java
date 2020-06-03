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
package org.apache.gobblin.cluster;

/**
 * Signals that the container has failed one or more health checks. In other words, the container has been detected
 * itself to be in an unhealthy state. The application may want to catch this exception to take an appropriate
 * action e.g. exiting with an appropriate exit code.
 */
public class ContainerHealthCheckException extends RuntimeException {
  public ContainerHealthCheckException() {
    super();
  }

  public ContainerHealthCheckException(String message) {
    super(message);
  }

  public ContainerHealthCheckException(String message, Throwable cause) {
    super(message, cause);
  }
}
