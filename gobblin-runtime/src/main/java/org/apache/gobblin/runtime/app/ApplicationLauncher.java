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

package org.apache.gobblin.runtime.app;

import java.io.Closeable;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.JobLauncher;


/**
 * Launches a Gobblin application. An application may launch one or multiple jobs via a {@link JobLauncher}. An
 * {@link ApplicationLauncher} is only responsible for starting and stopping one application. Typically, only one
 * application per JVM will be launched.
 *
 * <p>
 *   An application can be started via the {@link #start()} method and then stopped via the {@link #stop()} method.
 * </p>
 */
@Alpha
public interface ApplicationLauncher extends Closeable {

  /**
   * Start a Gobblin application.
   *
   * @throws ApplicationException if there is any problem starting the application
   */
  public void start() throws ApplicationException;

  /**
   * Stop the Gobblin application.
   *
   * @throws ApplicationException if there is any problem starting the application
   */
  public void stop() throws ApplicationException;
}
