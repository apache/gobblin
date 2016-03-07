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

package gobblin.runtime.app;

import java.io.Closeable;

import gobblin.runtime.JobLauncher;


/**
 * Launches a Gobblin application. An application may launch one or multiple jobs via a {@link JobLauncher}. An
 * {@link ApplicationLauncher} is only responsible for starting and stopping one application. Typically, only one
 * application per JVM will be launched.
 *
 * <p>
 *   An application can be started via the {@link #start()} method and then stopped via the {@link #stop()} method.
 * </p>
 */
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
