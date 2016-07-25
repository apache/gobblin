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
package gobblin.runtime.api;

import java.util.concurrent.TimeoutException;

import gobblin.annotation.Alpha;

/** Defines a standard set of methods for running Gobblin components. */
@Alpha
public interface StandardRunControls {

  /**
   * Starts the component. This is a non-blocking call. Use {@link #waitForRunning(long)} to
   * determine when the component has fully started.
   * */
  void start();


  /**
   * Checks if the component is fully initialized and ready to running.
   */
  boolean isRunning();

  /**
   * Blocks until the component is running.
   * @param timeoutMs             the number of milliseconds to wait; if <= 0, the wait is indefinite.
   * @throws TimeoutException     the component did not get to running state within the time allotted
   * @throws InterruptedException the waiting was interrupted
   */
  void waitForRunning(long timeoutMs) throws TimeoutException, InterruptedException;

  /**
   * Trigger a graceful shutdown. This will stop launching and new jobs/tasks/etc. and wait for any
   * subcomponents/jobs/task to finish gracefully.
   *
   * <p>This is a non-blocking call. Use {@link #waitForShutdown(long)} to determine when the
   * component is fully shutdown.
   */
  void shutdownGracefully();

  /**
   * Trigger an immediate shutdown. This will stop accepting/launching new jobs/tasks/etc. and also
   * attempt to kill any actively running jobs/tasks.
   *
   * <p>This is a non-blocking call. Use {@link #waitForShutdown(long)} to determine when Gobblin is
   * fully shutdown.
   */
  void shutdownForcefully();

  /**
   * Checks if the component is shutdown.
   */
  boolean isShutdown();

  /**
   * Blocks until the component is shutdown.
   * @param timeoutMs             the number of milliseconds to wait; if <= 0, the wait is indefinite.
   * @throws TimeoutException     the component did not shutdown within the time allotted
   * @throws InterruptedException the waiting was interrupted
   */
  void waitForShutdown(long timeoutMs)
      throws TimeoutException, InterruptedException;

}
