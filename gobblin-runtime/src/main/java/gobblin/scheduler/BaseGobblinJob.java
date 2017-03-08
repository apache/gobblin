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
package gobblin.scheduler;

import org.quartz.*;
import org.slf4j.MDC;

import java.util.Map;


public abstract class BaseGobblinJob implements Job {
  private final Map<String, String> mdcContext;

  public BaseGobblinJob() {
    this.mdcContext = MDC.getCopyOfContextMap();
  }

  /**
   * <p>
   * Called by the <code>{@link Scheduler}</code> when a <code>{@link Trigger}</code>
   * fires that is associated with the <code>Job</code>.
   * </p>
   *
   * <p>
   * The implementation may wish to set a
   * {@link JobExecutionContext#setResult(Object) result} object on the
   * {@link JobExecutionContext} before this method exits.  The result itself
   * is meaningless to Quartz, but may be informative to
   * <code>{@link JobListener}s</code> or
   * <code>{@link TriggerListener}s</code> that are watching the job's
   * execution.
   * </p>
   *
   * @throws JobExecutionException if there is an exception while executing the job.
   */
  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    Map<String, String> originalContext = MDC.getCopyOfContextMap();
    if (this.mdcContext != null) {
      MDC.setContextMap(this.mdcContext);
    }

    try {
      executeImpl(context);
    } finally {
      if (originalContext != null) {
        MDC.setContextMap(originalContext);
      } else {
        MDC.clear();
      }
    }
  }

  /**
   * <p>
   * Called by the <code>{@link Scheduler}</code> when a <code>{@link Trigger}</code>
   * fires that is associated with the <code>Job</code>.
   * </p>
   *
   * <p>
   * The implementation may wish to set a
   * {@link JobExecutionContext#setResult(Object) result} object on the
   * {@link JobExecutionContext} before this method exits.  The result itself
   * is meaningless to Quartz, but may be informative to
   * <code>{@link JobListener}s</code> or
   * <code>{@link TriggerListener}s</code> that are watching the job's
   * execution.
   * </p>
   *
   * @throws JobExecutionException if there is an exception while executing the job.
   */
  protected abstract void executeImpl(JobExecutionContext context) throws JobExecutionException;
}