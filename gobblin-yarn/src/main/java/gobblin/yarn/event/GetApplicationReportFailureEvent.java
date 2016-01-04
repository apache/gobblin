/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.yarn.event;

import org.apache.hadoop.yarn.api.records.ApplicationReport;


/**
 * A type of events posted when the {@link gobblin.yarn.GobblinYarnAppLauncher} fails to get
 * the {@link ApplicationReport} of the Gobblin Yarn application.
 *
 * @author Yinan Li
 */
public class GetApplicationReportFailureEvent {

  private final Throwable cause;

  public GetApplicationReportFailureEvent(Throwable cause) {
    this.cause = cause;
  }

  /**
   * Get the {@link Throwable} that's the cause of the failure.
   *
   * @return the {@link Throwable} that's the cause of the failure
   */
  public Throwable getCause() {
    return this.cause;
  }
}
