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

package gobblin.metrics.context;

import java.io.IOException;


/**
 * Thrown when a {@link gobblin.metrics.MetricContext} cannot be created as a child of a second
 * {@link gobblin.metrics.MetricContext} because the parent already has a {@link gobblin.metrics.MetricContext} with
 * that name.
 */
public class NameConflictException extends IOException {

  private static final long serialVersionUID = 5569840033725693663L;

  public NameConflictException(String message) {
    super(message);
  }

  public NameConflictException(String message, Throwable cause) {
    super(message, cause);
  }
}
