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

import gobblin.annotation.Alpha;


/**
 * An {@link Exception} thrown by an {@link ApplicationLauncher}.
 */
@Alpha
public class ApplicationException extends Exception {

  private static final long serialVersionUID = -7131035635096992762L;

  public ApplicationException(String message, Throwable cause) {
    super(message, cause);
  }
}
