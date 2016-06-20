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
package gobblin.data.management.conversion.hive.provider;

/**
 * An exception when {@link HiveUnitUpdateProvider} can not find updates
 */
public class UpdateNotFoundException extends Exception {

  private static final long serialVersionUID = -3750962295968867238L;

  public UpdateNotFoundException() {
    super();
  }

  public UpdateNotFoundException(String message) {
    super(message);
  }

  public UpdateNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  public UpdateNotFoundException(Throwable cause) {
    super(cause);
  }

}
