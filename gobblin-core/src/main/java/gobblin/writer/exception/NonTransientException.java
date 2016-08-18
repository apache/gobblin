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
package gobblin.writer.exception;

/**
 * NonTransientException that shows this is a permanent failure where retry cannot solve.
 */
public class NonTransientException extends RuntimeException {
  private static final long serialVersionUID = -973030180704599529L;

  public NonTransientException() {
    super();
  }

  public NonTransientException(String message) {
    super(message);
  }

  public NonTransientException(String message, Throwable t) {
    super(message, t);
  }

  public NonTransientException(Throwable t) {
    super(t);
  }
}