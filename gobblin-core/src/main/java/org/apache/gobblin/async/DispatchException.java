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

package org.apache.gobblin.async;

/**
 * Exception for dispatching failures. By default, it is a fatal exception
 */
public class DispatchException extends Exception {
  private final boolean isFatalException;
  public DispatchException(String message, Exception e) {
    this(message, e, true);
  }

  public DispatchException(String message) {
    this(message, true);
  }

  public DispatchException(String message, Exception e, boolean isFatal) {
    super(message, e);
    isFatalException = isFatal;
  }

  public DispatchException(String message, boolean isFatal) {
    super(message);
    isFatalException = isFatal;
  }

  public boolean isFatal() {
    return isFatalException;
  }
}
