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

package org.apache.gobblin.runtime.util;

public final class ExceptionCleanupUtils {

  private ExceptionCleanupUtils() {
  }

  /**
   * Removes exceptions that were wrapping another exception without providing a message of their own.
   *
   * When a checked exception is defined on the interface, and one of the implementations want to propagate a
   * different type of exception, that implementation can wrap the original exception. For example, Gobblin
   * codebase frequently wraps exception into new IOException(cause). As a result, users see large stack traces
   * where real error is hidden below several wrappers. This method will remove the wrappers to provide users with
   * better error messages.
   * */
  public static Throwable removeEmptyWrappers(Throwable exception) {
    if (exception == null) {
      return null;
    }

    if (exception.getCause() != null && exception.getCause().toString().equals(exception.getMessage())) {
      return removeEmptyWrappers(exception.getCause());
    }

    return exception;
  }
}
