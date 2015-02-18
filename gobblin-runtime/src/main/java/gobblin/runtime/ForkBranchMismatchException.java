/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime;

/**
 * A type of {@link java.lang.Exception}s thrown when the number of schemas or
 * data records returned by a {@link gobblin.fork.ForkOperator}
 * is not equal to the number of declared branches.
 *
 * @author ynli
 */
public class ForkBranchMismatchException extends Exception {

  public ForkBranchMismatchException(String message) {
    super(message);
  }
}
