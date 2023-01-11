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

package org.apache.gobblin.cluster;

/**
 * Exception to describe situations where Gobblin sees unexpected state from Helix. Historically, we've seen unexpected
 * null values, which bubble up as NPE. This exception is explicitly used to differentiate bad Gobblin code from
 * Helix failures (i.e. seeing a NPE implies Gobblin bug)
 */
public class GobblinHelixUnexpectedStateException extends Exception {
  public GobblinHelixUnexpectedStateException(String message, Object... args) {
    super(String.format(message, args));
  }
}
