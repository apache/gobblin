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
package gobblin.runtime.api;

import gobblin.annotation.Alpha;

/**
 * Defines an interface for managing a collection of {@JobExecutionStateListener}s
 */
@Alpha
public interface JobExecutionStateListenerContainer {
  void registerStateListener(JobExecutionStateListener listener);
  /** Like {@link #registerStateListener(JobExecutionStateListener)} but it will create a weak
   * reference. The implementation will automatically remove the listener registration once the
   * listener object gets GCed.
   *
   * <p>Note that weak listeners cannot be removed using {@link #unregisterStateListener(JobExecutionStateListener)}.
   **/
  void registerWeakStateListener(JobExecutionStateListener listener);
  void unregisterStateListener(JobExecutionStateListener listener);
}
