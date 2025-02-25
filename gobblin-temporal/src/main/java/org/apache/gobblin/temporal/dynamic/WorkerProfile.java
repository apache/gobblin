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

package org.apache.gobblin.temporal.dynamic;

import com.typesafe.config.Config;
import lombok.AllArgsConstructor;
import lombok.Data;


/** A named worker {@link Config} */
@Data
@AllArgsConstructor
public class WorkerProfile {
  private final String name;
  private final Config config;

  /**
   * Constructs a `WorkerProfile` with the baseline name and the specified configuration.
   *
   * @param config the configuration for the worker profile
   */
  public WorkerProfile(Config config) {
    this(WorkforceProfiles.BASELINE_NAME, config);
  }
}
