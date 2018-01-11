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
package org.apache.gobblin.configuration;

import com.typesafe.config.Config;

/**
 * For generating dynamic configuration that gets added to the job configuration.
 * These are configuration values that cannot be determined statically at job specification time.
 * One example is the SSL certificate location of a certificate that is fetched at runtime.
 */
public interface DynamicConfigGenerator {
  /**
   * Generate dynamic configuration that should be added to the job configuration.
   * @param config configuration
   * @return config object with the dynamic configuration
   */
  Config generateDynamicConfig(Config config);
}
