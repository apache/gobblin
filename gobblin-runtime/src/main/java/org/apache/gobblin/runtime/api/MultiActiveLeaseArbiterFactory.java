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

package org.apache.gobblin.runtime.api;

import java.io.IOException;
import java.util.Objects;

import com.typesafe.config.Config;

import javax.inject.Provider;
import lombok.extern.slf4j.Slf4j;


/**
 * An abstract base class for creating {@link MultiActiveLeaseArbiter} factories that use a specific configuration key.
 * Subclasses must provide a key to use in the constructor.
 */
@Slf4j
public abstract class MultiActiveLeaseArbiterFactory implements Provider<MultiActiveLeaseArbiter> {
    private final Config config;
    private final String key;

    public MultiActiveLeaseArbiterFactory(Config config, String key) {
      this.config = Objects.requireNonNull(config);
      this.key = Objects.requireNonNull(key);
    }

    @Override
    public MultiActiveLeaseArbiter get() {
      try {
        Config leaseArbiterConfig = this.config.getConfig(key);
        log.info("Lease arbiter will be initialized with config {}", leaseArbiterConfig);

        return new InstrumentedLeaseArbiter(config, new MysqlMultiActiveLeaseArbiter(leaseArbiterConfig), key);
      } catch (IOException e) {
        throw new RuntimeException("Failed to initialize " + key + " lease arbiter due to ", e);
      }
   }
}
