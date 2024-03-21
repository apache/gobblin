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

import javax.inject.Inject;
import javax.inject.Provider;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;


/**
 * A factory implementation that returns a {@link MysqlMultiActiveLeaseArbiter} instance used by the
 * {@link DagManagementTaskStreamImpl} in multi-active execution mode
 */
@Slf4j
public class DagActionExecutionMultiActiveLeaseArbiterFactory implements Provider<InstrumentedLeaseArbiter> {
  private final Config config;

  @Inject
  public DagActionExecutionMultiActiveLeaseArbiterFactory(Config config) {
    this.config = Objects.requireNonNull(config);
  }

  private InstrumentedLeaseArbiter createDagProcLeaseArbiter()
      throws ReflectiveOperationException, IOException {
    Config dagProcLeaseArbiterConfig = this.config.getConfig(ConfigurationKeys.EXECUTOR_LEASE_ARBITER_NAME);
    log.info("ReminderSettingDagProc lease arbiter will be initialized with config {}", dagProcLeaseArbiterConfig);

    return new InstrumentedLeaseArbiter(dagProcLeaseArbiterConfig,
        new MysqlMultiActiveLeaseArbiter(dagProcLeaseArbiterConfig), ConfigurationKeys.EXECUTOR_LEASE_ARBITER_NAME);
  }

  @Override
  public InstrumentedLeaseArbiter get() {
    try {
      InstrumentedLeaseArbiter leaseArbiter = createDagProcLeaseArbiter();
      return leaseArbiter;
    } catch (ReflectiveOperationException | IOException e) {
      throw new RuntimeException("Failed to initialize" + ConfigurationKeys.EXECUTOR_LEASE_ARBITER_NAME + " due to ", e);
    }
  }
}
