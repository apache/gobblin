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
package org.apache.gobblin.runtime.locks;

import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.typesafe.config.Config;

/**
 * A factory class for {@link JobLockFactory} instances. It allows factories to be configured
 * using Gobblin instance configuration.
 *
 * <p>Implementations of this interface must define the default constructor</p>
 */
public interface JobLockFactoryManager<T extends JobLock, F extends JobLockFactory<T>> {

  /** Provides an instance of a job lock factory with the specified config. If an instance with
   * the same configuration (implementation-specific) already exists, the old instance may be
   * returned to avoid race condition. This behavior is implementation-specific. */
  F getJobLockFactory(Config sysCfg, Optional<Logger> log);
}
