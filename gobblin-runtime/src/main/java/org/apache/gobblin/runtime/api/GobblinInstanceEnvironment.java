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

import org.slf4j.Logger;

import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.instrumented.Instrumentable;

/**
 * Defines Gobblin a set of standard configuration features for a gobblin instance. Passing an
 * instance of this interface can be used to configure Gobbling components like {@link JobCatalog},
 * {@link JobSpecScheduler} or {@link JobExecutionLauncher}.
 */
public interface GobblinInstanceEnvironment extends Instrumentable {

  /** The instance name (for debugging/logging purposes) */
  String getInstanceName();

  /** The logger used by this instance*/
  Logger getLog();

  /** The global system-wide configuration, typically provided by the {@link GobblinInstanceLauncher} */
  Configurable getSysConfig();

  /** The {@link SharedResourcesBroker} for this instance. */
  SharedResourcesBroker<GobblinScopeTypes> getInstanceBroker();

}
