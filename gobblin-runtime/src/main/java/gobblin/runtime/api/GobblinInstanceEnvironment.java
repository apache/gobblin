/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.runtime.api;

import org.slf4j.Logger;

import gobblin.instrumented.Instrumentable;

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

}
