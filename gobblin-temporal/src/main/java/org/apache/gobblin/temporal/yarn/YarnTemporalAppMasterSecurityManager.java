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

package org.apache.gobblin.temporal.yarn;

import com.google.common.base.Throwables;
import com.google.common.eventbus.EventBus;
import com.typesafe.config.Config;
import java.io.IOException;

import org.apache.gobblin.util.logs.LogCopier;
import org.apache.gobblin.yarn.YarnContainerSecurityManager;
import org.apache.gobblin.yarn.event.DelegationTokenUpdatedEvent;

import org.apache.hadoop.fs.FileSystem;


/**
 * Copied from {@link org.apache.gobblin.yarn.YarnAppMasterSecurityManager} that uses the {@link YarnService}
 *
 * This class was created for a fast way to start building out Gobblin on temporal without affecting mainline Yarn/Helix code
 */
public class YarnTemporalAppMasterSecurityManager extends YarnContainerSecurityManager {

  private YarnService _yarnService;
  public YarnTemporalAppMasterSecurityManager(Config config, FileSystem fs, EventBus eventBus, LogCopier logCopier, YarnService yarnService) {
    super(config, fs, eventBus, logCopier);
    this._yarnService = yarnService;
  }

  @Override
  public void handleTokenFileUpdatedEvent(DelegationTokenUpdatedEvent delegationTokenUpdatedEvent) {
    super.handleTokenFileUpdatedEvent(delegationTokenUpdatedEvent);
    try {
      _yarnService.updateToken();
    } catch (IOException ioe) {
      throw Throwables.propagate(ioe);
    }
  }
}

