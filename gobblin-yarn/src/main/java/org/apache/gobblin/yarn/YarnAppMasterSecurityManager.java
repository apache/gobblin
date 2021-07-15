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

package org.apache.gobblin.yarn;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

import com.google.common.base.Throwables;
import com.google.common.eventbus.EventBus;
import com.typesafe.config.Config;

import org.apache.gobblin.util.logs.LogCopier;
import org.apache.gobblin.yarn.event.DelegationTokenUpdatedEvent;


public class YarnAppMasterSecurityManager extends YarnContainerSecurityManager{

  private YarnService yarnService;
  public YarnAppMasterSecurityManager(Config config, FileSystem fs, EventBus eventBus, LogCopier logCopier, YarnService yarnService) {
    super(config, fs, eventBus, logCopier);
    this.yarnService = yarnService;
  }

  @Override
  public void handleTokenFileUpdatedEvent(DelegationTokenUpdatedEvent delegationTokenUpdatedEvent) {
    super.handleTokenFileUpdatedEvent(delegationTokenUpdatedEvent);
    try {
      yarnService.updateToken();
    } catch (IOException ioe) {
      throw Throwables.propagate(ioe);
    }
  }
}
