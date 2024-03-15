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

import java.util.Optional;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.helix.InstanceType;

import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.yarn.helix.HelixClusterLifecycleManager;


/**
 * <p>
 *   <br> Gobblin Yarn apps can run often need leverage Helix to do cluster management, and so this class provides optional
 *   mechanisms for sending token file updates via Helix. Token refreshes are needed when the application is expected
 *   to run for a long time, and the initial token is expected to expire.<br><br>
 *
 *   This class is meant to be shared by Gobblin apps that run on yarn with and without Helix
 * </p>
 */
@Slf4j
public abstract class AbstractYarnAppSecurityManager extends AbstractTokenRefresher {

  protected final Optional<HelixClusterLifecycleManager> helixClusterLifecycleManager;

  /**
   * Case 1: Gobblin-on-yarn without Helix integration. If Helix integration is needed, invoke
   * {@link AbstractYarnAppSecurityManager#AbstractYarnAppSecurityManager(Config, FileSystem, Path, HelixClusterLifecycleManager)}
   *
   * @param config
   * @param fs
   * @param tokenFilePath
   */
  public AbstractYarnAppSecurityManager(Config config, FileSystem fs, Path tokenFilePath) {
    this(config, fs, tokenFilePath, null);
  }

  /**
   * Case 2: Gobblin-on-yarn with Helix integration. If helix integration is not needed, use
   * {@link AbstractYarnAppSecurityManager#AbstractYarnAppSecurityManager(Config, FileSystem, Path)}
   * @param config
   * @param fs
   * @param tokenFilePath
   * @param helixClusterLifecycleManager
   */
  public AbstractYarnAppSecurityManager(Config config, FileSystem fs, Path tokenFilePath, HelixClusterLifecycleManager helixClusterLifecycleManager) {
    super(config, fs, tokenFilePath);
    this.helixClusterLifecycleManager = Optional.ofNullable(helixClusterLifecycleManager);
  }

  protected void sendTokenFileUpdatedMessage() {
    this.helixClusterLifecycleManager.ifPresent(HelixClusterLifecycleManager::sendTokenFileUpdatedMessage);
  }

  /**
   * This method is used to send TokenFileUpdatedMessage which will handle by {@link YarnContainerSecurityManager}
   */
  @VisibleForTesting
  protected void sendTokenFileUpdatedMessage(InstanceType instanceType) {
    if (this.helixClusterLifecycleManager.isPresent()) {
      this.helixClusterLifecycleManager.get().sendTokenFileUpdatedMessage(instanceType);
    }
  }
}
