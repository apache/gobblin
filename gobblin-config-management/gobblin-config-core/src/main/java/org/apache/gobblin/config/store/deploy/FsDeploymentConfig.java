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
package org.apache.gobblin.config.store.deploy;

import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * A {@link DeploymentConfig} for Hadoop {@link FileSystem} based stores.
 */
@Getter
@ToString
public class FsDeploymentConfig extends DeploymentConfig {

  /**
   * Since the config store needs to be accessed by all users, the default permission of the store will be read-execute
   * for all users
   */
  public static final FsPermission DEFAULT_STORE_PERMISSIONS = new FsPermission(FsAction.ALL, FsAction.READ_EXECUTE,
      FsAction.READ_EXECUTE);

  /**
   * Build a new {@link FsDeploymentConfig}
   *
   * @param deployableConfigSource Source that provides the deployable configs
   * @param version to be used for this deployment
   * @param storePermissions for configs being deployed
   */
  public FsDeploymentConfig(@NonNull final DeployableConfigSource deployableConfigSource, @NonNull final String version,
      @NonNull final FsPermission storePermissions) {
    super(deployableConfigSource, version);
    this.storePermissions = storePermissions;
  }

  /**
   * Build a new {@link FsDeploymentConfig} using the default store permission {@link #DEFAULT_STORE_PERMISSIONS}
   *
   * @param deployableConfigSource Source that provides the deployable configs
   * @param version to be used for this deployment
   */
  public FsDeploymentConfig(final DeployableConfigSource deployableConfigSource, final String version) {
    this(deployableConfigSource, version, DEFAULT_STORE_PERMISSIONS);
  }

  /**
   * Permission to be set on the configs deployed
   */
  private final FsPermission storePermissions;

}
