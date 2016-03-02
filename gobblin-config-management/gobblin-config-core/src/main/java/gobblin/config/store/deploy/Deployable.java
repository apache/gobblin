/*
 * Copyright (C) 2015-16 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.config.store.deploy;

import java.io.IOException;

import gobblin.config.store.api.ConfigStore;


/**
 * An interface to deploy and rollback {@link ConfigStore}s. {@link ConfigStore}s that implement {@link Deployable} can
 * be deployed using {@link StoreDeployer}
 *
 * @param <D> {@link DeploymentConfig} or its subclasses that has configs to deploy the store
 * @param <R> {@link RollbackConfig} or its subclasses that has configs to rollback
 */
public interface Deployable<D extends DeploymentConfig, R extends RollbackConfig> {

  /**
   * Deploy a version {@link DeploymentConfig#getNewVersion()} of configs provided by
   * {@link DeploymentConfig#getDeployableConfigSource()} on the {@link ConfigStore}
   *
   * @param deploymentConfig to use for this deployment
   */
  public void deploy(D deploymentConfig) throws IOException;

  /**
   * Rollback to an older version of configs on the {@link ConfigStore} using {@link RollbackConfig}
   *
   * @param rollbackConfig to use for this rollback
   */
  public void rollback(R rollbackConfig) throws IOException;
}
