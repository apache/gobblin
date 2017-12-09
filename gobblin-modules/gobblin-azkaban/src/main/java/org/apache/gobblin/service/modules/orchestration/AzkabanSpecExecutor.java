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
package org.apache.gobblin.service.modules.orchestration;

import java.util.concurrent.Future;

import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.runtime.spec_executorInstance.AbstractSpecExecutor;
import org.apache.gobblin.util.CompletedFuture;
import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


public class AzkabanSpecExecutor extends AbstractSpecExecutor {

  // Executor Instance
  protected final Config _config;

  private SpecProducer<Spec> azkabanSpecProducer;

  public AzkabanSpecExecutor(Config config) {
    this(config, Optional.absent());
  }

  public AzkabanSpecExecutor(Config config, Optional<Logger> log) {
    super(config, log);
    Config defaultConfig = ConfigFactory.load(ServiceAzkabanConfigKeys.DEFAULT_AZKABAN_PROJECT_CONFIG_FILE);
    _config = config.withFallback(defaultConfig);
    azkabanSpecProducer = new AzkabanSpecProducer(_config, log);
  }

  @Override
  public Future<String> getDescription() {
    return new CompletedFuture<>("SimpleSpecExecutorInstance with URI: " + specExecutorInstanceUri, null);
  }


  @Override
  public Future<? extends SpecProducer> getProducer() {
    return new CompletedFuture<>(this.azkabanSpecProducer, null);
  }

  @Override
  public Future<Config> getConfig() {
    return new CompletedFuture<>(_config, null);
  }

  @Override
  public Future<String> getHealth() {
    return new CompletedFuture<>("Healthy", null);
  }

  @Override
  protected void startUp() throws Exception {
    // nothing to do in default implementation
  }

  @Override
  protected void shutDown() throws Exception {
    // nothing to do in default implementation
  }
}
