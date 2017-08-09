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

package gobblin.runtime.spec_executorInstance;

import java.util.concurrent.Future;

import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.typesafe.config.Config;

import gobblin.runtime.api.Spec;
import gobblin.util.CompletedFuture;
import gobblin.runtime.api.SpecExecutor;
import gobblin.runtime.api.SpecConsumer;
import gobblin.runtime.api.SpecProducer;
import gobblin.runtime.api.GobblinInstanceEnvironment;



/**
 * An {@link SpecExecutor} implementation that keep provisioned {@link Spec} in memory.
 * Therefore there's no necessity to install {@link SpecConsumer} in this case.
 */
public class InMemorySpecExecutor extends AbstractSpecExecutor {
  // Communication mechanism components.
  // Not specifying final for further extension based on this implementation.
  private SpecProducer<Spec> inMemorySpecProducer;

  public InMemorySpecExecutor(Config config){
    this(config, Optional.absent());
  }

  public InMemorySpecExecutor(Config config, GobblinInstanceEnvironment env){
    this(config, Optional.of(env.getLog()));
  }

  public InMemorySpecExecutor(Config config, Optional<Logger> log) {
    super(config, log);
    inMemorySpecProducer = new InMemorySpecProducer(config);
  }

  @Override
  public Future<String> getDescription() {
    return new CompletedFuture("InMemory SpecExecutor", null);
  }

  @Override
  public Future<? extends SpecProducer> getProducer(){
    return new CompletedFuture(this.inMemorySpecProducer, null);
  }

  @Override
  protected void startUp() throws Exception {
    // Nothing to do in the abstract implementation.
  }

  @Override
  protected void shutDown() throws Exception {
    // Nothing to do in the abstract implementation.
  }

}
