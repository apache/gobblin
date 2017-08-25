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

package org.apache.gobblin.runtime.spec_executorInstance;

import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;

import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.util.CompletedFuture;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InMemorySpecProducer implements SpecProducer<Spec>, Serializable {
  private final Map<URI, Spec> provisionedSpecs;
  private transient Config config;

  private static final long serialVersionUID = 6106269076155338045L;

  public InMemorySpecProducer(Config config) {
    this.config = config;
    this.provisionedSpecs = Maps.newHashMap();
  }

  @Override
  public Future<?> addSpec(Spec addedSpec) {
    provisionedSpecs.put(addedSpec.getUri(), addedSpec);
    log.info(String.format("Added Spec: %s with Uri: %s for execution on this executor.", addedSpec, addedSpec.getUri()));

    return new CompletedFuture(Boolean.TRUE, null);
  }

  @Override
  public Future<?> updateSpec(Spec updatedSpec) {
    if (!provisionedSpecs.containsKey(updatedSpec.getUri())) {
      throw new RuntimeException("Spec not found: " + updatedSpec.getUri());
    }
    provisionedSpecs.put(updatedSpec.getUri(), updatedSpec);
    log.info(String.format("Updated Spec: %s with Uri: %s for execution on this executor.", updatedSpec, updatedSpec.getUri()));

    return new CompletedFuture(Boolean.TRUE, null);
  }

  @Override
  public Future<?> deleteSpec(URI deletedSpecURI) {
    if (!provisionedSpecs.containsKey(deletedSpecURI)) {
      throw new RuntimeException("Spec not found: " + deletedSpecURI);
    }
    provisionedSpecs.remove(deletedSpecURI);
    log.info(String.format("Deleted Spec with Uri: %s from this executor.", deletedSpecURI));

    return new CompletedFuture(Boolean.TRUE, null);
  }

  @Override
  public Future<? extends List<Spec>> listSpecs() {
    return new CompletedFuture<>(Lists.newArrayList(provisionedSpecs.values()), null);
  }
}