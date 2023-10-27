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

package org.apache.gobblin.temporal.cluster;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import lombok.extern.slf4j.Slf4j;

import com.typesafe.config.Config;


/**
 * Static holder to stash the {@link Config} used to construct each kind of {@link org.apache.gobblin.temporal.cluster.TemporalWorker}
 * (within the current JVM).  Lookup may be by either the {@link Class} of the worker or of any workflow or activity implementation supplied by
 * that worker.  The objective is to facilitate sharing the worker's Config with workflow and activity implementations (running within that worker).
 *
 * ATTENTION: for sanity, construct multiple instances of the same worker always with the same {@link Config}.  When this is violated, the `Config`
 * given to the most-recently constructed worker "wins".
 *
 * NOTE: the preservation and sharing of {@link Config} is predicated entirely on its immutability.  Thank you TypeSafe!
 * Storage indexing uses FQ class name, not the {@link Class}, to be independent of classloader.
 */
@Slf4j
public class WorkerConfig {
  private static final ConcurrentHashMap<String, Config> configByFQClassName = new ConcurrentHashMap<>();

  private WorkerConfig() {}

  /** @return whether initialized now (vs. being previously known) */
  public static boolean forWorker(Class<? extends TemporalWorker> workerClass, Config config) {
    return storeAs(workerClass.getName(), config);
  }

  /** @return whether initialized now (vs. being previously known) */
  public static boolean withImpl(Class<?> workflowOrActivityImplClass, Config config) {
    return storeAs(workflowOrActivityImplClass.getName(), config);
  }

  public static Optional<Config> ofWorker(Class<? extends TemporalWorker> workerClass) {
    return Optional.ofNullable(configByFQClassName.get(workerClass.getName()));
  }

  public static Optional<Config> ofImpl(Class<?> workflowOrActivityImplClass) {
    return Optional.ofNullable(configByFQClassName.get(workflowOrActivityImplClass.getName()));
  }

  public static Optional<Config> of(Object workflowOrActivityImpl) {
    return ofImpl(workflowOrActivityImpl.getClass());
  }

  private static boolean storeAs(String className, Config config) {
    Config prior = configByFQClassName.put(className, config);
    log.info("storing config of {} values as '{}'{}", config.entrySet().size(), className, prior == null ? " (new)" : "");
    return prior == null;
  }
}
