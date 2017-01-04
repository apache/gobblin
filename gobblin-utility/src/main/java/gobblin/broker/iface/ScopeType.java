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

package gobblin.broker.iface;

import java.util.Collection;

import javax.annotation.Nullable;


/**
 * Represents a DAG of scope types.
 *
 * For example, the topology of a distributed application might have scopes as follows:
 * <pre>
 * GLOBAL -> INSTANCE --> JOB       --> TASK
 *                    \-> CONTAINER -/
 * </pre>
 *
 * Where global represents multiple separate instances or even other applications, the instance creates containers
 * and process jobs, and each task of a job is run in a container. As seen in the graph, instance is a child of
 * global, job and container are children of instance, and task is a child of both job and container.
 *
 * @param <S> itself.
 */
public interface ScopeType<S extends ScopeType<S>> {

  /**
   * The name of this {@link ScopeType}.
   */
  String name();

  /**
   * @return Whether this _scopeInstance is process-local or shared across different processes.
   */
  boolean isLocal();

  /**
   * @return Collection of parent scopes in the DAG.
   */
  @Nullable
  Collection<S> parentScopes();

  /**
   * @return a default id for this {@link ScopeType} if applicable.
   */
  @Nullable String defaultId();
}
