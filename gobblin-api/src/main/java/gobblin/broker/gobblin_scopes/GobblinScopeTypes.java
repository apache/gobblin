/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobblin.broker.gobblin_scopes;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import gobblin.broker.iface.ScopeInstance;
import gobblin.broker.iface.ScopeType;

import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Getter;


/**
 * Scope topology for Gobblin ingestion applications.
 */
public enum GobblinScopeTypes implements ScopeType<GobblinScopeTypes> {

  GLOBAL("global", GobblinScopeInstance.class),
  INSTANCE("instance", GobblinScopeInstance.class, GLOBAL),
  JOB(null, JobScopeInstance.class, INSTANCE),
  CONTAINER("container", GobblinScopeInstance.class, INSTANCE),
  MULTI_TASK_ATTEMPT("multiTask", GobblinScopeInstance.class, JOB, CONTAINER),
  TASK(null, TaskScopeInstance.class, MULTI_TASK_ATTEMPT);

  private static final Set<GobblinScopeTypes> LOCAL_SCOPES = Sets.newHashSet(CONTAINER, TASK, MULTI_TASK_ATTEMPT);

  private final List<GobblinScopeTypes> parentScopes;
  private final String defaultId;
  @Getter(AccessLevel.PACKAGE)
  private final Class<?> baseInstanceClass;

  GobblinScopeTypes(String defaultId, Class<?> baseInstanceClass, GobblinScopeTypes... parentScopes) {
    this.defaultId = defaultId;
    this.parentScopes = Lists.newArrayList(parentScopes);
    this.baseInstanceClass = baseInstanceClass;
  }

  @Override
  public boolean isLocal() {
    return LOCAL_SCOPES.contains(this);
  }

  @Override
  public Collection<GobblinScopeTypes> parentScopes() {
    return this.parentScopes;
  }

  @Nullable
  @Override
  public ScopeInstance<GobblinScopeTypes> defaultScopeInstance() {
    return this.defaultId == null ? null : new GobblinScopeInstance(this, this.defaultId);
  }

  @Override
  public GobblinScopeTypes rootScope() {
    return GLOBAL;
  }

}
