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

package gobblin.broker;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import gobblin.broker.iface.ScopeInstance;
import gobblin.broker.iface.ScopeType;

import javax.annotation.Nullable;
import lombok.AllArgsConstructor;


/**
 * Simple {@link ScopeType} topology with only two levels.
 */
@AllArgsConstructor
public enum SimpleScopeType implements ScopeType<SimpleScopeType> {

  GLOBAL("global"),
  LOCAL("local", GLOBAL);

  private static final Set<SimpleScopeType> LOCAL_SCOPES = Sets.newHashSet(LOCAL);

  private final List<SimpleScopeType> parentScopes;
  private final String defaultId;

  SimpleScopeType(String defaultId, SimpleScopeType... parentScopes) {
    this.defaultId = defaultId;
    this.parentScopes = Lists.newArrayList(parentScopes);
  }

  @Override
  public boolean isLocal() {
    return LOCAL_SCOPES.contains(this);
  }

  @Override
  public Collection<SimpleScopeType> parentScopes() {
    return this.parentScopes;
  }

  @Nullable
  @Override
  public ScopeInstance defaultScopeInstance() {
    return this.defaultId == null ? null : new SimpleScope(this, this.defaultId);
  }

  @Override
  public SimpleScopeType rootScope() {
    return GLOBAL;
  }

}
