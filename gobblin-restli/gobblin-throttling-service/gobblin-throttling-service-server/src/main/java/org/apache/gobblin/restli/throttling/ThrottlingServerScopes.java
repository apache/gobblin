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

package org.apache.gobblin.restli.throttling;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.gobblin.broker.SimpleScope;
import org.apache.gobblin.broker.iface.ScopeInstance;
import org.apache.gobblin.broker.iface.ScopeType;

import javax.annotation.Nullable;


/**
 * Scopes for throttling server.
 */
public enum ThrottlingServerScopes implements ScopeType<ThrottlingServerScopes> {

  GLOBAL("global"),
  LEADER("leader", GLOBAL),
  SLAVE("slave", LEADER);

  private static final Set<ThrottlingServerScopes> LOCAL_SCOPES = Sets.newHashSet(LEADER, SLAVE);

  private final List<ThrottlingServerScopes> parentScopes;
  private final String defaultId;

  ThrottlingServerScopes(String defaultId, ThrottlingServerScopes... parentScopes) {
    this.defaultId = defaultId;
    this.parentScopes = Lists.newArrayList(parentScopes);
  }

  @Override
  public boolean isLocal() {
    return LOCAL_SCOPES.contains(this);
  }

  @Override
  public Collection<ThrottlingServerScopes> parentScopes() {
    return this.parentScopes;
  }

  @Nullable
  @Override
  public ScopeInstance defaultScopeInstance() {
    return this.defaultId == null ? null : new SimpleScope<>(this, this.defaultId);
  }

  @Override
  public ThrottlingServerScopes rootScope() {
    return GLOBAL;
  }

}
