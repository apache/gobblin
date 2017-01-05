/*
 * Copyright (C) 2014-2017 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.broker;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import gobblin.broker.iface.ScopeType;

import javax.annotation.Nullable;
import lombok.AllArgsConstructor;


/**
 * Simple {@link ScopeType} topology with only two levels.
 */
@AllArgsConstructor
public enum  SimpleScope implements ScopeType {

  GLOBAL("global"),
  LOCAL("local", GLOBAL);

  private static final Set<SimpleScope> LOCAL_SCOPES = Sets.newHashSet(LOCAL);

  private final List<SimpleScope> parentScopes;
  private final String defaultId;

  SimpleScope(String defaultId, SimpleScope... parentScopes) {
    this.defaultId = defaultId;
    this.parentScopes = Lists.newArrayList(parentScopes);
  }

  @Override
  public boolean isLocal() {
    return LOCAL_SCOPES.contains(this);
  }

  @Override
  public Collection<SimpleScope> parentScopes() {
    return this.parentScopes;
  }

  @Nullable
  @Override
  public String defaultId() {
    return this.defaultId;
  }

}
