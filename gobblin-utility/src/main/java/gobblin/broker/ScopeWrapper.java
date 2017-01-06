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

import gobblin.broker.iface.ScopeInstance;
import java.util.Collection;

import gobblin.broker.iface.ScopeType;

import lombok.Data;


/**
 * A wrapper around a {@link ScopeInstance} used by {@link SharedResourcesBrokerImpl} to store a
 * {@link ScopeInstance} and its descendants.
 */
@Data
class ScopeWrapper<S extends ScopeType<S>> {
  private final S type;
  private final ScopeInstance<S> scope;
  private final Collection<ScopeWrapper<S>> parentScopes;
}
