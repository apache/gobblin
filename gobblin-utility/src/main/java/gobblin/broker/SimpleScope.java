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
import gobblin.broker.iface.ScopeType;

import lombok.Data;


/**
 * A simple {@link ScopeInstance} implementation containing just a {@link ScopeType} and an string id.
 */
@Data
public class SimpleScope<S extends ScopeType<S>> implements ScopeInstance<S> {
  private final S type;
  private final String scopeId;
}
