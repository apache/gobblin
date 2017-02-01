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

import gobblin.broker.iface.ScopeType;
import gobblin.broker.iface.SharedResourceFactory;
import gobblin.broker.iface.SharedResourceFactoryResponse;
import gobblin.broker.iface.SharedResourceKey;
import lombok.Data;


/**
 * A {@link SharedResourceFactoryResponse} that indicates the broker should return the object at a different coordinate
 * (ie. factory, key, scope combination).
 */
@Data
public class ResourceCoordinate<T, K extends SharedResourceKey, S extends ScopeType<S>> implements SharedResourceFactoryResponse<T> {
  private final SharedResourceFactory<T, K, S> factory;
  private final K key;
  private final S scope;
}
