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

package org.apache.gobblin.metastore.predicates;

import org.apache.gobblin.metastore.metadata.StateStoreEntryManager;

import com.google.common.base.Predicate;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;


/**
 * A {@link Predicate} used to filter entries in a {@link org.apache.gobblin.metastore.StateStore}.
 *
 * {@link org.apache.gobblin.metastore.StateStore}s can usually partially push down extensions of this class, so it
 * is recommended to use bundled {@link StateStorePredicate} extensions as much as possible.
 */
@RequiredArgsConstructor
public class StateStorePredicate implements Predicate<StateStoreEntryManager> {

  /**
   * An additional {@link Predicate} for filtering. This predicate is never pushed down.
   */
  @Delegate
  private final Predicate<StateStoreEntryManager> customPredicate;
}
