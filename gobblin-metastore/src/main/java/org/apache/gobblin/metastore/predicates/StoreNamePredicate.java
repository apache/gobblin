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

import lombok.Getter;


/**
 * A {@link StateStorePredicate} to select only entries with a specific {@link #storeName}.
 */
public class StoreNamePredicate extends StateStorePredicate {

  @Getter
  private final String storeName;

  public StoreNamePredicate(String storeName, Predicate<StateStoreEntryManager> customPredicate) {
    super(customPredicate);
    this.storeName = storeName;
  }

  @Override
  public boolean apply(StateStoreEntryManager input) {
    return input.getStoreName().equals(this.storeName) && super.apply(input);
  }
}
