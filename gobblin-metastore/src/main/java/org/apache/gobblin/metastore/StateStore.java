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

package org.apache.gobblin.metastore;

import com.google.common.base.Predicate;
import com.typesafe.config.Config;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.metadata.StateStoreEntryManager;
import org.apache.gobblin.metastore.predicates.StateStorePredicate;


/**
 * An interface for stores that persist {@link State}s.
 *
 * <p>
 *     Each such store consists of zero or more tables, and each table
 *     stores zero or more {@link State}s keyed on the state IDs (see
 *     {@link State#getId()}).
 * </p>
 *
 * <p>
 *   Note: Implementations of dataset store should maintain a timestamp for every state they persist. Certain utilities
 *   will not work if this is not the case.
 * </p>
 *
 * @param <T> state object type
 *
 * @author Yinan Li
 */
public interface StateStore<T extends State> {

  interface Factory {
    <T extends State> StateStore<T> createStateStore(Config config, Class<T> stateClass);
  }

  /**
   * Create a new store.
   *
   * <p>
   *     A store that does not exist will be created when any put
   *     method is called against it.
   * </p>
   *
   * @param storeName store name
   * @return if the store is successfully created
   * @throws IOException
   */
  public boolean create(String storeName)
      throws IOException;

  /**
   * Create a new table in a store.
   *
   * <p>
   *     A table that does not exist will be created when any put
   *     method is called against it.
   * </p>
   *
   * @param storeName store name
   * @param tableName table name
   * @return if the table is successfully created
   * @throws IOException
   */
  public boolean create(String storeName, String tableName)
      throws IOException;

  /**
   * Check whether a given table exists.
   *
   * @param storeName store name
   * @param tableName table name
   * @return whether the given table exists
   * @throws IOException
   */
  public boolean exists(String storeName, String tableName)
      throws IOException;

  /**
   * Put a {@link State} into a table.
   *
   * <p>
   *     Calling this method against a store or a table that
   *     does not exist will cause it to be created.
   * </p>
   *
   * @param storeName store name
   * @param tableName table name
   * @param state {@link State} to be put into the table
   * @throws IOException
   */
  public void put(String storeName, String tableName, T state)
      throws IOException;

  /**
   * Put a collection of {@link State}s into a table.
   *
   * <p>
   *     Calling this method against a store or a table that
   *     does not exist will cause it to be created.
   * </p>
   *
   * @param storeName store name
   * @param tableName table name
   * @param states collection of {@link State}s to be put into the table
   * @throws IOException
   */
  public void putAll(String storeName, String tableName, Collection<T> states)
      throws IOException;

  /**
   * Get a {@link State} with a given state ID from a table.
   *
   * @param storeName store name
   * @param tableName table name
   * @param stateId state ID
   * @return {@link State} with the given state ID or <em>null</em>
   *         if the state with the given state ID does not exist
   * @throws IOException
   */
  public T get(String storeName, String tableName, String stateId)
      throws IOException;

  /**
   * Get all {@link State}s from a table.
   *
   * @param storeName store name
   * @param tableName table name
   * @return (possibly empty) list of {@link State}s from the given table
   * @throws IOException
   */
  public List<T> getAll(String storeName, String tableName)
      throws IOException;

  /**
   * Get all {@link State}s from a store.
   *
   * @param storeName store name
   * @return (possibly empty) list of {@link State}s from the given store
   * @throws IOException
   */
  public List<T> getAll(String storeName)
      throws IOException;

  /**
   * Get table names under the storeName
   *
   * @param storeName store name
   * @param predicate only returns names matching predicate
   * @return (possibly empty) list of state names from the given store
   * @throws IOException
   */
  public List<String> getTableNames(String storeName, Predicate<String> predicate)
      throws IOException;

  /**
   * Create an alias for an existing table.
   *
   * @param storeName store name
   * @param original original table name
   * @param alias alias table name
   * @throws IOException
   */
  public void createAlias(String storeName, String original, String alias)
      throws IOException;

  /**
   * Delete a table from a store.
   *
   * @param storeName store name
   * @param tableName table name
   * @throws IOException
   */
  public void delete(String storeName, String tableName)
      throws IOException;

  /**
   * Delete a store.
   *
   * @param storeName store name
   * @throws IOException
   */
  public void delete(String storeName)
      throws IOException;

  /**
   * Gets entry managers for all tables matching the input
   * @param predicate Predicate used to filter tables. To allow state stores to push down predicates, use native extensions
   *                  of {@link StateStorePredicate}.
   * @return A list of all {@link StateStoreEntryManager}s matching the predicate.
   * @throws IOException
   */
  default List<? extends StateStoreEntryManager> getMetadataForTables(StateStorePredicate predicate)
      throws IOException {
    throw new UnsupportedOperationException("Operation unsupported for predicate with class " + predicate.getClass());
  }
}
