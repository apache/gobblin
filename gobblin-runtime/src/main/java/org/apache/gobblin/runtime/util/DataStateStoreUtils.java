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
package org.apache.gobblin.runtime.util;

import java.io.IOException;

import lombok.extern.slf4j.Slf4j;

import com.typesafe.config.Config;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.DatasetStateStore;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Utilities for creating and managing {@link DatasetStateStore} instances based on job configuration.
 */
@Slf4j
public class DataStateStoreUtils {

  /**
   * Private constructor to prevent instantiation of this utility class.
   */
  private DataStateStoreUtils() {
    // Utility class should not be instantiated
  }

  /**
   * Creates a {@link DatasetStateStore} instance based on the provided job configuration.
   *
   * <p>This method performs the following operations:</p>
   * <ol>
   *   <li>Checks if state store is enabled via {@link ConfigurationKeys#STATE_STORE_ENABLED}</li>
   *   <li>Determines the appropriate state store type from configuration hierarchy</li>
   *   <li>Uses {@link ClassAliasResolver} to resolve the state store factory class</li>
   *   <li>Creates and returns the configured {@link DatasetStateStore} instance</li>
   * </ol>
   *
   * <p>If state store is disabled, a no-op state store type will be used. Otherwise, the method
   * looks for state store type in the following priority order:</p>
   * <ol>
   *   <li>{@link ConfigurationKeys#DATASET_STATE_STORE_TYPE_KEY}</li>
   *   <li>{@link ConfigurationKeys#STATE_STORE_TYPE_KEY}</li>
   *   <li>{@link ConfigurationKeys#DEFAULT_STATE_STORE_TYPE}</li>
   * </ol>
   *
   * @param jobConfig the job configuration containing state store settings and type information.
   *                  Must not be null.
   * @return a configured {@link DatasetStateStore} instance ready for use
   * @throws IOException if there's an error creating the state store, including:
   *         <ul>
   *           <li>Class resolution failures</li>
   *           <li>Factory instantiation errors</li>
   *           <li>State store creation failures</li>
   *         </ul>
   * @throws RuntimeException if there's a runtime error during state store initialization,
   *         which will be logged and re-thrown
   */
  public static DatasetStateStore<? extends State> createStateStore(Config jobConfig)
      throws IOException {
    boolean stateStoreEnabled = !jobConfig.hasPath(ConfigurationKeys.STATE_STORE_ENABLED) || jobConfig
        .getBoolean(ConfigurationKeys.STATE_STORE_ENABLED);

    String stateStoreType;

    if (!stateStoreEnabled) {
      stateStoreType = ConfigurationKeys.STATE_STORE_TYPE_NOOP;
    } else {
      stateStoreType = ConfigUtils.getString(jobConfig, ConfigurationKeys.DATASET_STATE_STORE_TYPE_KEY, ConfigUtils
          .getString(jobConfig, ConfigurationKeys.STATE_STORE_TYPE_KEY, ConfigurationKeys.DEFAULT_STATE_STORE_TYPE));
    }

    ClassAliasResolver<DatasetStateStore.Factory> resolver = new ClassAliasResolver<>(DatasetStateStore.Factory.class);

    try {
      DatasetStateStore.Factory stateStoreFactory = resolver.resolveClass(stateStoreType).newInstance();
      return stateStoreFactory.createStateStore(jobConfig);
    } catch (RuntimeException e) {
      log.error("Error in initializing DataStateStore of type {} ", stateStoreType, e);
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
