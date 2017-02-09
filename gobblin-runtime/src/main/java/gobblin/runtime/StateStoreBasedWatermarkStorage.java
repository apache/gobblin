/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package gobblin.runtime;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.metastore.StateStore;
import gobblin.source.extractor.CheckpointableWatermark;
import gobblin.util.ClassAliasResolver;
import gobblin.util.io.GsonInterfaceAdapter;
import gobblin.writer.WatermarkStorage;


/**
 * A Watermark storage implementation that can use any {@link StateStore} for persistence.
 */
@Slf4j
public class StateStoreBasedWatermarkStorage implements WatermarkStorage {

  private static final Gson GSON = GsonInterfaceAdapter.getGson(Object.class);

  public static final String WATERMARK_STORAGE_TYPE_KEY ="streaming.watermarkStateStore.type";
  public static final String WATERMARK_STORAGE_TYPE_DEFAULT ="zk";
  public static final String WATERMARK_STORAGE_CONFIG_PREFIX="streaming.watermarkStateStore.config.";
  private static final String WATERMARK_STORAGE_PREFIX="streamingWatermarks:";

  public final StateStore<CheckpointableWatermarkState> _stateStore;
  private final String _storeName;

  /**
   * A private method that creates a state store config
   * @return a filled out config that can be passed on to a state store.
   */
  Config getStateStoreConfig(State state) {
    // Select and prefix-strip all properties prefixed by WATERMARK_STORAGE_CONFIG_PREFIX
    Properties properties = state.getProperties();
    for (String key : properties.stringPropertyNames())  {
      if (key.startsWith(WATERMARK_STORAGE_CONFIG_PREFIX)) {
        properties.setProperty(key.substring(WATERMARK_STORAGE_CONFIG_PREFIX.length()),
            (String) properties.get(key));
      }
    }

    Config config = ConfigFactory.parseProperties(properties);

    // Defaults
    if (!config.hasPath(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY)) {
      config = config.withValue(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY,
          ConfigValueFactory.fromAnyRef("/streamingWatermarks"));
    }
    return config;
  }

  public StateStoreBasedWatermarkStorage(State taskState) {
    Preconditions.checkArgument(taskState != null);
    Preconditions.checkArgument(!taskState.getProp(ConfigurationKeys.JOB_NAME_KEY).isEmpty());
    String watermarkStateStoreType = taskState.getProp(WATERMARK_STORAGE_TYPE_KEY, WATERMARK_STORAGE_TYPE_DEFAULT);
    ClassAliasResolver<StateStore.Factory> resolver =
        new ClassAliasResolver<>(StateStore.Factory.class);
    StateStore.Factory stateStoreFactory;

    try {
      stateStoreFactory = resolver.resolveClass(watermarkStateStoreType).newInstance();
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException(cnfe);
    } catch (InstantiationException ie) {
      throw new RuntimeException(ie);
    } catch (IllegalAccessException iae) {
      throw new RuntimeException(iae);
    }

    Config config = getStateStoreConfig(taskState);
    _stateStore = stateStoreFactory.createStateStore(config, CheckpointableWatermarkState.class);
    _storeName = WATERMARK_STORAGE_PREFIX + taskState.getProp(ConfigurationKeys.JOB_NAME_KEY);
    log.info("State Store directory configured as : {}", config.getString(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY));
    log.info("Configured the StateStoreBasedWatermarkStorage with storeName: {}", _storeName);
  }

  @Override
  public void commitWatermarks(Iterable<CheckpointableWatermark> watermarks)
      throws IOException {
    for (CheckpointableWatermark watermark: watermarks) {
      String tableName = watermark.getSource();
      _stateStore.put(_storeName, tableName, new CheckpointableWatermarkState(watermark, GSON));
    }
  }


  @Override
  public Map<String, CheckpointableWatermark> getCommittedWatermarks(Class<? extends CheckpointableWatermark> watermarkClass,
      Iterable<String> sourcePartitions)
      throws IOException {
    Map<String, CheckpointableWatermark> committed = new HashMap<String, CheckpointableWatermark>();
    for (String sourcePartition: sourcePartitions) {
      CheckpointableWatermarkState watermarkState = _stateStore.get(_storeName, sourcePartition, sourcePartition);
      if (watermarkState != null) {
        CheckpointableWatermark watermark = GSON.fromJson(watermarkState.getProp(sourcePartition), watermarkClass);
        committed.put(sourcePartition, watermark);
      }
    }
    if (committed.isEmpty()) {
      log.warn("Didn't find any committed watermarks");
    }
    return committed;
  }

  Iterable<CheckpointableWatermarkState> getAllCommittedWatermarks() throws IOException {
    return _stateStore.getAll(_storeName);
  }

}
