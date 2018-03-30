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

package org.apache.gobblin.runtime;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;
import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.DatasetStateStore;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


/**
 * An extension of {@link FsDatasetStateStore} where all operations are noop. Used to disable the state store.
 */
public class NoopDatasetStateStore extends FsDatasetStateStore {

  @Alias("noop")
  public static class Factory implements DatasetStateStore.Factory {
    @Override
    public DatasetStateStore<JobState.DatasetState> createStateStore(Config config) {
      // dummy root dir for noop state store
      Config config2 = config.withValue(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, ConfigValueFactory.fromAnyRef(""));
      return FsDatasetStateStore.createStateStore(config2, NoopDatasetStateStore.class.getName());
    }
  }

  public NoopDatasetStateStore(FileSystem fs, String storeRootDir, Integer threadPoolSize) {
    super(fs, storeRootDir, threadPoolSize);
  }

  public NoopDatasetStateStore(FileSystem fs, String storeRootDir) {
    super(fs, storeRootDir);
  }

  @Override
  public List<JobState.DatasetState> getAll(String storeName, String tableName) throws IOException {
    return Lists.newArrayList();
  }

  @Override
  public List<JobState.DatasetState> getAll(String storeName) throws IOException {
    return Lists.newArrayList();
  }

  @Override
  public Map<String, JobState.DatasetState> getLatestDatasetStatesByUrns(String jobName) throws IOException {
    return Maps.newHashMap();
  }

  @Override
  public void persistDatasetState(String datasetUrn, JobState.DatasetState datasetState) throws IOException {}

  @Override
  public boolean create(String storeName) throws IOException {
    return true;
  }

  @Override
  public boolean create(String storeName, String tableName) throws IOException {
    return true;
  }

  @Override
  public boolean exists(String storeName, String tableName) throws IOException {
    return false;
  }

  @Override
  public void put(String storeName, String tableName, JobState.DatasetState state) throws IOException {}

  @Override
  public void putAll(String storeName, String tableName, Collection<JobState.DatasetState> states) throws IOException {}

  @Override
  public void createAlias(String storeName, String original, String alias) throws IOException {}

  @Override
  public void delete(String storeName, String tableName) throws IOException {}

  @Override
  public void delete(String storeName) throws IOException {}
}
