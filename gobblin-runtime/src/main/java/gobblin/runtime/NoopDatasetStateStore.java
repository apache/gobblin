/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime;

import gobblin.annotation.Alias;
import gobblin.configuration.ConfigurationKeys;
import gobblin.metastore.DatasetStateStore;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
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
    public DatasetStateStore<JobState.DatasetState> createStateStore(Properties props) {
      return FsDatasetStateStore.createStateStore(props, NoopDatasetStateStore.class.getName());
    }
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
