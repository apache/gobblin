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

package gobblin.metastore;

import gobblin.configuration.State;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public interface DatasetStateStore<T extends State> extends StateStore<T> {
  String DATASET_STATE_STORE_TABLE_SUFFIX = ".jst";
  String CURRENT_DATASET_STATE_FILE_SUFFIX = "current";

  interface Factory {
    <T extends State> DatasetStateStore<T> createStateStore(Properties props);
  }

  public Map<String, T> getLatestDatasetStatesByUrns(String jobName) throws IOException;

  public T getLatestDatasetState(String storeName, String datasetUrn) throws IOException;

  public void persistDatasetState(String datasetUrn, T datasetState) throws IOException;
}
