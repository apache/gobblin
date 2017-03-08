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

package gobblin.metastore;

import java.io.IOException;
import java.util.Map;

import com.google.common.base.Optional;
import com.typesafe.config.Config;

import gobblin.configuration.State;


public interface DatasetStateStore<T extends State> extends StateStore<T> {
  String DATASET_STATE_STORE_TABLE_SUFFIX = ".jst";

  interface Factory {
    <T extends State> DatasetStateStore<T> createStateStore(Config config);
  }

  Map<String, T> getLatestDatasetStatesByUrns(String jobName) throws IOException;

  T getLatestDatasetState(String storeName, String datasetUrn) throws IOException;

  Map<Optional<String>, String> getLatestDatasetStateTablesByUrn(String jobName) throws IOException;

  void persistDatasetState(String datasetUrn, T datasetState) throws IOException;
}
