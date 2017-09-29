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

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.metastore.DatasetStateStore;
import org.apache.gobblin.runtime.util.MigrationCliOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;


/**
 * A script used for state store migration:
 * In the case that users are willing to change the storage medium of job state due to some reasons.
 *
 * Current implementation doesn't support data awareness on either source or target side.
 * And only migrate a single job state instead of migrating all history versions.
 */
@Slf4j
public class StateStoreMigrationToolkit {
  private static String SOURCE_KEY = "source";
  private static String DESTINATION_KEY = "destination";
  private static String JOB_NAME_KEY = "jobName";

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    String[] genericCmdLineOpts = new GenericOptionsParser(new Configuration(), args).getCommandLine().getArgs();
    Config config = MigrationCliOptions.parseArgs(StateStoreMigrationToolkit.class, genericCmdLineOpts);

    Preconditions.checkNotNull(config.getObject(SOURCE_KEY));
    Preconditions.checkNotNull(config.getObject(DESTINATION_KEY));
    Preconditions.checkNotNull(config.getString(JOB_NAME_KEY));

    DatasetStateStore dstDatasetStateStore =
        DatasetStateStore.buildDatasetStateStore(config.getConfig(DESTINATION_KEY));
    DatasetStateStore srcDatasetStateStore = DatasetStateStore.buildDatasetStateStore(config.getConfig(SOURCE_KEY));

    Map<String, JobState.DatasetState> map =
        srcDatasetStateStore.getLatestDatasetStatesByUrns(config.getString(JOB_NAME_KEY));
    for (String key : map.keySet()) {
      dstDatasetStateStore.persistDatasetState(key, map.get(key));
    }
  }
}
