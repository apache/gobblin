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

package gobblin.runtime;

import com.typesafe.config.Config;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.typesafe.config.ConfigFactory;

import gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import gobblin.broker.SharedResourcesBrokerFactory;
import gobblin.commit.CommitSequenceStore;
import gobblin.commit.DeliverySemantics;
import gobblin.metastore.JobHistoryStore;
import gobblin.source.Source;


public class DummyJobContext extends JobContext {

  private final Map<String, JobState.DatasetState> datasetStateMap;

  public DummyJobContext(Properties jobProps, Logger logger, Map<String, JobState.DatasetState> datasetStateMap)
      throws Exception {
    super(jobProps, logger, SharedResourcesBrokerFactory.createDefaultTopLevelBroker(ConfigFactory.empty(),
        GobblinScopeTypes.GLOBAL.defaultScopeInstance()));
    this.datasetStateMap = datasetStateMap;
  }

  @Override
  protected FsDatasetStateStore createStateStore(Config config)
      throws IOException {
    return new NoopDatasetStateStore(FileSystem.getLocal(new Configuration()), "");
  }

  @Override
  protected Optional<JobHistoryStore> createJobHistoryStore(Properties jobProps) {
    return Optional.absent();
  }

  @Override
  protected Optional<CommitSequenceStore> createCommitSequenceStore()
      throws IOException {
    return Optional.absent();
  }

  @Override
  protected Source<?, ?> createSource(Properties jobProps)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    return null;
  }

  @Override
  protected void setTaskStagingAndOutputDirs() {
    // nothing
  }

  @Override
  protected Callable<Void> createSafeDatasetCommit(boolean shouldCommitDataInJob,
      DeliverySemantics deliverySemantics, String datasetUrn, JobState.DatasetState datasetState,
      boolean isMultithreaded, JobContext jobContext) {
    return new Callable<Void>() {
      @Override
      public Void call()
          throws Exception {
        return null;
      }
    };
  }

  @Override
  protected Map<String, JobState.DatasetState> computeDatasetStatesByUrns() {
    return this.datasetStateMap;
  }
}
