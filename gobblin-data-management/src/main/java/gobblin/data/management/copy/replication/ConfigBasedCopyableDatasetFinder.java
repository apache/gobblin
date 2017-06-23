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
package gobblin.data.management.copy.replication;



import gobblin.data.management.retention.dataset.ConfigurableCleanableDataset;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.hadoop.fs.FileSystem;

import com.typesafe.config.Config;

import gobblin.config.client.ConfigClient;
import gobblin.dataset.Dataset;

import lombok.extern.slf4j.Slf4j;

/**
 * Based on the ConfigStore object to find all {@link ConfigBasedMultiDatasets} to replicate.
 * Specifically for replication job.
 * Normal DistcpNG Job which doesn'involve Dataflow concepts should not use this DatasetFinder but
 * different implementation of {@link ConfigBasedDatasetsFinder}. 
 */
@Slf4j
public class ConfigBasedCopyableDatasetFinder extends ConfigBasedDatasetsFinder {

  public ConfigBasedCopyableDatasetFinder(FileSystem fs, Properties jobProps) throws IOException{
    super(fs, jobProps);
  }

  protected Callable<Void> findDatasetsCallable(final ConfigClient confClient,
      final URI u, final Properties p, final Collection<Dataset> datasets) {
    return new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        // Process each {@link Config}, find dataset and add those into the datasets
        Config c = confClient.getConfig(u);
        List<Dataset> datasetForConfig = new ConfigBasedMultiDatasets(c, p).getConfigBasedDatasetList();
        datasets.addAll(datasetForConfig);
        return null;
      }
    };
  }
}
