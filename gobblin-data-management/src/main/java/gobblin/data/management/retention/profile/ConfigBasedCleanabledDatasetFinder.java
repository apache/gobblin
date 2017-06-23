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
package gobblin.data.management.retention.profile;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.typesafe.config.Config;

import gobblin.dataset.Dataset;
import gobblin.data.management.copy.replication.ConfigBasedDatasetsFinder;
import gobblin.data.management.retention.dataset.ConfigurableCleanableDataset;
import gobblin.config.client.ConfigClient;
import gobblin.configuration.ConfigurationKeys;

import lombok.extern.slf4j.Slf4j;


/**
 * Based on the ConfigStore object to find all {@link ConfigurableCleanableDataset}
 * Specifically for Retention job.
 */
@Slf4j
public class ConfigBasedCleanabledDatasetFinder extends ConfigBasedDatasetsFinder{

  public FileSystem fileSystem;
  public static final String DATASET_PATH = ConfigurationKeys.CONFIG_BASED_PREFIX + ".fullDatasetPath";

  public ConfigBasedCleanabledDatasetFinder(FileSystem fs, Properties jobProps) throws IOException{
    super(fs, jobProps);
    fileSystem = fs;
  }

  protected Callable<Void> findDatasetsCallable(final ConfigClient confClient,
      final URI u, final Properties p, final Collection<Dataset> datasets) {
    return new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        // Process each {@link Config}, find dataset and add those into the datasets
        Config c = confClient.getConfig(u);
        Dataset datasetForConfig =
            new ConfigurableCleanableDataset(fileSystem, p, new Path(c.getString(DATASET_PATH)), c, log);
        datasets.add(datasetForConfig);
        return null;
      }
    };
  }
}
