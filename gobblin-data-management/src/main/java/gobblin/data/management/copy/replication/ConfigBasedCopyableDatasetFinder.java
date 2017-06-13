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



import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.fs.FileSystem;

import com.typesafe.config.Config;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import gobblin.config.client.ConfigClient;
import gobblin.dataset.Dataset;
import gobblin.data.management.copy.CopySource;
import gobblin.util.Either;
import gobblin.util.ExecutorsUtils;
import gobblin.util.executors.IteratorExecutor;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConfigBasedCopyableDatasetFinder extends ConfigBasedDatasetsFinder {

  private final int threadPoolSize;
  public ConfigBasedCopyableDatasetFinder(FileSystem fs, Properties jobProps) throws IOException{
    super(fs, jobProps);
    this.threadPoolSize = jobProps.containsKey(CopySource.MAX_CONCURRENT_LISTING_SERVICES)
        ? Integer.parseInt(jobProps.getProperty(CopySource.MAX_CONCURRENT_LISTING_SERVICES))
        : CopySource.DEFAULT_MAX_CONCURRENT_LISTING_SERVICES;
  }

  /**
   * Based on the {@link #whitelistTag}, find all URI which imports the tag. Then filter out
   *
   * 1. disabled dataset URI
   * 2. None leaf dataset URI
   *
   * Then created {@link ConfigBasedDataset} based on the {@link Config} of the URIs
   */
  @Override
  public List<Dataset> findDatasets() throws IOException {
    Set<URI> leafDatasets = getValidDatasetURIs(this.commonRoot);
    if (leafDatasets.isEmpty()) {
      return ImmutableList.of();
    }

    // Parallel execution for copyDataset for performance consideration.
    final List<Dataset> result = new CopyOnWriteArrayList<>();
    Iterator<Callable<Void>> callableIterator =
        Iterators.transform(leafDatasets.iterator(), new Function<URI, Callable<Void>>() {
          @Override
          public Callable<Void> apply(final URI datasetURI) {
            return findDatasetsCallable(configClient, datasetURI, props, result);
          }
        });

    this.executeItertorExecutor(callableIterator);
    log.info("found {} datasets in ConfigBasedDatasetsFinder", result.size());
    return result;
  }

  private void executeItertorExecutor(Iterator<Callable<Void>> callableIterator) throws IOException {
    try {
      IteratorExecutor<Void> executor = new IteratorExecutor<>(callableIterator, this.threadPoolSize,
          ExecutorsUtils.newDaemonThreadFactory(Optional.of(log), Optional.of(this.getClass().getSimpleName())));
      List<Either<Void, ExecutionException>> results = executor.executeAndGetResults();
      IteratorExecutor.logFailures(results, log, 10);
    } catch (InterruptedException ie) {
      throw new IOException("Dataset finder is interrupted.", ie);
    }
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
