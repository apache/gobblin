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
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.typesafe.config.Config;

import gobblin.config.client.ConfigClient;
import gobblin.config.client.api.ConfigStoreFactoryDoesNotExistsException;
import gobblin.config.client.api.VersionStabilityPolicy;
import gobblin.config.store.api.ConfigStoreCreationException;
import gobblin.config.store.api.VersionDoesNotExistException;
import gobblin.configuration.ConfigurationKeys;
import gobblin.data.management.copy.CopyConfiguration;
import gobblin.data.management.copy.CopySource;
import gobblin.data.management.copy.CopyableDatasetBase;
import gobblin.dataset.DatasetsFinder;
import gobblin.util.Either;
import gobblin.util.ExecutorsUtils;
import gobblin.util.PathUtils;
import gobblin.util.executors.IteratorExecutor;
import lombok.extern.slf4j.Slf4j;


/**
 * Based on the configuration store to find all {@link ConfigBasedDataset}
 * @author mitu
 *
 */

@Slf4j
public class ConfigBasedDatasetsFinder implements DatasetsFinder<CopyableDatasetBase> {

  // specify the whitelist tag in the config store used by data replication
  // the datasets which import this tag will be processed by data replication
  public static final String GOBBLIN_REPLICATION_CONFIG_STORE_WHITELIST_TAG =
      CopyConfiguration.COPY_PREFIX + ".whitelist.tag";

  // specify the blacklist tags in the config store used by data replication
  // the datasets which import these tags will NOT be processed by data replication
  // and blacklist override the whitelist
  public static final String GOBBLIN_REPLICATION_CONFIG_STORE_BLACKLIST_TAGS =
      CopyConfiguration.COPY_PREFIX + ".blacklist.tags";

  // specify the common root for all the datasets which will be processed by data replication
  public static final String GOBBLIN_REPLICATION_CONFIG_STORE_DATASET_COMMON_ROOT =
      CopyConfiguration.COPY_PREFIX + ".dataset.common.root";

  private final String storeRoot;
  private final Path replicationDatasetCommonRoot;
  private final Path whitelistTag;
  private final Optional<List<Path>> blacklistTags;
  private final ConfigClient configClient;
  private final Properties props;
  private final int threadPoolSize;

  public ConfigBasedDatasetsFinder(FileSystem fs, Properties props) throws IOException {
    // ignore the input FileSystem , the source file system could be different for different datasets

    Preconditions.checkArgument(props.containsKey(ConfigurationKeys.CONFIG_MANAGEMENT_STORE_URI),
        "missing required config entery " + ConfigurationKeys.CONFIG_MANAGEMENT_STORE_URI);
    Preconditions.checkArgument(props.containsKey(GOBBLIN_REPLICATION_CONFIG_STORE_DATASET_COMMON_ROOT),
        "missing required config entery " + GOBBLIN_REPLICATION_CONFIG_STORE_DATASET_COMMON_ROOT);
    Preconditions.checkArgument(props.containsKey(GOBBLIN_REPLICATION_CONFIG_STORE_WHITELIST_TAG),
        "missing required config entery " + GOBBLIN_REPLICATION_CONFIG_STORE_WHITELIST_TAG);

    this.storeRoot = props.getProperty(ConfigurationKeys.CONFIG_MANAGEMENT_STORE_URI);
    this.replicationDatasetCommonRoot = PathUtils.mergePaths(new Path(this.storeRoot),
        new Path(props.getProperty(GOBBLIN_REPLICATION_CONFIG_STORE_DATASET_COMMON_ROOT)));
    this.whitelistTag = PathUtils.mergePaths(new Path(this.storeRoot),
        new Path(props.getProperty(GOBBLIN_REPLICATION_CONFIG_STORE_WHITELIST_TAG)));
    this.threadPoolSize = props.containsKey(CopySource.MAX_CONCURRENT_LISTING_SERVICES)
        ? Integer.parseInt(props.getProperty(CopySource.MAX_CONCURRENT_LISTING_SERVICES))
        : CopySource.DEFAULT_MAX_CONCURRENT_LISTING_SERVICES;

    if (props.containsKey(GOBBLIN_REPLICATION_CONFIG_STORE_BLACKLIST_TAGS)) {
      List<String> disableStrs = Splitter.on(",").omitEmptyStrings()
          .splitToList(props.getProperty(GOBBLIN_REPLICATION_CONFIG_STORE_BLACKLIST_TAGS));
      List<Path> disablePaths = new ArrayList<Path>();
      for (String s : disableStrs) {
        disablePaths.add(PathUtils.mergePaths(new Path(this.storeRoot), new Path(s)));
      }
      this.blacklistTags = Optional.of(disablePaths);
    } else {
      this.blacklistTags = Optional.absent();
    }

    configClient = ConfigClient.createConfigClient(VersionStabilityPolicy.WEAK_LOCAL_STABILITY);
    this.props = props;
  }

  protected static Set<URI> getValidDatasetURIs(Collection<URI> allDatasetURIs, Set<URI> disabledURISet,
      Path datasetCommonRoot) {
    if (allDatasetURIs == null || allDatasetURIs.isEmpty()) {
      return ImmutableSet.of();
    }

    Comparator<URI> pathLengthComparator = new Comparator<URI>() {
      public int compare(URI c1, URI c2) {
        return c1.getPath().length() - c2.getPath().length();
      }
    };

    List<URI> sortedDatasetsList = new ArrayList<URI>(allDatasetURIs);

    // sort the URI based on the path length to make sure the parent path appear before children
    Collections.sort(sortedDatasetsList, pathLengthComparator);

    TreeSet<URI> uriSet = new TreeSet<URI>();
    Set<URI> noneLeaf = new HashSet<URI>();

    for (URI u : sortedDatasetsList) {
      // filter out none common root
      if (PathUtils.isAncestor(datasetCommonRoot, new Path(u.getPath()))) {
        URI floor = uriSet.floor(u);
        // check for ancestor Paths
        if (floor != null && PathUtils.isAncestor(new Path(floor.getPath()), new Path(u.getPath()))) {
          noneLeaf.add(floor);
        }
        uriSet.add(u);
      }
    }

    // only get the leaf nodes
    Set<URI> validURISet = new HashSet<URI>();
    for (URI u : uriSet) {
      if (!noneLeaf.contains(u)) {
        validURISet.add(u);
      }
    }

    // remove disabled URIs
    for (URI disable : disabledURISet) {
      if (validURISet.remove(disable)) {
        log.info("skip disabled dataset " + disable);
      }
    }
    return validURISet;
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
  public List<CopyableDatasetBase> findDatasets() throws IOException {
    Collection<URI> allDatasetURIs;
    Set<URI> disabledURIs = ImmutableSet.of();
    try {
      // get all the URIs which imports {@link #replicationTag}
      allDatasetURIs = configClient.getImportedBy(new URI(whitelistTag.toString()), true);

      if (this.blacklistTags.isPresent()) {
        disabledURIs = new HashSet<URI>();
        for (Path s : this.blacklistTags.get()) {
          disabledURIs.addAll(configClient.getImportedBy(new URI(s.toString()), true));
        }
      }
    } catch (VersionDoesNotExistException | ConfigStoreFactoryDoesNotExistsException | ConfigStoreCreationException
        | URISyntaxException e) {
      log.error("Caught error while getting all the datasets URIs " + e.getMessage());
      throw new RuntimeException(e);
    }

    Set<URI> leafDatasets = getValidDatasetURIs(allDatasetURIs, disabledURIs, this.replicationDatasetCommonRoot);
    if (leafDatasets.isEmpty()) {
      return ImmutableList.of();
    }

    final List<CopyableDatasetBase> result = new CopyOnWriteArrayList<>();

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

  @Override
  public Path commonDatasetRoot() {
    return this.replicationDatasetCommonRoot;
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

  protected Callable<Void> findDatasetsCallable(final ConfigClient confClient, final URI u, final Properties p,
      final Collection<CopyableDatasetBase> datasets) {
    return new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        // Process each {@link Config}, find dataset and add those into the datasets
        Config c = confClient.getConfig(u);
        List<ConfigBasedDataset> datasetForConfig = new ConfigBasedMultiDatasets(c, p).getConfigBasedDatasetList();
        datasets.addAll(datasetForConfig);
        return null;
      }
    };
  }
}
