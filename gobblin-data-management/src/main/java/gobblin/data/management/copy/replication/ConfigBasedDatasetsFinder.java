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
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.typesafe.config.Config;

import gobblin.config.client.ConfigClient;
import gobblin.config.client.api.ConfigStoreFactoryDoesNotExistsException;
import gobblin.config.client.api.VersionStabilityPolicy;
import gobblin.config.store.api.ConfigStoreCreationException;
import gobblin.config.store.api.VersionDoesNotExistException;
import gobblin.configuration.ConfigurationKeys;
import gobblin.dataset.DatasetsFinder;
import gobblin.util.PathUtils;
import gobblin.data.management.copy.CopyConfiguration;
import gobblin.data.management.copy.CopySource;
import gobblin.dataset.Dataset;
import gobblin.util.Either;
import gobblin.util.ExecutorsUtils;
import gobblin.util.executors.IteratorExecutor;
import lombok.extern.slf4j.Slf4j;


/**
 * Based on the configuration store to find all {@link ConfigBasedDataset}
 * @author mitu
 *
 */

@Slf4j
public abstract class ConfigBasedDatasetsFinder implements DatasetsFinder {

  // specify the whitelist tag in the config store used by data replication or data retention
  // the datasets which import this tag will be processed by data replication or data retention
  public static final String GOBBLIN_CONFIG_STORE_WHITELIST_TAG =
      ConfigurationKeys.CONFIG_BASED_PREFIX + ".whitelist.tag";

  // specify the blacklist tags in the config store used by data replication or data retention
  // the datasets which import these tags will NOT be processed by data replication or data retention
  // and blacklist override the whitelist
  public static final String GOBBLIN_CONFIG_STORE_BLACKLIST_TAGS =
      ConfigurationKeys.CONFIG_BASED_PREFIX + ".blacklist.tags";

  // specify the common root for all the datasets which will be processed by data replication/data retention
  public static final String GOBBLIN_CONFIG_STORE_DATASET_COMMON_ROOT =
      ConfigurationKeys.CONFIG_BASED_PREFIX + ".dataset.common.root";

  // In addition to the white/blacklist tags, this configuration let the user to black/whitelist some datasets
  // in the job-level configuration, which is not in configStore
  // as to have easier approach to black/whitelist some datasets.
  // The semantics keep still as tag, which the blacklist override whitelist if any dataset in common.
  public static final String JOB_LEVEL_BLACKLIST = CopyConfiguration.COPY_PREFIX + ".configBased.blacklist" ;
  public static final String JOB_LEVEL_WHITELIST = CopyConfiguration.COPY_PREFIX + ".configBased.whitelist" ;

  // There are some cases that WATERMARK checking is desired, like
  // Unexpected data loss on target while not changing watermark accordingly.
  // This configuration make WATERMARK checking configurable for operation convenience, default true
  public static final String WATERMARK_ENABLE = CopyConfiguration.COPY_PREFIX + ".configBased.watermark.enabled" ;


  protected final String storeRoot;
  protected final Path commonRoot;
  protected final Path whitelistTag;
  protected final Optional<List<Path>> blacklistTags;
  protected final ConfigClient configClient;
  protected final Properties props;
  private final int threadPoolSize;

  private Optional<List<String>> blacklistURNs;
  private Optional<List<String>> whitelistURNs;


  public ConfigBasedDatasetsFinder(FileSystem fs, Properties jobProps) throws IOException {
    // ignore the input FileSystem , the source file system could be different for different datasets

    Preconditions.checkArgument(jobProps.containsKey(ConfigurationKeys.CONFIG_MANAGEMENT_STORE_URI),
        "missing required config entery " + ConfigurationKeys.CONFIG_MANAGEMENT_STORE_URI);
    Preconditions.checkArgument(jobProps.containsKey(GOBBLIN_CONFIG_STORE_WHITELIST_TAG),
        "missing required config entery " + GOBBLIN_CONFIG_STORE_WHITELIST_TAG);
    Preconditions.checkArgument(jobProps.containsKey(GOBBLIN_CONFIG_STORE_DATASET_COMMON_ROOT),
        "missing required config entery " + GOBBLIN_CONFIG_STORE_DATASET_COMMON_ROOT);

    this.storeRoot = jobProps.getProperty(ConfigurationKeys.CONFIG_MANAGEMENT_STORE_URI);
    this.commonRoot = PathUtils.mergePaths(new Path(this.storeRoot),
        new Path(jobProps.getProperty(GOBBLIN_CONFIG_STORE_DATASET_COMMON_ROOT)));
    this.whitelistTag = PathUtils.mergePaths(new Path(this.storeRoot),
        new Path(jobProps.getProperty(GOBBLIN_CONFIG_STORE_WHITELIST_TAG)));
    this.threadPoolSize = jobProps.containsKey(CopySource.MAX_CONCURRENT_LISTING_SERVICES)
        ? Integer.parseInt(jobProps.getProperty(CopySource.MAX_CONCURRENT_LISTING_SERVICES))
        : CopySource.DEFAULT_MAX_CONCURRENT_LISTING_SERVICES;


    if (jobProps.containsKey(GOBBLIN_CONFIG_STORE_BLACKLIST_TAGS)) {
      List<String> disableStrs = Splitter.on(",").omitEmptyStrings()
          .splitToList(jobProps.getProperty(GOBBLIN_CONFIG_STORE_BLACKLIST_TAGS));
      List<Path> disablePaths = new ArrayList<Path>();
      for (String s : disableStrs) {
        disablePaths.add(PathUtils.mergePaths(new Path(this.storeRoot), new Path(s)));
      }
      this.blacklistTags = Optional.of(disablePaths);
    } else {
      this.blacklistTags = Optional.absent();
    }

    configClient = ConfigClient.createConfigClient(VersionStabilityPolicy.WEAK_LOCAL_STABILITY);
    this.props = jobProps;


    if (props.containsKey(JOB_LEVEL_BLACKLIST)) {
      this.blacklistURNs = Optional.of(Splitter.on(",").omitEmptyStrings().splitToList(props.getProperty(JOB_LEVEL_BLACKLIST)));
    } else {
      this.blacklistURNs = Optional.absent();
    }

    if (props.containsKey(JOB_LEVEL_WHITELIST)) {
      this.whitelistURNs = Optional.of(Splitter.on(",").omitEmptyStrings().splitToList(props.getProperty(JOB_LEVEL_WHITELIST)));
    } else {
      this.whitelistURNs = Optional.absent();
    }
  }

  protected Set<URI> getValidDatasetURIs(Path datasetCommonRoot) {
    Collection<URI> allDatasetURIs;
    Set<URI> disabledURISet = new HashSet();
    if (this.blacklistURNs.isPresent()) {
      for(String urn : this.blacklistURNs.get()) {
        disabledURISet.add(this.datasetURNtoURI(urn));
      }
    }

    try {
      // get all the URIs which imports {@link #replicationTag} or all from whitelistURNs
      allDatasetURIs = this.whitelistURNs.isPresent()
          ? this.whitelistURNs.get().stream().map(u -> this.datasetURNtoURI(u)).collect(Collectors.toList()) : configClient.getImportedBy(new URI(whitelistTag.toString()), true);
      populateDisabledURIs(disabledURISet);
    } catch ( ConfigStoreFactoryDoesNotExistsException | ConfigStoreCreationException
        | URISyntaxException e) {
      log.error("Caught error while getting all the datasets URIs " + e.getMessage());
      throw new RuntimeException(e);
    }
    return getValidDatasetURIs(allDatasetURIs, disabledURISet, datasetCommonRoot);
  }

  /**
   * Extended signature for testing convenience.
   */
  protected static Set<URI> getValidDatasetURIs(Collection<URI> allDatasetURIs, Set<URI> disabledURISet, Path datasetCommonRoot){
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
      } else {
        log.info("There's no URI " + disable + " available in validURISet.");
      }
    }
    return validURISet;
  }

  private void populateDisabledURIs(Set<URI> disabledURIs) throws
                                                           URISyntaxException,
                                                           ConfigStoreFactoryDoesNotExistsException,
                                                           ConfigStoreCreationException,
                                                           VersionDoesNotExistException {
    if (this.blacklistTags.isPresent()) {
      disabledURIs = new HashSet<URI>();
      for (Path s : this.blacklistTags.get()) {
        disabledURIs.addAll(configClient.getImportedBy(new URI(s.toString()), true));
      }
    }
  }

  @Override
  public Path commonDatasetRoot() {
    return this.commonRoot;
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

  protected void executeItertorExecutor(Iterator<Callable<Void>> callableIterator) throws IOException {
    try {
      IteratorExecutor<Void> executor = new IteratorExecutor<>(callableIterator, this.threadPoolSize,
          ExecutorsUtils.newDaemonThreadFactory(Optional.of(log), Optional.of(this.getClass().getSimpleName())));
      List<Either<Void, ExecutionException>> results = executor.executeAndGetResults();
      IteratorExecutor.logFailures(results, log, 10);
    } catch (InterruptedException ie) {
      throw new IOException("Dataset finder is interrupted.", ie);
    }
  }

  /**
   * Helper funcition for converting datasetURN into URI
   */
  private URI datasetURNtoURI(String datasetURN) {
    try {
      return new URI(PathUtils.mergePaths(new Path(this.storeRoot), new Path(datasetURN)).toString());
    }catch (URISyntaxException e) {
      log.error("Dataset with URN:" + datasetURN + " cannot be converted into URI. Skip the dataset");
      return null;
    }
  }

  protected abstract Callable<Void> findDatasetsCallable(final ConfigClient confClient,
      final URI u, final Properties p, final Collection<Dataset> datasets);
}
