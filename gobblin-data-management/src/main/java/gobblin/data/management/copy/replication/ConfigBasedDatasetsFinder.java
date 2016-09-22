package gobblin.data.management.copy.replication;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import gobblin.config.client.ConfigClient;
import gobblin.config.client.api.ConfigStoreFactoryDoesNotExistsException;
import gobblin.config.client.api.VersionStabilityPolicy;
import gobblin.config.store.api.ConfigStoreCreationException;
import gobblin.config.store.api.VersionDoesNotExistException;
import gobblin.dataset.DatasetsFinder;
import gobblin.util.PathUtils;
import lombok.extern.slf4j.Slf4j;


/**
 * Based on the configuration store to find all {@link ConfigBasedDataset}
 * @author mitu
 *
 */

@Slf4j
public class ConfigBasedDatasetsFinder implements DatasetsFinder<ConfigBasedDataset> {

  // prefix key for config store used by data replication
  public static final String GOBBLIN_REPLICATION_CONFIG_STORE = "gobblin.replication.configStore";

  // specify the config store root used by data replication
  public static final String GOBBLIN_REPLICATION_CONFIG_STORE_ROOT = GOBBLIN_REPLICATION_CONFIG_STORE + ".root";

  // specify the whitelist tag in the config store used by data replication
  // the datasets which import this tag will be processed by data replication
  public static final String GOBBLIN_REPLICATION_CONFIG_STORE_WHITELIST_TAG =
      GOBBLIN_REPLICATION_CONFIG_STORE + "whitelist.tag";

  // specify the blacklist tags in the config store used by data replication
  // the datasets which import these tags will NOT be processed by data replication 
  // and blacklist override the whitelist 
  public static final String GOBBLIN_REPLICATION_CONFIG_STORE_BLACKLIST_TAGS =
      GOBBLIN_REPLICATION_CONFIG_STORE + "blacklist.tags";

  // specify the common root for all the datasets which will be processed by data replication
  public static final String GOBBLIN_REPLICATION_CONFIG_STORE_DATASET_COMMON_ROOT =
      GOBBLIN_REPLICATION_CONFIG_STORE + "dataset.common.root";

  private final String storeRoot;
  private final Path replicationDatasetCommonRoot;
  private final Path whitelistTag;
  private final Optional<List<Path>> blacklistTags;
  private final ConfigClient configClient;

  public ConfigBasedDatasetsFinder(Properties props) throws IOException {
    Preconditions.checkArgument(props.containsKey(GOBBLIN_REPLICATION_CONFIG_STORE_ROOT),
        "missing required config entery " + GOBBLIN_REPLICATION_CONFIG_STORE_ROOT);
    Preconditions.checkArgument(props.containsKey(GOBBLIN_REPLICATION_CONFIG_STORE_DATASET_COMMON_ROOT),
        "missing required config entery " + GOBBLIN_REPLICATION_CONFIG_STORE_DATASET_COMMON_ROOT);
    Preconditions.checkArgument(props.containsKey(GOBBLIN_REPLICATION_CONFIG_STORE_WHITELIST_TAG),
        "missing required config entery " + GOBBLIN_REPLICATION_CONFIG_STORE_WHITELIST_TAG);

    this.storeRoot = props.getProperty(GOBBLIN_REPLICATION_CONFIG_STORE_ROOT);
    this.replicationDatasetCommonRoot = PathUtils.mergePaths(new Path(this.storeRoot),
        new Path(props.getProperty(GOBBLIN_REPLICATION_CONFIG_STORE_DATASET_COMMON_ROOT)));
    this.whitelistTag = PathUtils.mergePaths(new Path(this.storeRoot),
        new Path(props.getProperty(GOBBLIN_REPLICATION_CONFIG_STORE_WHITELIST_TAG)));

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
  public List<ConfigBasedDataset> findDatasets() throws IOException {
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

    List<ConfigBasedDataset> result = new ArrayList<ConfigBasedDataset>();
    for (URI leaf : leafDatasets) {
      try {
        result.add(new ConfigBasedDataset(configClient.getConfig(leaf)));
      } catch (VersionDoesNotExistException | ConfigStoreFactoryDoesNotExistsException
          | ConfigStoreCreationException e) {
        log.error("Caught error while retrieve config for " + leaf + ", skipping.");
      }
    }
    return result;
  }

  @Override
  public Path commonDatasetRoot() {
    return this.replicationDatasetCommonRoot;
  }
}
