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

import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
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

  public static final String CONFIG_STORE_ROOT = "config.store.root";
  public static final String CONFIG_STORE_REPLICATION_ROOT = "config.store.replication.root";
  public static final String CONFIG_STORE_REPLICATION_TAG = "config.store.replication.tag";
  public static final String CONFIG_STORE_REPLICATION_DISABLE_TAG = "config.store.replication.disable.tag";

  private final String storeRoot;
  private final Path replicationRootPath;
  private final Path replicationTag;
  private final Optional<Path> replicationDisableTag;
  private final ConfigClient configClient;

  public ConfigBasedDatasetsFinder(Properties props) throws IOException {
    Preconditions.checkArgument(props.containsKey(CONFIG_STORE_ROOT),
        "missing required config entery " + CONFIG_STORE_ROOT);
    Preconditions.checkArgument(props.containsKey(CONFIG_STORE_REPLICATION_ROOT),
        "missing required config entery " + CONFIG_STORE_REPLICATION_ROOT);
    Preconditions.checkArgument(props.containsKey(CONFIG_STORE_REPLICATION_TAG),
        "missing required config entery " + CONFIG_STORE_REPLICATION_TAG);

    this.storeRoot = props.getProperty(CONFIG_STORE_ROOT);
    this.replicationRootPath =
        PathUtils.mergePaths(new Path(this.storeRoot), new Path(props.getProperty(CONFIG_STORE_REPLICATION_ROOT)));
    this.replicationTag =
        PathUtils.mergePaths(new Path(this.storeRoot), new Path(props.getProperty(CONFIG_STORE_REPLICATION_TAG)));
    this.replicationDisableTag = props.containsKey(CONFIG_STORE_REPLICATION_DISABLE_TAG) ? Optional.of(PathUtils
        .mergePaths(new Path(this.storeRoot), new Path(props.getProperty(CONFIG_STORE_REPLICATION_DISABLE_TAG))))
        : Optional.absent();

    configClient = ConfigClient.createConfigClient(VersionStabilityPolicy.WEAK_LOCAL_STABILITY);
  }

  protected static Set<URI> getValidDatasetURIs(Collection<URI> allDatasetURIs, Collection<URI> disabledURIs, Path ancestor) {
    if (allDatasetURIs == null || allDatasetURIs.isEmpty()) {
      return ImmutableSet.of();
    }

    Comparator<URI> pathLengthComparator = new Comparator<URI>() {
      public int compare(URI c1, URI c2) {
        return c1.getPath().length() - c2.getPath().length();
      }
    };

    List<URI> datasetsList = new ArrayList<URI>(allDatasetURIs);

    // sort the URI based on the path length to make sure the parent path appear before children
    Collections.sort(datasetsList, pathLengthComparator);

    Set<URI> disabledSet = new HashSet<URI>(disabledURIs);
    Set<URI> validURISet = new HashSet<URI>();

    // remove non leaf URI
    for (URI u : datasetsList) {
      URI needToRemove = null;

      // valid dataset URI
      if (PathUtils.isAncestor(ancestor, new Path(u.getPath()))) {
        
        for (URI leaf : validURISet) {
          if (PathUtils.isAncestor(new Path(leaf.getPath()), new Path(u.getPath()))) {
            // due to the sort, at most one element need to be removed from leafDatasets
            needToRemove = leaf;
            break;
          }
        }

        if (needToRemove != null) {
          validURISet.remove(needToRemove);
        }
        validURISet.add(u);
      }
    }
    
    // remove disabled URIs
    for(URI disable: disabledSet){
      if (validURISet.remove(disable) ){
        log.info("skip disabled dataset " + disable );
      }
    }
    return validURISet;
  }

  /**
   * Based on the {@link #replicationTag}, find all URI which imports the tag. Then filter out
   * 
   * 1. disabled dataset URI
   * 2. None leaf dataset URI
   * 
   * Then created {@link ConfigBasedDataset} based on the {@link Config} of the URIs
   */
  @Override
  public List<ConfigBasedDataset> findDatasets() throws IOException {
    Collection<URI> allDatasetURIs;
    Collection<URI> disabledURIs;
    try {
      // get all the URIs which imports {@link #replicationTag}
      allDatasetURIs = configClient.getImportedBy(new URI(replicationTag.toString()), true);
      
      disabledURIs = this.replicationDisableTag.isPresent()? configClient.getImportedBy(new URI(this.replicationDisableTag.get().toString()), true)
          : ImmutableList.of();
      
    } catch (VersionDoesNotExistException | ConfigStoreFactoryDoesNotExistsException | ConfigStoreCreationException
        | URISyntaxException e) {
      log.error("Caught error while getting all the datasets URIs " + e.getMessage());
      throw new RuntimeException(e);
    }

    Set<URI> leafDatasets = getValidDatasetURIs(allDatasetURIs, disabledURIs, this.replicationRootPath);
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
    return this.replicationRootPath;
  }
}
