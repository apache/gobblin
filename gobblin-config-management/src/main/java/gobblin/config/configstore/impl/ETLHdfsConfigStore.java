package gobblin.config.configstore.impl;

import gobblin.config.configstore.ConfigStoreWithStableVersion;
import gobblin.config.configstore.VersionFinder;

import java.net.URI;
import java.util.Collection;
import java.util.Collections;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


/**
 * ETLHdfsConfigStore is used for ETL configuration dataset management
 * @author mitu
 *
 */
public class ETLHdfsConfigStore extends HdfsConfigStoreWithOwnInclude implements ConfigStoreWithStableVersion {

  public final static String DATASET_PREFIX = "datasets";
  public final static String TAG_PREFIX = "tags";
  public final static String ID_DELEMETER = "/";
  
  public ETLHdfsConfigStore(URI physical_root, URI logic_root) {
    this(physical_root, logic_root, new SimpleVersionFinder());
  }

  public ETLHdfsConfigStore(URI physical_root, URI logic_root, VersionFinder<String> vc) {
    super(physical_root, logic_root, vc);
  }

  @Override
  public Collection<URI> getChildren(URI uri, String version) {
    if (isValidURI(uri)) {
      return super.getChildren(uri, version);
    }
    return Collections.emptyList();
  }

  @Override
  public Collection<URI> getOwnImports(URI uri, String version) {
    if (!isValidURI(uri)) {
      return Collections.emptyList();
    }

    Collection<URI> superRes = super.getOwnImports(uri, version);
    for (URI i : superRes) {
      // can not import datasets
      if (isValidDataset(i)) {
        throw new RuntimeException(String.format("URI %s Can not import dataset %s", uri.toString(), i.toString()));
      }
    }

    return superRes;
  }

  @Override
  public Config getOwnConfig(URI uri, String version) {
    if (isValidURI(uri)) {
      return super.getOwnConfig(uri, version);
    }
    return ConfigFactory.empty();
  }

  /**
   * 
   * @param uri - should be relative to store
   * @return
   */
  protected static final boolean isValidURI(URI uri) {
    return isRootURI(uri) || isValidTag(uri) || isValidDataset(uri);
  }

  protected static final boolean isValidTag(URI uri) {
    if (uri == null)
      return false;

    if (uri.toString().equals(TAG_PREFIX) || uri.toString().startsWith(TAG_PREFIX + ID_DELEMETER))
      return true;

    return false;
  }

  protected static final boolean isValidDataset(URI uri) {
    if (uri == null)
      return false;

    if (uri.toString().equals(DATASET_PREFIX) || uri.toString().startsWith(DATASET_PREFIX + ID_DELEMETER))
      return true;

    return false;
  }
  
}
