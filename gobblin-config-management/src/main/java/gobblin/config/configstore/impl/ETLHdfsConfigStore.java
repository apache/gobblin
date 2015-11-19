package gobblin.config.configstore.impl;

import gobblin.config.configstore.ConfigStoreWithImportedBy;
import gobblin.config.configstore.ConfigStoreWithImportedByRecursively;
import gobblin.config.configstore.ConfigStoreWithResolution;
import gobblin.config.configstore.ConfigStoreWithStableVersion;
import gobblin.config.configstore.ImportMappings;
import gobblin.config.configstore.VersionComparator;
import gobblin.config.utils.CircularDependencyChecker;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


/**
 * ETLHdfsConfigStore is used for ETL configuration dataset management
 * @author mitu
 *
 */
public class ETLHdfsConfigStore extends HdfsConfigStoreWithOwnInclude implements ConfigStoreWithResolution,
    ConfigStoreWithImportedBy, ConfigStoreWithImportedByRecursively, ConfigStoreWithStableVersion {

  public final static String DATASET_PREFIX = "datasets";
  public final static String TAG_PREFIX = "tags";
  public final static String ID_DELEMETER = "/";
  
  private final Map<String,ImportMappings> im_map = new HashMap<String, ImportMappings>() ;
  
  public ETLHdfsConfigStore(URI root) {
    this(root, new SimpleVersionComparator());
  }

  public ETLHdfsConfigStore(URI root, VersionComparator<String> vc) {
    super(root, vc);
  }

  @Override
  public Config getResolvedConfig(URI uri, String version) {
    CircularDependencyChecker.checkCircularDependency(this, version, uri);

    Config self = this.getOwnConfig(uri, version);

    // root can not include anything, otherwise will have circular dependency 
    if (isRootURI(uri)) {
      return self;
    }

    Collection<URI> imported = this.getOwnImports(uri, version);
    Iterator<URI> it = imported.iterator();
    List<Config> importedConfigs = new ArrayList<Config>();
    while (it.hasNext()) {
      importedConfigs.add(this.getResolvedConfig(it.next(), version));
    }

    // apply the reverse order for imported
    for (int i = importedConfigs.size() - 1; i >= 0; i--) {
      self = self.withFallback(importedConfigs.get(i));
    }

    Config ancestor = this.getAncestorConfig(uri, version);
    return self.withFallback(ancestor);
  }

  protected Config getAncestorConfig(URI uri, String version) {
    URI parent = getParent(uri);
    Config res = getResolvedConfig(parent, version);
    return res;
  }
  
  public URI getParent(URI uri) {
    if (isValidURI(uri)) {
      String pStr = uri.getPath();
      if(pStr.length()==0) return null;
      
      Path p = (new Path(uri.getPath())).getParent();
      try {
        return new URI(uri.getScheme(), uri.getAuthority(), p.toString(), uri.getQuery(), uri.getFragment());
      } catch (URISyntaxException e) {
        // Should not come here
        e.printStackTrace();
      }
    }
    return null;
  }

  @Override
  public Collection<URI> getImportsRecursively(URI uri, String version) {
    CircularDependencyChecker.checkCircularDependency(this, version, uri);

    Collection<URI> result = getOwnImports(uri, version);
    // root can not include anything, otherwise will have circular dependency 
    if (isRootURI(uri)) {
      return result;
    }

    Collection<URI> imported = this.getOwnImports(uri, version);
    Iterator<URI> it = imported.iterator();

    while (it.hasNext()) {
      result.addAll(this.getImportsRecursively(it.next(), version));
    }

    result.addAll(this.getImportsRecursively(getParent(uri), version));

    return dedup(result);
  }

  protected Collection<URI> dedup(Collection<URI> input) {
    Set<URI> set = new LinkedHashSet<URI>(input);
    return set;
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

  public static final boolean isValidURI(URI uri) {
    return isRootURI(uri) || isValidTag(uri) || isValidDataset(uri);
  }

  public static final boolean isValidTag(URI uri) {
    if (uri == null)
      return false;

    if (uri.toString().equals(TAG_PREFIX) || uri.toString().startsWith(TAG_PREFIX + ID_DELEMETER))
      return true;

    return false;
  }

  public static final boolean isValidDataset(URI uri) {
    if (uri == null)
      return false;

    if (uri.toString().equals(DATASET_PREFIX) || uri.toString().startsWith(DATASET_PREFIX + ID_DELEMETER))
      return true;

    return false;
  }

  private ImportMappings getImportMappings(String version){
    if( this.im_map.get(version)==null ){
      ImportMappings  im = new SimpleImportMappings(this, version);
      this.im_map.put(version, im);
    }
    return this.im_map.get(version);
  }
  
  @Override
  public Collection<URI> getImportedByRecursively(URI uri, String version) {
    ImportMappings im = this.getImportMappings(version);
    return im.getImportedByMappingRecursively().get(uri);
  }

  @Override
  public Map<URI, Config> getConfigsImportedByRecursively(URI uri, String version) {
    Collection<URI> importedByRec = this.getImportedByRecursively(uri, version);
    Map<URI, Config> result = new HashMap<URI, Config>();
    
    Iterator<URI> it = importedByRec.iterator();
    URI tmp;
    while(it.hasNext()){
      tmp = it.next();
      result.put(tmp, this.getResolvedConfig(tmp, version));
    }
    
    return result;
  }

  @Override
  public Collection<URI> getImportedBy(URI uri, String version) {
    ImportMappings im = this.getImportMappings(version);
    return im.getImportedByMapping().get(uri);
  }

  @Override
  public Map<URI, Config> getConfigsImportedBy(URI uri, String version) {
    Collection<URI> importedBy = this.getImportedBy(uri, version);
    Map<URI, Config> result = new HashMap<URI, Config>();
    
    Iterator<URI> it = importedBy.iterator();
    URI tmp;
    while(it.hasNext()){
      tmp = it.next();
      result.put(tmp, this.getOwnConfig(tmp, version));
    }
    
    return result;
  }
}
