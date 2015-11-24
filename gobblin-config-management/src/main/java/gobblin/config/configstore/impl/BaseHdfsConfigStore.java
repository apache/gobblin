package gobblin.config.configstore.impl;

import gobblin.config.configstore.ConfigStore;
import gobblin.config.configstore.VersionComparator;
import gobblin.config.utils.PathUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.google.common.io.Closer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * The BaseHdfsConfigStore implements basic functions from ConfigStore which is related to HDFS
 * @author mitu
 *
 */
public abstract class BaseHdfsConfigStore implements ConfigStore {

  private static final Logger LOG = Logger.getLogger(BaseHdfsConfigStore.class);
  public static final String CONFIG_FILE_NAME = "main.conf";

  private final Map<String, Path> versionRootMap= new HashMap<String,Path>();
  private final URI storeURI;
  private final Path location;
  private final String currentVersion;
  protected final FileSystem fs;
  protected final VersionComparator<String> vc;

  public BaseHdfsConfigStore(URI root) {
    this(root, new SimpleVersionComparator());
  }

  public BaseHdfsConfigStore(URI root, VersionComparator<String> vc) {
    try {
      this.storeURI = root;
      this.location = new Path(root);
      this.fs = FileSystem.get(root, new Configuration());
    } catch (IOException ioe) {
      LOG.error("can not initial the file system " + ioe.getMessage(), ioe);
      throw new RuntimeException(ioe);
    }

    this.vc = vc;
    this.currentVersion = this.findCurrentVersion();
  }

  private String findCurrentVersion() {
    try {
      if (this.fs.isFile(this.location)) {
        throw new RuntimeException(String.format("location %s should be a directory ", this.location));
      }

      FileStatus[] fileStatus = this.fs.listStatus(this.location);
      if (fileStatus == null || fileStatus.length == 0) {
        throw new RuntimeException(String.format("location %s does not have any versions ", this.location));
      }

      List<String> versions = new ArrayList<String>();
      for (FileStatus f : fileStatus) {
        // versions should be directory
        if (!f.isDir()) {
          continue;
        }
        versions.add(f.getPath().getName());
      }
      String res = this.vc.getCurrentVersion(versions);

      if (res == null) {
        throw new RuntimeException(String.format("location %s does not have any valid version: ", this.location));
      }

      return res;
    } catch (IOException ioe) {
      LOG.error("can not find current version: " + ioe.getMessage(), ioe);
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public String getCurrentVersion() {
    return this.currentVersion;
  }
  
  @Override
  public URI getStoreURI(){
    return storeURI;
  }
  
  @SuppressWarnings("deprecation")
  protected Path getVersionRoot(String version){
    if(this.versionRootMap.containsKey(version)){
      return this.versionRootMap.get(version);
    }
    
    Path versionRoot = new Path(this.location, version);
    try {
      if(this.fs.isDirectory(versionRoot)){
        this.versionRootMap.put(version, versionRoot);
        return versionRoot;
      }
      else {
        String errorMesg = "Specified version " + versionRoot + " is not valid directory";
        LOG.error(errorMesg);
        throw new VersionDoesNotExistException(errorMesg);
      }
    } catch (IOException ioe) {
      LOG.error("can not find the version " + version + " error:  " + ioe.getMessage(), ioe);
      throw new VersionDoesNotExistException(ioe.getMessage());
    } 
  }

  @Override
  public Collection<URI> getChildren(URI uri, String version) {
    if (uri == null)
      return Collections.emptyList();;
    
    Path self = getPath(uri, version) ;
    
    try {
      if (this.fs.isFile(self)) {
        return Collections.emptyList();
      }

      FileStatus[] fileStatus = this.fs.listStatus(self);
      if (fileStatus == null || fileStatus.length == 0) {
        return Collections.emptyList();
      }

      List<URI> res = new ArrayList<URI>();
      for (FileStatus f : fileStatus) {
        // valid node should be a directory
        if (!f.isDir()) {
          continue;
        }
        try {
          res.add(new URI(this.getRelativePath(f.getPath(), version)));
        } catch (URISyntaxException e) {
          LOG.error(String.format("Got error when create URI for %s, exception %s", f.getPath(), e.getMessage()), e);
          throw new RuntimeException(e);
        }
      }

      return res;
    } catch (IOException ioe) {
      LOG.error(String.format("Got error when find children for %s, exception %s", self, ioe.getMessage()));
      throw new RuntimeException(ioe);
    }
  }
  
  @Override
  public Config getOwnConfig(URI uri, String version) {
    if (uri == null)
      return ConfigFactory.empty();


    Path self = getPath(uri, version);
    Path configFile = new Path(self, CONFIG_FILE_NAME);
    try {
      if (!this.fs.isFile(configFile)) {
        return ConfigFactory.empty();
      }
    } catch (IOException e) {
      LOG.error(String.format("Got error when get own config for %s, exception %s", self, e.getMessage()), e);
      return ConfigFactory.empty();
    }
    
    try ( FSDataInputStream configFileStream = this.fs.open(configFile)){
      return ConfigFactory.parseReader(new InputStreamReader(configFileStream)).resolve();
    }
    catch (IOException ioe) {
      LOG.error(String.format("Got error when get own config for %s, exception %s", self, ioe.getMessage()), ioe);
      throw new RuntimeException(ioe);
    }
  }

  protected String getRelativePath(Path p, String version) {
    String root = PathUtils.getPathWithoutSchemeAndAuthority(this.getVersionRoot(version)).toString();
    String input = PathUtils.getPathWithoutSchemeAndAuthority(p).toString();

    if (input.equals(root)) {
      return "";
    }
    return input.substring(root.length()+1);
  }
  
  protected static final boolean isRootURI(URI uri) {
    if (uri == null)
      return false;

    return uri.toString().length()==0;
  }
  
  protected final Path getPath(URI uri, String version){
    Path self ;
    if(isRootURI(uri)){
      self = this.getVersionRoot(version);
    }else{
      self = new Path(this.getVersionRoot(version), uri.toString());
    }
    return self;
  }
}
