package gobblin.config.configstore.impl;


import gobblin.config.configstore.ConfigStore;
import gobblin.config.configstore.VersionComparator;
import gobblin.config.utils.PathUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * The BaseHdfsConfigStore implements basic functions from ConfigStore which is related to HDFS
 * @author mitu
 *
 */
public abstract class BaseHdfsConfigStore implements ConfigStore {

  /**
   * 
   */
  private static final long serialVersionUID = 4048170056813280775L;

  private static final Logger LOG = Logger.getLogger(BaseHdfsConfigStore.class);
  public static final String CONFIG_FILE_NAME = "main.conf";

  private final String scheme;
  private final Path location;
  private final String currentVersion;
  protected final FileSystem fs;
  protected final VersionComparator<String> vc;
  protected final Path currentVersionRoot;

  public BaseHdfsConfigStore(String scheme, String location) {
    this(scheme, location, new SimpleVersionComparator());
  }

  public BaseHdfsConfigStore(String scheme, String location, VersionComparator<String> vc) {
    this.scheme = scheme;
    this.location = new Path(location);
    try {
      this.fs = this.location.getFileSystem(new Configuration());
    } catch (IOException ioe) {
      LOG.error("can not initial the file system " + ioe.getMessage(), ioe);
      throw new RuntimeException(ioe);
    }

    this.vc = vc;
    this.currentVersion = this.findCurrentVersion();
    this.currentVersionRoot = new Path(this.location, this.currentVersion);
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
  public String getScheme() {
    return this.scheme;
  }

  @Override
  public String getCurrentVersion() {
    return this.currentVersion;
  }

  @Override
  public URI getParent(URI uri) {
    if (uri == null || uri.toString().length() == 0)
      return null;

    Path self = new Path(this.currentVersionRoot, uri.toString());
    Path parent = self.getParent();

    try {
      return new URI(getRelativePath(parent));
    } catch (URISyntaxException e) {
      LOG.error(String.format("Got error when create URI for %s, exception %s", parent, e.getMessage()), e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public Collection<URI> getChildren(URI uri) {
    if (uri == null)
      return null;
    
    Path self = getPath(uri) ;
    
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
          res.add(new URI(this.getRelativePath(f.getPath())));
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

  protected String getRelativePath(Path p) {
    String root = PathUtils.getPathWithoutSchemeAndAuthority(this.currentVersionRoot).toString();
    String input = PathUtils.getPathWithoutSchemeAndAuthority(p).toString();


    if (input.equals(root)) {
      return "";
    }
    return input.substring(root.length()+1);
  }
  
  public static final boolean isRootURI(URI uri) {
    if (uri == null)
      return false;

    return uri.toString().length()==0;
  }
  
  public final Path getPath(URI uri){
    Path self ;
    if(isRootURI(uri)){
      self = this.currentVersionRoot;
    }else{
      self = new Path(this.currentVersionRoot, uri.toString());
    }
    return self;
  }
}
