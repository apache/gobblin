package gobblin.config.configstore.impl;

import gobblin.config.configstore.ConfigStore;
import gobblin.config.configstore.VersionDoesNotExistException;
import gobblin.config.configstore.VersionFinder;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
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

  private final Map<String, Path> versionRootMap = new HashMap<String, Path>();
  private final URI storeURI;
  private final Path location;
  private final String currentVersion;
  protected final FileSystem fs;
  protected final VersionFinder<String> vc;

  /**
   * 
   * @param physical_root - actual hdfs URI, example: new URI("hdfs://eat1-nertznn01.grid.linkedin.com:9000/user/mitu/HdfsBasedConfigTest")
   * @param logic_root - logic hdfs URI, the scheme name could be different than actual scheme name, 
   *  example: new URI("etl-hdfs://eat1-nertznn01.grid.linkedin.com:9000/user/mitu/HdfsBasedConfigTest")
   */
  public BaseHdfsConfigStore(URI physical_root, URI logic_root) {
    this(physical_root, logic_root, new SimpleVersionFinder());
  }

  public BaseHdfsConfigStore(URI physical_root, URI logic_root, VersionFinder<String> vc) {
    try {
      this.storeURI = logic_root;
      this.location = new Path(physical_root);
      this.fs = FileSystem.get(physical_root, new Configuration());
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
  public URI getStoreURI() {
    return storeURI;
  }

  @SuppressWarnings("deprecation")
  protected Path getVersionRoot(String version) throws VersionDoesNotExistException{
    if (this.versionRootMap.containsKey(version)) {
      return this.versionRootMap.get(version);
    }

    Path versionRoot = new Path(this.location, version);
    try {
      if (this.fs.isDirectory(versionRoot)) {
        this.versionRootMap.put(version, versionRoot);
        return versionRoot;
      } else {
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
  public Collection<URI> getChildren(URI uri, String version) throws VersionDoesNotExistException{
    if (uri == null)
      return Collections.emptyList();

    Path self = getPath(uri, version);

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

        res.add(getRelativePathURI(f.getPath(), version));
      }
      return res;
    } catch (IOException ioe) {
      LOG.error(String.format("Got error when find children for %s, exception %s", self, ioe.getMessage()));
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public Config getOwnConfig(URI uri, String version) throws VersionDoesNotExistException{
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

    try (FSDataInputStream configFileStream = this.fs.open(configFile)) {
      return ConfigFactory.parseReader(new InputStreamReader(configFileStream)).resolve();
    } catch (IOException ioe) {
      LOG.error(String.format("Got error when get own config for %s, exception %s", self, ioe.getMessage()), ioe);
      throw new RuntimeException(ioe);
    }
  }

  private URI getRelativePathURI(Path p, String version) throws VersionDoesNotExistException{
    URI versionRootURI = this.getVersionRoot(version).toUri();
    URI input = p.toUri();

    return versionRootURI.relativize(input);
  }

  protected static final boolean isRootURI(URI uri) {
    if (uri == null)
      return false;

    return uri.toString().length() == 0;
  }

  protected final Path getPath(URI uri, String version) throws VersionDoesNotExistException{
    Path self;
    if (isRootURI(uri)) {
      self = this.getVersionRoot(version);
    } else {
      self = new Path(this.getVersionRoot(version), uri.toString());
    }
    return self;
  }
}
