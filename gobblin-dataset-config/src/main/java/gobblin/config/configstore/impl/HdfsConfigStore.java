package gobblin.config.configstore.impl;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.typesafe.config.Config;

import gobblin.config.configstore.ConfigStore;

public class HdfsConfigStore implements ConfigStore{

  /**
   * 
   */
  private static final long serialVersionUID = 4048170056813280775L;
  
  private static final Logger LOG = Logger.getLogger(HdfsConfigStore.class);

  private final String scheme;
  private final String location;
  private final String currentVersion;
  private final FileSystem fs;

  public HdfsConfigStore(String scheme, String location){
    this.scheme = scheme;
    this.location = location;
    this.fs = getFileSystem();

    this.currentVersion = this.findCurrentVersion();
  }

  private FileSystem getFileSystem(){
    Configuration conf = new Configuration();

    try {
      if(this.location.startsWith("hdfs://")){
        return FileSystem.get(conf);
      }
      else if(this.location.startsWith("file://")){
        return FileSystem.getLocal(conf).getRawFileSystem();
      }
    }catch(IOException ioe){
      LOG.error("can not initial the file system " + ioe.getMessage());
      throw new RuntimeException(ioe);
    }

    throw new IllegalArgumentException("can not initial the file system using " + this.location);
  }

  private String findCurrentVersion(){

    Path path = new Path(this.location);
    try {
      System.out.println("path exists ? " + fs.exists(path) );

      return "1.0";
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return null;
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
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Collection<URI> getChildren(URI uri) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Collection<URI> getImports(URI uri) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Config getOwnConfig(URI uri) {
    // TODO Auto-generated method stub
    return null;
  }

}
