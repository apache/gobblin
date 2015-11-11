package gobblin.config.configstore.impl;

import gobblin.config.configstore.VersionComparator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


public class HdfsConfigStoreWithOwnInclude extends BaseHdfsConfigStore {

  /**
   * 
   */
  private static final long serialVersionUID = 4048170056813280775L;

  private static final Logger LOG = Logger.getLogger(HdfsConfigStoreWithOwnInclude.class);
  public static final String INCLUDE_FILE_NAME = "includes";

  public HdfsConfigStoreWithOwnInclude(String scheme, String location) {
    this(scheme, location, new SimpleVersionComparator());
  }

  public HdfsConfigStoreWithOwnInclude(String scheme, String location, VersionComparator<String> vc) {
    super(scheme, location, vc);
  }

  
  @Override
  public Collection<URI> getImports(URI uri) {
    List<URI> result = new ArrayList<URI>();

    try {
      Path self = new Path(this.currentVersionRoot, uri.toString());
      Path includeFile = new Path(self, INCLUDE_FILE_NAME);
      List<String> imports = this.getImports(includeFile);
      for(String s: imports){
        try {
          result.add(new URI(s));
        }
        catch (URISyntaxException e) {
          LOG.error("Could not parse  " + s + " as URI", e);
        }
      }
      
      return result;
    }
    catch(IOException ioe){
      LOG.error("Could not find imports at path " + uri, ioe);
      return result;
    } 
  }

  private List<String> getImports(Path p) throws IOException{
    List<String> allImports = Lists.newArrayList();

    if(!fs.exists(p) || !fs.isFile(p)) {
      LOG.error("Include file " + p + " does not exist or is not a file.");
      return allImports;
    }

    Closer closer = Closer.create();

    try {
      FSDataInputStream fsDataInputStream = closer.register(fs.open(p));
      InputStreamReader inputStreamReader = closer.register(new InputStreamReader(fsDataInputStream));
      BufferedReader br = closer.register(new BufferedReader(inputStreamReader));
      String line = br.readLine();
      while (line != null) {
        Path singleImport = new Path(this.currentVersionRoot, line);
        if(this.fs.isDirectory(singleImport)){
          allImports.add(line);
        }
        else {
          LOG.error("Invalid imported for " + line);
        }

        line = br.readLine();
      }
    } catch(IOException exception) {
      LOG.error("Could not find imports at path " + p, exception);
      return Lists.newArrayList();
    } finally {
      closer.close();
    }

    return allImports;
  }
  
  @Override
  public Config getOwnConfig(URI uri) {
    if (uri == null)
      return ConfigFactory.empty();

    Closer closer = Closer.create();
    Path self = new Path(this.currentVersionRoot, uri.toString());
    Path configFile = new Path(self, CONFIG_FILE_NAME);
    try {
      if (!this.fs.isFile(configFile)) {
        return ConfigFactory.empty();
      }

      FSDataInputStream configFileStream = closer.register(this.fs.open(configFile));
      return ConfigFactory.parseReader(new InputStreamReader(configFileStream)).resolve();
    }
    catch (IOException ioe) {
      LOG.error(String.format("Got error when get own config for %s, exception %s", self, ioe.getMessage()), ioe);
      throw new RuntimeException(ioe);
    }
    finally {
      try {
        closer.close();
      } catch (IOException e) {
        LOG.error("Failed to close FsInputStream: " + e, e);
      }
    }
  }
}
