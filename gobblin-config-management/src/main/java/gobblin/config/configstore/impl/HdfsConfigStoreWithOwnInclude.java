package gobblin.config.configstore.impl;

import gobblin.config.configstore.VersionFinder;

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

/**
 * HdfsConfigStoreWithOwnInclude extends BaseHdfsConfigStore and use it's own way
 * to implements how configuration node includes other configuration nodes
 * @author mitu
 *
 */
public class HdfsConfigStoreWithOwnInclude extends BaseHdfsConfigStore {

  private static final Logger LOG = Logger.getLogger(HdfsConfigStoreWithOwnInclude.class);
  public static final String INCLUDE_FILE_NAME = "includes";

  public HdfsConfigStoreWithOwnInclude(URI physical_root, URI logic_root) {
    this(physical_root, logic_root, new SimpleVersionFinder());
  }

  public HdfsConfigStoreWithOwnInclude(URI physical_root, URI logic_root, VersionFinder<String> vc) {
    super(physical_root, logic_root, vc);
  }

  
  @Override
  public Collection<URI> getOwnImports(URI uri, String version) {
    List<URI> result = new ArrayList<URI>();

    try {
      Path self = getPath(uri, version);
      Path includeFile = new Path(self, INCLUDE_FILE_NAME);
      List<String> imports = this.getImports(includeFile, version);
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

  @SuppressWarnings("deprecation")
  private List<String> getImports(Path p, String version) throws IOException{
    List<String> allImports = Lists.newArrayList();

    if(!fs.exists(p) || !fs.isFile(p)) {
      return allImports;
    }

    Closer closer = Closer.create();

    try {
      FSDataInputStream fsDataInputStream = closer.register(fs.open(p));
      InputStreamReader inputStreamReader = closer.register(new InputStreamReader(fsDataInputStream));
      BufferedReader br = closer.register(new BufferedReader(inputStreamReader));
      String line = br.readLine();
      while (line != null) {
        Path singleImport = new Path(getVersionRoot(version), line);
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
  
}
