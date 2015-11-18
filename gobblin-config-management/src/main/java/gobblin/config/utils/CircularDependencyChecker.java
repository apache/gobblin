package gobblin.config.utils;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;

import gobblin.config.configstore.ConfigStore;

public class CircularDependencyChecker {
  
  /**
   * @param cs - {@gobblin.config.configstore.ConfigStore} to check circular dependency
   * @param uri - URI relative to the input ConfigStore. This is the starting point to check circular dependency
   */
  public static void checkCircularDependency(ConfigStore cs, String version, URI uri){
    // check ancestor chain
    checkAncestorCircularDependency(cs, version, uri, uri, new ArrayList<URI>());
    
    // check self imported chain
    checkImportCircularDependency(cs, version, uri, uri, new ArrayList<URI>());
  }

  /**
   * 
   * @param cs - ConfigStore to check with
   * @param version - ConfigStore version to check with
   * @param initialURI - the initial URI passed from public function checkCircularDependency(...)
   * @param uri - current URI to check with
   * @param previous - all previously imports
   */
  private static void checkAncestorCircularDependency(ConfigStore cs, String version, URI initialURI, URI uri, List<URI> previous){    
    String pStr = uri.getPath();
    if(pStr.length()==0) return;
    
    Path p = (new Path(uri.getPath())).getParent();
    URI parent = p.toUri();
    
    List<URI> current = new ArrayList<URI>();
    current.addAll(previous);
    current.add(uri);
    
    // should check parent imports chain
    checkImportCircularDependency(cs, version, parent, parent, current);
    
    if(parent!=null){
      checkAncestorCircularDependency(cs, version, initialURI, parent, current);
    }
  }
  
  private static void checkImportCircularDependency(ConfigStore cs, String version, URI initialURI, URI uri, List<URI> previous){
    for(URI p: previous){
      if(uri!=null && uri.equals(p)){
        previous.add(p);
        throw new CircularDependencyException(getChain(initialURI, previous, uri));
      }
    }
    
    Collection<URI> imported = cs.getOwnImports(uri, version);
    Iterator<URI> it = imported.iterator();
    while(it.hasNext()){
      URI singleImport = it.next();
      
      if(singleImport.equals(uri)){
        throw new CircularDependencyException(String.format("URI %s import self", uri));
      }
      List<URI> current = new ArrayList<URI>();
      current.addAll(previous);
      current.add(uri);
      checkImportCircularDependency(cs, version, initialURI, singleImport, current);
      
      // checked the ancestor chain for that imported URI , without this the parent to children circle will not
      // be detected if starting from parent
      checkAncestorCircularDependency(cs, version, initialURI, singleImport, current);
    }
  }
  
  private static String getChain(URI initialURI, List<URI>chain, URI circular){
    StringBuilder sb = new StringBuilder();
    sb.append("Initial URI: " + initialURI);
    for(URI u: chain){
      sb.append(" -> " + u);
    }
    
    sb.append(" the uri causing circular dependency: " + circular );
    return sb.toString();
  }
}
