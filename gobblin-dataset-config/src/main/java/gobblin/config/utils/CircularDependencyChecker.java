package gobblin.config.utils;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import gobblin.config.configstore.ConfigStore;

public class CircularDependencyChecker {
  
  public static void checkCircularDependency(ConfigStore cs, URI uri){
    // check ancestor chain
    checkAncestorCircularDependency(cs, uri, uri, new ArrayList<URI>());
    
    // check self imported chain
    checkImportCircularDependency(cs, uri, uri, new ArrayList<URI>());
  }

  private static void checkAncestorCircularDependency(ConfigStore cs, URI initialURI, URI uri, List<URI> previous){
    URI parent = cs.getParent(uri);
    if(parent==null) return;
    
    List<URI> current = new ArrayList<URI>();
    current.addAll(previous);
    current.add(uri);
    
    // TBD, should check parent
    checkImportCircularDependency(cs, parent, parent, current);
    
    if(parent!=null){
      checkAncestorCircularDependency(cs, initialURI, parent, current);
    }
  }
  
  private static void checkImportCircularDependency(ConfigStore cs, URI initialURI, URI uri, List<URI> previous){
    for(URI p: previous){
      if(uri!=null && uri.equals(p)){
        throw new CircularDependencyException(getChain(initialURI, previous, uri));
      }
    }
    
    Collection<URI> imported = cs.getOwnImports(uri);
    Iterator<URI> it = imported.iterator();
    while(it.hasNext()){
      URI singleImport = it.next();
      if(singleImport.equals(uri)){
        throw new CircularDependencyException(String.format("URI %s import self", uri));
      }
      List<URI> current = new ArrayList<URI>();
      current.addAll(previous);
      current.add(uri);
      checkImportCircularDependency(cs, initialURI, singleImport, current);
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
