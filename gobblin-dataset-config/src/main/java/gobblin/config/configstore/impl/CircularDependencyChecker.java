package gobblin.config.configstore.impl;

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
//    System.out.println("uri is " + uri);
//    for(URI p: previous){
//      System.out.println("p is " + p);
//      if(uri.equals(p)){
//        throw new CircularDependencyException(getChain(initialURI, previous, uri));
//      }
//    }
    
    URI parent = cs.getParent(uri);
    
    // TBD, should check parent
    checkImportCircularDependency(cs, parent, parent, new ArrayList<URI>(previous));
    
    if(parent!=null){
      List<URI> current = new ArrayList<URI>();
      current.addAll(previous);
      current.add(uri);
      checkAncestorCircularDependency(cs, initialURI, parent, current);
    }
  }
  
  private static void checkImportCircularDependency(ConfigStore cs, URI initialURI, URI uri, List<URI> previous){
    for(URI p: previous){
      if(uri!=null && uri.equals(p)){
        throw new CircularDependencyException(getChain(initialURI, previous, uri));
      }
    }
    
    Collection<URI> imported = cs.getImports(uri);
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
