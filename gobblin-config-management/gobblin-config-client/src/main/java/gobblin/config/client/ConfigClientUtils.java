/*
 * Copyright (C) 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.config.client;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;

import gobblin.config.common.impl.SingleLinkedListConfigKeyPath;
import gobblin.config.store.api.ConfigKeyPath;
import gobblin.config.store.api.ConfigStore;

/**
 * Utility class to transfer {@link URI} to {@link ConfigKeyPath} and vice versa 
 * 
 * @author mitu
 *
 */
public class ConfigClientUtils {

  /**
   * 
   * @param configKeyURI - URI provided by client , which could missing authority/store root directory
   * @param cs           - ConfigStore corresponding to the input URI. Require input URI's scheme/authority name 
   *                       match ConfigStore's scheme/authority
   * @return             - {@link ConfigKeyPath} for the relative path
   */
  public static ConfigKeyPath buildConfigKeyPath(URI configKeyURI, ConfigStore cs){
    checkMatchingSchemeAndAuthority(configKeyURI, cs);
    // Example store root is   etl-hdfs://eat1-nertznn01.grid.linkedin.com:9000/user/mitu/HdfsBasedConfigTest
    
    // configKeyURI is etl-hdfs:///datasets/a1/a2
    if(configKeyURI.getAuthority()==null){
      return getConfigKeyPath(configKeyURI.getPath());
    }
    // configKeyURI is etl-hdfs://eat1-nertznn01.grid.linkedin.com:9000/user/mitu/HdfsBasedConfigTest/datasets/a1/a2
    else {
      URI relative = cs.getStoreURI().relativize(configKeyURI);
      return getConfigKeyPath(relative.getPath());
    } 
  }
  
  /**
   * Build the {@link  ConfigKeyPath} based on the absolute/relative path
   * @param input - absolute/relative file path
   * @return      - {@link  ConfigKeyPath} corresponding to the input
   */
  public static ConfigKeyPath getConfigKeyPath(String input){
    ConfigKeyPath result = SingleLinkedListConfigKeyPath.ROOT;
    String[] paths = input.split("/");
    for(String p: paths){
      // in case input start with "/", some elements could be "", which should be skip
      if(p.equals("")){
        continue;
      }
      result = result.createChild(p);
    }
    return result;
  }
  
  /**
   * 
   * @param configKeyPath - {@link ConfigKeyPath} object which represent the config node in input {@link ConfigStore}
   * @param cs            - the corresponding ConfigStore
   * @return              - the absolute URI which combine the ConfigStore root URI and input ConfigKeyPath
   */
  public static URI getAbsoluteURI(ConfigKeyPath configKeyPath, ConfigStore cs){
    URI storeRoot = cs.getStoreURI();
    String path = storeRoot.getPath();
    if(path.endsWith("/")){
      path = path + configKeyPath.getAbsolutePathString().substring(1);
    }
    else{
      path = path + configKeyPath.getAbsolutePathString();
    }
    
    try {
      return new URI(storeRoot.getScheme(), storeRoot.getAuthority(), path , null, null);
    } catch (URISyntaxException e) {
      // should not come here
      throw new RuntimeException("Can not build URI based on " + configKeyPath);
    }
  }
  
  /**
   * batch process for {@link #getAbsoluteURI(Collection, ConfigStore)}
   * @param configKeyPaths
   * @param cs
   * @return
   */
  public static Collection<URI> getAbsoluteURI(Collection<ConfigKeyPath> configKeyPaths, ConfigStore cs){
    Collection<URI> result = new ArrayList<>();
    for(ConfigKeyPath p: configKeyPaths){
      result.add(getAbsoluteURI(p, cs));
    }
    
    return result;
  }
  
  private static void checkMatchingSchemeAndAuthority(URI configKeyURI, ConfigStore cs){
    if(configKeyURI == null || cs == null){
      throw new IllegalArgumentException("input can not be null");
    }
    
    Preconditions.checkArgument(configKeyURI.getScheme().equals(cs.getStoreURI().getScheme()),
        "Scheme name not match");
    boolean authorityCheck = configKeyURI.getAuthority() == null ||
        configKeyURI.getAuthority().equals(cs.getStoreURI().getAuthority());
    Preconditions.checkArgument(authorityCheck, "Authority not match");
  }
  
  /**
   * Utility method to check whether one URI is the ancestor of the other
   * 
   * return true iff both URI's scheme/authority name match and ancestor's path is the prefix of the descendant's path
   * @param descendant: the descendant URI to check
   * @param ancestor  : the ancestor URI to check
   * @return
   */
  public static boolean isAncestorOrSame(URI descendant, URI ancestor){
    if(ancestor == null || descendant == null){
      throw new IllegalArgumentException("input can not be null");
    }
    
    if(!stringSame(descendant.getScheme(), ancestor.getScheme())){
      return false;
    }
    
    if(!stringSame(descendant.getAuthority(), ancestor.getAuthority())){
      return false;
    }
    
    return isAncestorOrSame(new Path(descendant.getPath()), new Path(ancestor.getPath()));
  }
  
  public static boolean stringSame(String l, String r){
    if(l==null && r==null){
      return true;
    }
    
    if(l==null || r==null){
      return false;
    }
    
    return l.equals(r);
  }
  
  public static boolean isAncestorOrSame(Path descendant, Path ancestor){
    if(ancestor == null || descendant == null){
      throw new IllegalArgumentException("input can not be null");
    }
    
    if(descendant.equals(ancestor)) return true;
    Path parent = descendant.getParent();
    if(parent ==null) return false;
    
    return isAncestorOrSame(parent, ancestor);
  }
}
