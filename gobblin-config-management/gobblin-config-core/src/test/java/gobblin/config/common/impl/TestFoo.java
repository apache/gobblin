package gobblin.config.common.impl;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ListMultimap;

import gobblin.config.store.api.ConfigKeyPath;
public class TestFoo {

  public static void main(String[]args){
    ListMultimap<String, String> childrenMap = ArrayListMultimap.create();
    
    List<String> emptyList = new ArrayList<String>();
    emptyList.add("bar");
    System.out.println("in map ? " + childrenMap.containsKey("foo"));
    
    childrenMap.putAll("foo", emptyList);
    System.out.println("in map ? " + childrenMap.containsKey("foo"));
    for(String s: childrenMap.keySet()){
      System.out.println("childmap key " +s);
    }
    
    Map<String, List<String>> map2 = new HashMap<String, List<String>>();
    
    System.out.println("in map2 ? " + map2.containsKey("foo"));
    
    map2.put("foo", emptyList);
    System.out.println("in map2 ? " + map2.containsKey("foo"));
  }
}
