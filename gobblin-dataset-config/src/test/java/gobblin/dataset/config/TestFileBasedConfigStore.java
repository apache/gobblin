package gobblin.dataset.config;


import java.io.*;
import java.util.*;

import com.typesafe.config.*;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestFileBasedConfigStore {
  @Test public void testTags() throws Exception {
    //FileBasedConfigStore cs = new FileBasedConfigStore("/Users/mitu/AAA", "test");
    FileBasedConfigStore cs = new FileBasedConfigStore("filebasedConfig", "test");
    
    cs.loadConfigs();
    String urn = "config-ds.nertz.tracking.big-event";
    Config c = cs.getConfig(urn);
    
    System.out.println("Urn is " + urn);
    for(Map.Entry<String, ConfigValue> entry: c.entrySet()){
      System.out.println("app key: " +entry.getKey() + " ,value:" + entry.getValue());

    }

    List<String> tags = cs.getAssociatedTags("config-ds.nertz.tracking");
    if(tags!=null){
      for(String t: tags){
        System.out.println("tags is " + t);
      }
    }
    else {
      System.out.println("No associated tags");
    }
  }
}
