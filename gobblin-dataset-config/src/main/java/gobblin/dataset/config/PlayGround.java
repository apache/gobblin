
/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.dataset.config;

import java.io.*;
import java.util.*;

import com.typesafe.config.*;


public class PlayGround {
  public static void main(String[] args){

    //    //Config tagConf = ConfigFactory.load("tags22.conf");
    //    
    //    Config tagConf = ConfigFactory.defaultReference();
    //    for(Map.Entry<String, ConfigValue> entry: tagConf.entrySet()){
    //      System.out.println("reference key: " +entry.getKey() + " ,value:" + entry.getValue());
    //      
    //    }
    //    
    //    Config appConf = ConfigFactory.load();
    //    for(Map.Entry<String, ConfigValue> entry: appConf.entrySet()){
    //      System.out.println("app key: " +entry.getKey() + " ,value:" + entry.getValue());
    //      
    //    }
    //    
    //    System.out.println("done");

    FileBasedConfigStore cs = new FileBasedConfigStore("/Users/mitu/AAA", "test");
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