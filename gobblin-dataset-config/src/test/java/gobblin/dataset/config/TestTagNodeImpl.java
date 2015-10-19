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

import org.testng.Assert;
import org.testng.annotations.Test;


public class TestTagNodeImpl {
  @Test public void testTags() throws Exception {
//    Config conf = ConfigFactory.parseFile(new File("/Users/mitu/github/gobblin-1/gobblin-dataset-config/src/test/resources/tags.conf") );
//
//    for(Map.Entry<String, ConfigValue> entry: conf.entrySet()){
//      System.out.println("All key: " +entry.getKey() + " ,value:" + entry.getValue());
//      
//    }
    
    Repository.getInstance(new File("/Users/mitu/github/gobblin-1/gobblin-dataset-config/src/test/resources/tags11.conf"));
    
    TagNodeImpl tagNode = new TagNodeImpl(null, "nertz.tracking.big-event");
    Config tagConf = tagNode.getConfig();
    for(Map.Entry<String, ConfigValue> entry: tagConf.entrySet()){
      System.out.println("key: " +entry.getKey() + " ,value:" + entry.getValue());
      
    }
    
    System.out.println(String.format("value for %s is %s", "retention", tagConf.getInt("retention")));
    
//    System.out.println("The answer is: " + conf.getString("nertz.tracking.deleteTarget"));
//
//    System.out.println("The answer is: " + conf.getString("nertz.tracking.retention"));
//
//    System.out.println("The answer is: " + conf.getString("nertz.tracking.big-event.retention"));
//    
//    try{
//      System.out.println("The answer is: " + conf.getString("nertz.tracking.big-event.deleteTarget"));
//    }
//    catch(ConfigException ce){
//      
//    }
  }
}
