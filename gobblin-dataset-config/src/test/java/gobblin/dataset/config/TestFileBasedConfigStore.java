package gobblin.dataset.config;


import java.io.*;
import java.util.*;
import java.net.URL;

import com.typesafe.config.*;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;

public class TestFileBasedConfigStore {
  private FileBasedConfigStore cs;
  
  @BeforeClass
  public void startUp() throws Exception{
    URL url = getClass().getResource("/filebasedConfig/");
    cs = new FileBasedConfigStore(new File(url.getFile()), "test");
    cs.loadConfigs();
  }
  
  @Test public void testSubstitution_Inherent() throws Exception {
    
    String urn = "config-ds.a1.a2.a3";
    Config c = cs.getConfig(urn);
    
    for(Map.Entry<String, ConfigValue> entry: c.entrySet()){
      System.out.println("app key: " +entry.getKey() + " ,value:" + entry.getValue());
    }

    Assert.assertTrue(c.getString("testsubs").equals("foobar20"));
    Assert.assertTrue(c.getBoolean("deleteTarget"));
    Assert.assertTrue(c.getInt("retention")==3);
    
    Assert.assertTrue(c.getString("keyInA1").equals("valueInA1"));
    Assert.assertTrue(c.getString("keyInA2").equals("valueInA2"));
    
    //List<String> tags = cs.getAssociatedTags("config-tag.tag1.tag2");
    List<String> tags = cs.getAssociatedTags(urn);
    for(String s: tags){
      System.out.println("tags is " +s );
    }
  }
}
