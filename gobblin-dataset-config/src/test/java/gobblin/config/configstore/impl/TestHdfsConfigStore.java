package gobblin.config.configstore.impl;

import java.io.*;
import java.util.*;
import java.net.URL;

import com.typesafe.config.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;


public class TestHdfsConfigStore {

  @Test public void testValid() throws Exception {
    //HdfsConfigStore store = new HdfsConfigStore("ETL_Local", "file:///Users/mitu/HdfsBasedConfigTest");
    
    //String test = "/Users/mitu/HdfsBasedConfigTest";
    //System.out.println("AAA is " + new File("/Users/mitu/HdfsBasedConfigTest").exists());
    
    Configuration _hadoopConf = new Configuration();
    FileSystem _localFS = FileSystem.getLocal(_hadoopConf);
    
    System.out.println("OK");
    //System.out.println("AAA is " + _localFS.exists(new Path(test)));
  }
}
