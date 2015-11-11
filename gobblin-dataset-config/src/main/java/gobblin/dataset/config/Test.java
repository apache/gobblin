package gobblin.dataset.config;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import com.typesafe.config.ConfigValue;
public class Test {
  private static boolean debug = true;
  private static void printConfig(Config c, String urn) {
    if(!debug) return;
    System.out.println("-----------------------------------");
    System.out.println("Config for " + urn);
    for (Map.Entry<String, ConfigValue> entry : c.entrySet()) {
      System.out.println("app key: " + entry.getKey() + " ,value:" + entry.getValue());
    }
  }
  
  public static void main(String[] args) throws IOException {
    
    // TODO Auto-generated method stub
//    Configuration _hadoopConf = new Configuration();
//    FileSystem _localFS = FileSystem.getLocal(_hadoopConf);
//    System.out.println("OK1");
    
    //String fileName = "/Users/mitu/HdfsBasedConfigTest/v3.0/datasets/a1/a2/a3/main.conf";
    String fileName = "/Users/mitu/main.conf";
    

    
    Config c = ConfigFactory.parseFile(new File(fileName));
    printConfig(c, "test ");
    
    System.out.println("----------------");


    
  }

}
