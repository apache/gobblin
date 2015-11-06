package gobblin.dataset.config;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
public class Test {

  public static void main(String[] args) throws IOException {
    // TODO Auto-generated method stub
    Configuration _hadoopConf = new Configuration();
    FileSystem _localFS = FileSystem.getLocal(_hadoopConf);
    System.out.println("OK");
  }

}
