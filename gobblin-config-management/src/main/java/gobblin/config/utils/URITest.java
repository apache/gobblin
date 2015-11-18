package gobblin.config.utils;

import java.net.URI;

import org.apache.hadoop.fs.Path;

public class URITest {
  
  public static void display(URI u, URI n){
    System.out.println("scheme: " + u.getScheme());
    
    System.out.println("authority: " + u.getAuthority());
    
    System.out.println("path: " + u.getPath());
    
    System.out.println("raw path: " + u.getRawPath());
    
    System.out.println("isAbs: " + u.isAbsolute());
    
    System.out.println("relative: " + u.relativize(n));
    
    System.out.println("resolved: " + u.resolve(n));
    
    System.out.println("-------------------");
  }

  public static void main(String[] args) throws Exception{
    URI test = new URI("http://java.sun.com/j2se/1.3/docs/guide/index.html");
    URI n = new URI("index2.html");
    
    display(test, n);
    
    //test = new URI("etl-hdfs://eat1-nertznn01.grid.linkedin.com:9000/user/mitu");
    test = new URI("etl-hdfs:///var/folders/rr/ps06_n69685g9m4cmfs_907c000ckg/T/1447889403003-0/HdfsBasedConfigTest");
    n = new URI("user");
    display(test, n);
    
    URI test2 = new URI("file", test.getAuthority(), test.getPath(), test.getQuery(), test.getFragment());
    display(test2, n);
    
//    test = new URI("file:///user/mitu");
//    n = new URI("user");
//    display(test, n);
    
//    Path p = new Path("j2se/1.3/docs/guide/index.html");
//    int i=1;
//    while(p!=null && i<10){
//      System.out.println("parent " + p.getParent().toUri());
//      p = p.getParent();
//    }
    
  }
}
