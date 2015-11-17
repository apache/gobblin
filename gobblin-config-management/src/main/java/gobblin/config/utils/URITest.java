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
    
    test = new URI("hdfs://eat1-nertznn01.grid.linkedin.com:9000/user/mitu");
    n = new URI("user");
    display(test, n);
    
    test = new URI("user/mitu");
    n = new URI("user");
    display(test, n);
    
    Path p = new Path("j2se/1.3/docs/guide/index.html");
    int i=1;
    while(p!=null && i<10){
      System.out.println("parent " + p.getParent().toUri());
      p = p.getParent();
    }
    
  }
}
