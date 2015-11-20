package gobblin.config.utils;

import java.net.URI;
import java.util.TreeMap;

import org.testng.Assert;
import org.testng.annotations.Test;


public class TestURIComparator {

  @Test
  public void testAgainstTreeMap() throws Exception {
    TreeMap<URI, String> testMap = new TreeMap<URI, String>(new URIComparator());

    testMap.put(new URI("etl-hdfs://eat1-nertznn01.grid.linkedin.com:9000/user/mitu"), "etl-hdfs");
    testMap.put(new URI("foobar://eat1-nertznn01.grid.linkedin.com:9000/user/mitu"), "foobar");
    testMap.put(new URI("file:/var/folders/rr"), "file");

    Assert.assertEquals(testMap.size(), 3);
    URI testURI = new URI("etl-hdfs://eat1-nertznn01.grid.linkedin.com:9000/user/mitu/sub1/sub2");
    Assert.assertEquals(testMap.get(testURI), "etl-hdfs");

    testURI = new URI("etl-hdfs://eat1-nertznn01.grid.linkedin.com:9000/user/mitu/");
    Assert.assertEquals(testMap.get(testURI), "etl-hdfs");

    testURI = new URI("etl-hdfs://eat1-nertznn01.grid.linkedin.com:9000/user/mituFOOBAR");
    Assert.assertEquals(testMap.containsKey(testURI), false);

    testURI = new URI("foobar://FOO-nertznn01.grid.linkedin.com:9000/user/mitu");
    Assert.assertEquals(testMap.containsKey(testURI), false);

    testURI = new URI("file:/abc/var/folders/rr");
    Assert.assertEquals(testMap.containsKey(testURI), false);
    
    testURI = new URI("file:///var/folders/rr/sub1");
    Assert.assertEquals(testMap.get(testURI), "file");
    
    testURI = new URI("etl-hdfs://eat1-nertznn01.grid.linkedin.com:9000/user/mitu/sub1/sub2");
    testMap.remove(testURI);
    Assert.assertEquals(testMap.size(), 2);
    Assert.assertEquals(testMap.containsKey(testURI), false);
  }
}
