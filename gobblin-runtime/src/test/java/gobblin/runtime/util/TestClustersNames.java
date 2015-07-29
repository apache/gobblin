package gobblin.runtime.util;

import org.testng.Assert;
import org.testng.annotations.Test;

/** Unit tests for {@link ClustersNames}. This test relies on the ClustersNames.properties file */
public class TestClustersNames {
  @Test
  public void testClustersNames() {
    ClustersNames clustersNames = ClustersNames.getInstance();
    Assert.assertEquals(clustersNames.getClusterName("http://cluster1-rm.some.company.com"),
                        "cluster1");
    Assert.assertEquals(clustersNames.getClusterName("http://cluster2-rm.some.company.com:12345"),
                        "cluster2");
    Assert.assertEquals(clustersNames.getClusterName("cluster1-rm.some.company.com"),
                        "cluster1-rm.some.company.com");
    Assert.assertEquals(clustersNames.getClusterName("http://cluster3-rm.some.company.com:12345"),
        "cluster3-rm.some.company.com");
    Assert.assertEquals(clustersNames.getClusterName("uri:fancy-uri"),
        "uri:fancy-uri");
  }
}
