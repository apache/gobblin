package gobblin.config.configstore.impl;

import java.util.Collection;
import java.util.Iterator;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestConfigStoreFactory {
  @Test public void testValidStore() throws Exception{
    ConfigStoreFactoryUsingProperty csf = new ConfigStoreFactoryUsingProperty("/Users/mitu/CircularDependencyTest/stores.conf");
    Collection<String> schemes = csf.getConfigStoreSchemes();
    
    Assert.assertEquals(schemes.size(), 2);
    Iterator<String> it = schemes.iterator();
    Assert.assertEquals(it.next(), "DAI-ETL");
    Assert.assertEquals(it.next(), "DALI");
  }
}
