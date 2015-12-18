package gobblin.config.common.impl;

import gobblin.config.store.api.ConfigKeyPath;

import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = { "gobblin.config.common.impl" })

public class TestSingleLinkedListConfigKeyPath {
  
  @Test
  public void testAll(){
    ConfigKeyPath data = SingleLinkedListConfigKeyPath.ROOT.createChild("data");
    Assert.assertEquals(data.getAbsolutePathString(), "/data");
    
    ConfigKeyPath profile = data.createChild("databases").createChild("identity").createChild("profile");
    Assert.assertEquals(profile.toString(), "/data/databases/identity/profile");
  }
}
