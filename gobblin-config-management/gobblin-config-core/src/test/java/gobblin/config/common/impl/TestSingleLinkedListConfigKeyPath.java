package gobblin.config.common.impl;

import java.util.HashSet;
import java.util.Set;

import gobblin.config.store.api.ConfigKeyPath;

import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = { "gobblin.config.common.impl" })
public class TestSingleLinkedListConfigKeyPath {
  @Test
  public void testRootPath() {
    Assert.assertEquals(SingleLinkedListConfigKeyPath.ROOT.getAbsolutePathString(), "/");
    Assert.assertEquals(SingleLinkedListConfigKeyPath.ROOT.getOwnPathName(), "");
  }

  @Test(expectedExceptions = java.lang.UnsupportedOperationException.class)
  public void testGetParentOfRoot() {
    SingleLinkedListConfigKeyPath.ROOT.getParent();
  }

  @Test
  public void testNonRoot() {
    ConfigKeyPath data = SingleLinkedListConfigKeyPath.ROOT.createChild("data");
    Assert.assertEquals(data.getAbsolutePathString(), "/data");

    ConfigKeyPath profile = data.createChild("databases").createChild("identity").createChild("profile");
    Assert.assertEquals(profile.toString(), "/data/databases/identity/profile");
  }

  @Test
  public void testHash() {
    ConfigKeyPath data = SingleLinkedListConfigKeyPath.ROOT.createChild("data");

    ConfigKeyPath profile1 = data.createChild("databases").createChild("identity").createChild("profile");
    ConfigKeyPath profile2 = data.createChild("databases").createChild("identity").createChild("profile");

    Assert.assertFalse(profile1 == profile2);
    Assert.assertTrue(profile1.equals(profile2));
    Assert.assertEquals(profile1.hashCode(), profile2.hashCode());

    Set<ConfigKeyPath> testSet = new HashSet<ConfigKeyPath>();
    testSet.add(profile1);
    Assert.assertTrue(testSet.contains(profile2));
  }
}
