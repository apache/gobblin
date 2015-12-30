package gobblin.config.common.impl;

import gobblin.config.store.api.ConfigKeyPath;

import java.util.HashSet;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link LinkedListConfigKeyPath}
 */
public class LinkedListConfigKeyPathTest {

  @Test
  public void testAttributes() {
    ConfigKeyPath rootPath = LinkedListConfigKeyPath.getRootPath();
    Assert.assertEquals(rootPath.getOwnPathName(), "");
    try {
      rootPath.getParent();
      Assert.fail("should not reach this");
    }
    catch (UnsupportedOperationException e) {
      // OK
    }
    Assert.assertEquals(rootPath.getAbsolutePathString(), "/");
    Assert.assertTrue(rootPath.isRootPath());

    ConfigKeyPath path1 = rootPath.createChild("a");
    Assert.assertEquals(path1.getOwnPathName(), "a");
    Assert.assertEquals(path1.getParent(), rootPath);
    Assert.assertEquals(path1.getAbsolutePathString(), "/a");
    Assert.assertFalse(path1.isRootPath());

    ConfigKeyPath path2 = path1.createChild("b");
    Assert.assertEquals(path2.getOwnPathName(), "b");
    Assert.assertEquals(path2.getParent(), path1);
    Assert.assertEquals(path2.getAbsolutePathString(), "/a/b");
    Assert.assertFalse(path2.isRootPath());

    ConfigKeyPath path22 = path1.createChild("b");
    Assert.assertEquals(path22, path2);

    Set<ConfigKeyPath> pathSet = new HashSet<>();
    pathSet.add(path2);
    Assert.assertTrue(pathSet.contains(path22));
  }

  @Test
  public void testCreate() {
    LinkedListConfigKeyPath path = LinkedListConfigKeyPath.createFromPathString("/ABC/DEF/GHI");
    Assert.assertEquals(path.getOwnPathName(), "GHI");
    Assert.assertEquals(path.getAbsolutePathString(), "/ABC/DEF/GHI");
    Assert.assertEquals(path.getParent().getOwnPathName(), "DEF");
    Assert.assertEquals(path.getParent().getAbsolutePathString(), "/ABC/DEF");
    Assert.assertEquals(path.getParent().getParent().getOwnPathName(), "ABC");
    Assert.assertEquals(path.getParent().getParent().getAbsolutePathString(), "/ABC");
    Assert.assertTrue(path.getParent().getParent().getParent().isRootPath());

    LinkedListConfigKeyPath path2 = LinkedListConfigKeyPath.createFromPathString("/ABC//DEF/GHI/");
    Assert.assertEquals(path2, path);

    Assert.assertTrue(LinkedListConfigKeyPath.createFromPathString("/").isRootPath());

    try {
      LinkedListConfigKeyPath.createFromPathString("a/b/c");
      Assert.fail("exception expected");
    }
    catch (IllegalArgumentException e) {
      // OK
    }

    path2 = LinkedListConfigKeyPath.createFromURIString("/ABC/DEF/GHI");
    Assert.assertEquals(path2, path);

    path2 = LinkedListConfigKeyPath.createFromURIString("scheme:/ABC/DEF/GHI");
    Assert.assertEquals(path2, path);

    path2 = LinkedListConfigKeyPath.createFromURIString("scheme://some.server/ABC/DEF/GHI");
    Assert.assertEquals(path2, path);

    Assert.assertTrue(LinkedListConfigKeyPath.createFromURIString("scheme://some.server/").isRootPath());
  }
}
