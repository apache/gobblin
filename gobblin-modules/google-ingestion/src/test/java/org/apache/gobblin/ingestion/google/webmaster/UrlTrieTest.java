/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.ingestion.google.webmaster;

import java.util.ArrayList;
import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = {"gobblin.source.extractor.extract.google.webmaster"})
public class UrlTrieTest {
  @Test
  public void testTrieRoot1() {
    UrlTrie trie = new UrlTrie("", new ArrayList<String>());
    UrlTrieNode root = trie.getRoot();
    Assert.assertTrue(root.getValue() == null);
    //Assert.assertTrue(root.getParent() == null);
  }

  @Test
  public void testTrieRoot2() {
    UrlTrie trie = new UrlTrie(null, new ArrayList<String>());
    UrlTrieNode root = trie.getRoot();
    Assert.assertTrue(root.getValue() == null);
    //Assert.assertTrue(root.getParent() == null);
  }

  @Test
  public void testTrieRoot3() {
    UrlTrie trie = new UrlTrie("www.linkedin.com/", new ArrayList<String>());
    UrlTrieNode root = trie.getRoot();
    Assert.assertTrue(root.getValue().equals('/'));
    Assert.assertEquals(0, root.getSize());
    //Assert.assertTrue(root.getParent() == null);
  }

  @Test
  public void testParent() {
    UrlTrie trie = new UrlTrie("www.linkedin.com/", Arrays.asList("www.linkedin.com/in/"));
    UrlTrieNode root = trie.getRoot();
    Assert.assertEquals(1, root.getSize());
    UrlTrieNode child = root.getChild("in/");
    Assert.assertEquals(1, child.getSize());
    //Assert.assertEquals(root, child.getParent().getParent().getParent());
  }

  @Test
  public void testSiblings() {
    UrlTrie trie = new UrlTrie("https://www.linkedin.com/",
        Arrays.asList("https://www.linkedin.com/a", "https://www.linkedin.com/b"));

    UrlTrieNode root = trie.getRoot();
    //Assert.assertEquals(root.nextSibling(), null);

    UrlTrieNode bNode = root.getChild("b");
    //Assert.assertEquals(root.getChild("a").nextSibling(), bNode);
    //Assert.assertEquals(bNode.nextSibling(), null);
  }

  @Test
  public void testTrieFlat() {
    UrlTrie trie = new UrlTrie("https://www.linkedin.com/",
        Arrays.asList("https://www.linkedin.com/jobs/", "https://www.linkedin.com/in/"));

    UrlTrieNode root = trie.getRoot();

    Assert.assertTrue(root.getValue().equals('/'));
    Assert.assertEquals(2, root.children.size());
    Assert.assertFalse(root.isExist());
    Assert.assertEquals(2, root.getSize());

    // Path1
    String path1 = "jobs/";
    checkEmptyPath(trie, path1, 1);
    UrlTrieNode jobNode = trie.getChild("jobs/");
    Assert.assertTrue(jobNode.getValue().equals('/'));
    Assert.assertEquals(1, jobNode.getSize());
    Assert.assertTrue(jobNode.isExist());

    // Path2
    String path2 = "in/";
    checkEmptyPath(trie, path2, 1);
    UrlTrieNode inNode = trie.getChild("in/");
    Assert.assertTrue(inNode.getValue().equals('/'));
    Assert.assertEquals(1, inNode.getSize());
    Assert.assertTrue(inNode.isExist());
  }

  @Test
  public void testDuplicate() {
    UrlTrie trie = new UrlTrie("https://www.linkedin.com/",
        Arrays.asList("https://www.linkedin.com/", "https://www.linkedin.com/", "https://www.linkedin.com/in/"));

    UrlTrieNode root = trie.getRoot();

    Assert.assertTrue(root.getValue().equals('/'));
    Assert.assertEquals(1, root.children.size());
    Assert.assertTrue(root.isExist());
    Assert.assertEquals(3, root.getSize());

    // Path1
    String path1 = "in/";
    checkEmptyPath(trie, path1, 1);
    UrlTrieNode inNode = trie.getChild("in/");
    Assert.assertTrue(inNode.getValue().equals('/'));
    Assert.assertEquals(1, inNode.getSize());
    Assert.assertTrue(inNode.isExist());
  }

  @Test
  public void testTrieVertical() {
    UrlTrie trie = new UrlTrie("https://www.linkedin.com/",
        Arrays.asList("https://www.linkedin.com/", "https://www.linkedin.com/in/",
            "https://www.linkedin.com/in/chenguo"));

    UrlTrieNode root = trie.getRoot();

    Assert.assertTrue(root.getValue().equals('/'));
    Assert.assertEquals(1, root.children.size());
    Assert.assertTrue(root.isExist());
    Assert.assertEquals(3, root.getSize());

    // Path1
    String path1 = "in/";
    checkEmptyPath(trie, path1, 2);
    UrlTrieNode inNode = trie.getChild("in/");
    Assert.assertTrue(inNode.getValue().equals('/'));
    Assert.assertEquals(2, inNode.getSize());
    Assert.assertTrue(inNode.isExist());

    UrlTrieNode chenguo = inNode.getChild("chenguo");
    Assert.assertEquals(root.getChild("in/chenguo"), chenguo);
    Assert.assertTrue(chenguo.getValue().equals('o'));
    Assert.assertEquals(1, chenguo.getSize());
    Assert.assertTrue(chenguo.isExist());
  }

  private void checkEmptyPath(UrlTrie trie, String path, int pathChildrenCount) {
    for (int i = 1; i < path.length(); ++i) {
      UrlTrieNode node = trie.getChild(path.substring(0, i));
      Assert.assertTrue(node.getValue().equals(path.charAt(i - 1)));
      Assert.assertEquals(pathChildrenCount, node.getSize());
      Assert.assertFalse(node.isExist());
    }
  }
}
