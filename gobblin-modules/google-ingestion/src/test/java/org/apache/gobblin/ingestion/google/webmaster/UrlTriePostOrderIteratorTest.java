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

package gobblin.ingestion.google.webmaster;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = {"gobblin.source.extractor.extract.google.webmaster"})
public class UrlTriePostOrderIteratorTest {
  private String _property = "www.linkedin.com/";

  @Test
  public void testEmptyTrie1WithSize1() {
    UrlTrie trie = new UrlTrie("", new ArrayList<String>());
    UrlTriePostOrderIterator iterator = new UrlTriePostOrderIterator(trie, 1);
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testEmptyTrie2WithSize1() {
    UrlTrie trie = new UrlTrie(_property, new ArrayList<String>());
    UrlTriePostOrderIterator iterator = new UrlTriePostOrderIterator(trie, 1);
    Assert.assertFalse(iterator.hasNext());
  }

  /**
   * The trie is:
   *  /
   *  0
   *  1
   *  2
   */
  @Test
  public void testVerticalTrie1TraversalWithSize1() {
    UrlTrie trie = new UrlTrie(_property, Arrays.asList(_property + "0", _property + "01", _property + "012"));
    UrlTriePostOrderIterator iterator = new UrlTriePostOrderIterator(trie, 1);
    ArrayList<String> chars = new ArrayList<>();
    while (iterator.hasNext()) {
      Pair<String, UrlTrieNode> next = iterator.next();
      chars.add(next.getLeft());
    }
    Assert.assertEquals(new String[]{_property + "012", _property + "01", _property + "0", _property}, chars.toArray());
  }

  /**
   * The trie is:
   *  /
   *  0
   *  1
   *  2
   */
  @Test
  public void testVerticalTrie1TraversalWithSize2() {
    UrlTrie trie = new UrlTrie(_property, Arrays.asList(_property + "0", _property + "01", _property + "012"));
    UrlTriePostOrderIterator iterator = new UrlTriePostOrderIterator(trie, 2);
    ArrayList<String> chars = new ArrayList<>();
    while (iterator.hasNext()) {
      Pair<String, UrlTrieNode> next = iterator.next();
      chars.add(next.getLeft());
    }
    Assert.assertEquals(new String[]{_property + "01", _property + "0", _property}, chars.toArray());
  }

  /**
   * The trie is:
   *  /
   *  0
   *  1
   *  2
   */
  @Test
  public void testVerticalTrie1TraversalWithSize3() {
    UrlTrie trie = new UrlTrie(_property, Arrays.asList(_property + "0", _property + "01", _property + "012"));
    UrlTriePostOrderIterator iterator = new UrlTriePostOrderIterator(trie, 3);
    ArrayList<String> chars = new ArrayList<>();
    while (iterator.hasNext()) {
      Pair<String, UrlTrieNode> next = iterator.next();
      chars.add(next.getLeft());
    }
    //the root node is a leaf node
    Assert.assertEquals(new String[]{_property}, chars.toArray());
  }

  /**
   * The trie is:
   *  /
   *  0
   *  1
   *  2
   */
  @Test
  public void testVerticalTrie1TraversalWithSize4() {
    UrlTrie trie = new UrlTrie(_property, Arrays.asList(_property + "0", _property + "01", _property + "012"));
    UrlTriePostOrderIterator iterator = new UrlTriePostOrderIterator(trie, 4);
    ArrayList<String> chars = new ArrayList<>();
    while (iterator.hasNext()) {
      Pair<String, UrlTrieNode> next = iterator.next();
      chars.add(next.getLeft());
    }
    //the root node is a leaf node
    Assert.assertEquals(new String[]{_property}, chars.toArray());
  }

  @Test
  public void testTrie1TraversalWithSize1() {
    UrlTrie trie = getUrlTrie1(_property);
    UrlTriePostOrderIterator iterator = new UrlTriePostOrderIterator(trie, 1);
    ArrayList<String> chars = new ArrayList<>();
    while (iterator.hasNext()) {
      Pair<String, UrlTrieNode> next = iterator.next();
      chars.add(next.getLeft());
    }
    Assert.assertEquals(new String[]{_property + "0", _property + "13", _property + "14", _property + "1", _property},
        chars.toArray());
  }

  @Test
  public void testTrie2TraversalWithSize1() {
    UrlTrie trie = getUrlTrie2(_property);
    UrlTriePostOrderIterator iterator = new UrlTriePostOrderIterator(trie, 1);
    ArrayList<String> chars = new ArrayList<>();
    while (iterator.hasNext()) {
      Pair<String, UrlTrieNode> next = iterator.next();
      chars.add(next.getLeft());
    }
    Assert.assertEquals(new String[]{
            _property + "03",
            _property + "04",
            _property + "0",
            _property + "1", _property + "257", _property + "25", _property + "26", _property + "2", _property},
        chars.toArray());
  }

  @Test
  public void testTrie2TraversalWithSize2() {
    UrlTrie trie = getUrlTrie2(_property);
    UrlTriePostOrderIterator iterator = new UrlTriePostOrderIterator(trie, 2);
    ArrayList<String> chars = new ArrayList<>();
    while (iterator.hasNext()) {
      Pair<String, UrlTrieNode> next = iterator.next();
      chars.add(next.getLeft());
    }
    Assert.assertEquals(new String[]{//
        _property + "03", //group size 1, contains
        _property + "04", //group size 1, contains
        _property + "0", //group size 1(count is 3), equals
        _property + "1", //group size 1, contains
        _property + "25",  //group size 2, contains
        _property + "26",  //group size 1, contains
        _property + "2",  //group size 1(count is 4), equals
        _property //group size 1(count is 9), equals
    }, chars.toArray());
  }

  @Test
  public void testTrie2TraversalWithSize3() {
    UrlTrie trie = getUrlTrie2(_property);
    UrlTriePostOrderIterator iterator = new UrlTriePostOrderIterator(trie, 3);
    ArrayList<String> chars = new ArrayList<>();
    while (iterator.hasNext()) {
      Pair<String, UrlTrieNode> next = iterator.next();
      chars.add(next.getLeft());
    }
    Assert.assertEquals(new String[]{//
        _property + "0", //group size 3, contains
        _property + "1", //group size 1, contains
        _property + "25",  //group size 2, contains
        _property + "26",  //group size 1, contains
        _property + "2",  //group size 1(count is 4), equals
        _property //group size 1(count is 9), equals
    }, chars.toArray());
  }

  /**
   * The trie is:
   *    /
   *  0  1
   *    3 4
   */
  public static UrlTrie getUrlTrie1(String property) {
    return new UrlTrie(property, Arrays.asList(property + "1", property + "0", property + "13", property + "14"));
  }

  /**
   * The trie is:
   *     /
   *  0  1  2
   * 3 4   5 6
   *       7
   */
  public static UrlTrie getUrlTrie2(String property) {
    return new UrlTrie(property,
        Arrays.asList(property + "26", property + "257", property + "25", property + "1", property + "0",
            property + "2", property + "03", property + "04"));
  }
}
