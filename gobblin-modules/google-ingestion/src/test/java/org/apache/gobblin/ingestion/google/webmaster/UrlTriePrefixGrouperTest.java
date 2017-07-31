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
import java.util.List;

import org.apache.commons.lang3.tuple.Triple;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.ingestion.google.webmaster.GoogleWebmasterFilter.FilterOperator;


@Test(groups = {"gobblin.source.extractor.extract.google.webmaster"})
public class UrlTriePrefixGrouperTest {
  private String _property = "www.linkedin.com/";

  /**
   * The trie is:
   *     /
   *     0
   *  1*   2*
   */
  @Test
  public void testGrouping1() {
    UrlTrie trie = new UrlTrie(_property, Arrays.asList(_property + "01", _property + "02"));
    UrlTriePrefixGrouper grouper = new UrlTriePrefixGrouper(trie, 1);
    ArrayList<String> chars = new ArrayList<>();
    ArrayList<FilterOperator> operators = new ArrayList<>();

    while (grouper.hasNext()) {
      Triple<String, FilterOperator, UrlTrieNode> group = grouper.next();
      chars.add(group.getLeft());
      operators.add(group.getMiddle());
    }
    Assert.assertEquals(new String[]{_property + "01", _property + "02"}, chars.toArray());
    Assert.assertEquals(new FilterOperator[]{FilterOperator.CONTAINS, FilterOperator.CONTAINS}, operators.toArray());
  }

  /**
   * The trie is:
   *     /
   *     0*
   *  1*    2*
   */
  @Test
  public void testGrouping2() {
    UrlTrie trie = new UrlTrie(_property, Arrays.asList(_property + "0", _property + "01", _property + "02"));
    UrlTriePrefixGrouper grouper = new UrlTriePrefixGrouper(trie, 1);
    ArrayList<String> chars = new ArrayList<>();
    ArrayList<FilterOperator> operators = new ArrayList<>();

    while (grouper.hasNext()) {
      Triple<String, FilterOperator, UrlTrieNode> group = grouper.next();
      chars.add(group.getLeft());
      operators.add(group.getMiddle());
    }
    Assert.assertEquals(new String[]{_property + "01", _property + "02", _property + "0"}, chars.toArray());
    Assert.assertEquals(new FilterOperator[]{FilterOperator.CONTAINS, FilterOperator.CONTAINS, FilterOperator.EQUALS},
        operators.toArray());
  }

  /**
   * The trie is:
   *     /
   *  0  1  2
   * 3 4   5 6
   *       7
   */
  @Test
  public void testTrie2GroupingWithSize3() {
    UrlTrie trie = UrlTriePostOrderIteratorTest.getUrlTrie2(_property);
    UrlTriePrefixGrouper grouper = new UrlTriePrefixGrouper(trie, 3);
    ArrayList<String> chars = new ArrayList<>();
    ArrayList<FilterOperator> operators = new ArrayList<>();

    Triple<String, FilterOperator, UrlTrieNode> group = null;
    while (grouper.hasNext()) {
      group = grouper.next();
      chars.add(group.getLeft());
      operators.add(group.getMiddle());
    }
    Assert.assertEquals(
        new String[]{_property + "0", _property + "1", _property + "25", _property + "26", _property + "2"},
        chars.toArray());
    Assert.assertEquals(
        new FilterOperator[]{FilterOperator.CONTAINS, FilterOperator.CONTAINS, FilterOperator.CONTAINS, FilterOperator.CONTAINS, FilterOperator.EQUALS},
        operators.toArray());

    //The group is at www.linkedin.com/2 in the end with operator EQUALS
    ArrayList<String> pages = UrlTriePrefixGrouper.groupToPages(group);
    Assert.assertEquals(pages.toArray(), new String[]{_property + "2"});
  }

  @Test
  public void testGroupToPagesWithContainsOperator() {
    List<String> pages = Arrays.asList(_property + "13", _property + "14");
    UrlTrie trie = new UrlTrie(_property, pages);
    ArrayList<String> actual =
        UrlTriePrefixGrouper.groupToPages(Triple.of(_property, FilterOperator.CONTAINS, trie.getRoot()));
    Assert.assertEquals(actual.toArray(), pages.toArray());
  }

  @Test
  public void testGroupToPagesWithContainsOperator2() {
    List<String> pages = Arrays.asList(_property + "13", _property + "14", _property + "1", _property + "1");
    UrlTrie trie = new UrlTrie(_property, pages);
    ArrayList<String> actual =
        UrlTriePrefixGrouper.groupToPages(Triple.of(_property, FilterOperator.CONTAINS, trie.getRoot()));
    Assert.assertEquals(actual.toArray(), new String[]{_property + "13", _property + "14", _property + "1"});
  }

  @Test
  public void testGroupToPagesWithEqualsOperator() {
    List<String> pages = Arrays.asList(_property + "13", _property + "14");
    UrlTrie trie1 = new UrlTrie(_property, pages);
    ArrayList<String> actual1 =
        UrlTriePrefixGrouper.groupToPages(Triple.of(_property, FilterOperator.EQUALS, trie1.getRoot()));
    Assert.assertEquals(actual1.size(), 0);

    List<String> pagesWithRoot = new ArrayList<>();
    pagesWithRoot.addAll(pages);
    pagesWithRoot.add(_property);
    UrlTrie trie2 = new UrlTrie(_property, pagesWithRoot);
    ArrayList<String> actual2 =
        UrlTriePrefixGrouper.groupToPages(Triple.of(_property, FilterOperator.EQUALS, trie2.getRoot()));
    Assert.assertEquals(actual2.toArray(), new String[]{_property});
  }

  @Test
  public void testWhenTrieSizeLessThanGroupSize1() {
    List<String> pages = Arrays.asList(_property + "13");
    UrlTrie trie1 = new UrlTrie(_property, pages);
    UrlTriePrefixGrouper grouper = new UrlTriePrefixGrouper(trie1, 1);
    Triple<String, FilterOperator, UrlTrieNode> next = grouper.next();
    Assert.assertEquals(next.getLeft(), _property);
    Assert.assertEquals(next.getMiddle(), FilterOperator.CONTAINS);
    Assert.assertEquals(next.getRight().getValue(), Character.valueOf('/'));
    Assert.assertFalse(next.getRight().isExist());
    Assert.assertFalse(grouper.hasNext());
  }

  @Test
  public void testWhenTrieSizeLessThanGroupSize2() {
    List<String> pages = Arrays.asList(_property + "13");
    UrlTrie trie1 = new UrlTrie(_property, pages);
    UrlTriePrefixGrouper grouper = new UrlTriePrefixGrouper(trie1, 2);
    Triple<String, FilterOperator, UrlTrieNode> next = grouper.next();
    Assert.assertEquals(next.getLeft(), _property);
    Assert.assertEquals(next.getMiddle(), FilterOperator.CONTAINS);
    Assert.assertEquals(next.getRight().getValue(), Character.valueOf('/'));
    Assert.assertFalse(next.getRight().isExist());
    Assert.assertFalse(grouper.hasNext());
  }

//  @Test
//  public void fun() throws FileNotFoundException {
//    UrlTrie trie = new UrlTrie("https://" + _property, new ArrayList<String>());
//    FileReader fileReader = new FileReader(new File("/Users/chguo/projects/seo/src/main/java/test/output2.txt"));
//    try (BufferedReader br = new BufferedReader(fileReader)) {
//      String line;
//      while ((line = br.readLine()) != null) {
//        trie.add(line);
//      }
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//
//    UrlTriePrefixGrouper _grouper = new UrlTriePrefixGrouper(trie, 3);
////    ArrayList<String> chars = new ArrayList<>();
////    ArrayList<FilterOperator> operators = new ArrayList<>();
//    while (_grouper.hasNext()) {
//      Triple<String, FilterOperator, UrlTrieNode> group = _grouper.next();
//      System.out.println(group.getLeft() + "  " + group.getMiddle() + "  " + group.getRight().getSize());
//    }
//  }
}
