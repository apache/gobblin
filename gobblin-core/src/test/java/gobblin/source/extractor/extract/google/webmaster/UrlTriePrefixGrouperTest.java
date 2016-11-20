package gobblin.source.extractor.extract.google.webmaster;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.testng.Assert;
import org.testng.annotations.Test;
import gobblin.source.extractor.extract.google.webmaster.GoogleWebmasterFilter.FilterOperator;


@Test(groups = {"gobblin.source.extractor.extract.google.webmaster"})
public class UrlTriePrefixGrouperTest {
  private String _property = "www.linkedin.com/";

  @Test
  public void testEmptyTrie1GrouperWithSize1() {
    UrlTrie trie = new UrlTrie("", new ArrayList<String>());
    UrlTriePrefixGrouper grouper = new UrlTriePrefixGrouper(trie, 1);
    Assert.assertFalse(grouper.hasNext());
  }

  @Test
  public void testEmptyTrie2GrouperWithSize1() {
    UrlTrie trie = new UrlTrie(_property, new ArrayList<String>());
    UrlTriePrefixGrouper grouper = new UrlTriePrefixGrouper(trie, 1);
    Assert.assertFalse(grouper.hasNext());
  }

  /**
   * The trie is:
   *  /
   *  0
   *  1
   *  2
   */
  @Test
  public void testVerticalTrie1TraversalWithGroupSize1() {
    UrlTrie trie = new UrlTrie(_property, Arrays.asList(_property + "0", _property + "01", _property + "012"));
    UrlTriePrefixGrouper grouper = new UrlTriePrefixGrouper(trie, 1);
    ArrayList<String> chars = new ArrayList<>();
    while (grouper.hasNext()) {
      Pair<String, UrlTrieNode> next = grouper.next();
      Character value = next.getRight().getValue();
      chars.add(next.getLeft() + value);
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
  public void testVerticalTrie1TraversalWithGroupSize2() {
    UrlTrie trie = new UrlTrie(_property, Arrays.asList(_property + "0", _property + "01", _property + "012"));
    UrlTriePrefixGrouper grouper = new UrlTriePrefixGrouper(trie, 2);
    ArrayList<String> chars = new ArrayList<>();
    while (grouper.hasNext()) {
      Pair<String, UrlTrieNode> next = grouper.next();
      Character value = next.getRight().getValue();
      chars.add(next.getLeft() + value);
    }
    Assert.assertEquals(new String[]{_property + "01", _property + "0", _property}, chars.toArray());
  }

  @Test
  public void testTrie1TraversalWithGroupSize1() {
    UrlTrie trie = getUrlTrie1();
    UrlTriePrefixGrouper grouper = new UrlTriePrefixGrouper(trie, 1);
    ArrayList<String> chars = new ArrayList<>();
    while (grouper.hasNext()) {
      Pair<String, UrlTrieNode> next = grouper.next();
      Character value = next.getRight().getValue();
      chars.add(next.getLeft() + value);
    }
    Assert.assertEquals(new String[]{_property + "0", _property + "13", _property + "14", _property + "1", _property},
        chars.toArray());
  }

  @Test
  public void testTrie2TraversalWithGroupSize1() {
    UrlTrie trie = getUrlTrie2();
    UrlTriePrefixGrouper grouper = new UrlTriePrefixGrouper(trie, 1);
    ArrayList<String> chars = new ArrayList<>();
    while (grouper.hasNext()) {
      Pair<String, UrlTrieNode> next = grouper.next();
      Character value = next.getRight().getValue();
      chars.add(next.getLeft() + value);
    }
    Assert.assertEquals(new String[]{
            _property + "03",
            _property + "04",
            _property + "0",
            _property + "1", _property + "257", _property + "25", _property + "26", _property + "2", _property},
        chars.toArray());
  }

  @Test
  public void testTrie2TraversalWithGroupSize2() {
    UrlTrie trie = getUrlTrie2();
    UrlTriePrefixGrouper grouper = new UrlTriePrefixGrouper(trie, 2);
    ArrayList<String> chars = new ArrayList<>();
    while (grouper.hasNext()) {
      Pair<String, UrlTrieNode> next = grouper.next();
      Character value = next.getRight().getValue();
      chars.add(next.getLeft() + value);
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
  public void testTrie2TraversalWithGroupSize3() {
    UrlTrie trie = getUrlTrie2();
    UrlTriePrefixGrouper grouper = new UrlTriePrefixGrouper(trie, 3);
    ArrayList<String> chars = new ArrayList<>();
    while (grouper.hasNext()) {
      Pair<String, UrlTrieNode> next = grouper.next();
      Character value = next.getRight().getValue();
      chars.add(next.getLeft() + value);
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
    Triple<String, FilterOperator, UrlTrieNode> group = grouper.nextGroup();
    while (group != null) {
      chars.add(group.getLeft());
      operators.add(group.getMiddle());
      group = grouper.nextGroup();
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
    Triple<String, FilterOperator, UrlTrieNode> group = grouper.nextGroup();
    while (group != null) {
      chars.add(group.getLeft());
      operators.add(group.getMiddle());
      group = grouper.nextGroup();
    }
    Assert.assertEquals(new String[]{_property + "01", _property + "02", _property + "0"}, chars.toArray());
    Assert.assertEquals(new FilterOperator[]{FilterOperator.CONTAINS, FilterOperator.CONTAINS, FilterOperator.EQUALS},
        operators.toArray());
  }

  @Test
  public void testTrie2GroupingWithSize3() {
    UrlTrie trie = getUrlTrie2();
    UrlTriePrefixGrouper grouper = new UrlTriePrefixGrouper(trie, 3);
    ArrayList<String> chars = new ArrayList<>();
    ArrayList<FilterOperator> operators = new ArrayList<>();
    Triple<String, FilterOperator, UrlTrieNode> group = grouper.nextGroup();
    while (group != null) {
      chars.add(group.getLeft());
      operators.add(group.getMiddle());
      System.out.println(group.getLeft());
      group = grouper.nextGroup();
    }
    Assert.assertEquals(
        new String[]{_property + "0", _property + "1", _property + "25", _property + "26", _property + "2"},
        chars.toArray());
    Assert.assertEquals(
        new FilterOperator[]{FilterOperator.CONTAINS, FilterOperator.EQUALS, FilterOperator.CONTAINS, FilterOperator.CONTAINS, FilterOperator.EQUALS},
        operators.toArray());
  }

  /**
   * The trie is:
   *    /
   *  0  1
   *    3 4
   */
  private UrlTrie getUrlTrie1() {
    return new UrlTrie(_property, Arrays.asList(_property + "1", _property + "0", _property + "13", _property + "14"));
  }

  /**
   * The trie is:
   *     /
   *  0  1  2
   * 3 4   5 6
   *       7
   */
  private UrlTrie getUrlTrie2() {
    return new UrlTrie(_property,
        Arrays.asList(_property + "26", _property + "257", _property + "25", _property + "1", _property + "0",
            _property + "2", _property + "03", _property + "04"));
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
//    UrlTriePrefixGrouper grouper = new UrlTriePrefixGrouper(trie, 17);
//    ArrayList<String> chars = new ArrayList<>();
//    ArrayList<FilterOperator> operators = new ArrayList<>();
//    Triple<String, FilterOperator, UrlTrieNode> group = grouper.nextGroup();
//    while (group != null) {
//      //chars.add(group.getLeft());
//      //operators.add(group.getMiddle());
//      System.out.println(group.getLeft() + "  " + group.getMiddle() + "  " + group.getRight().getDescendants());
//      group = grouper.nextGroup();
//    }
//  }
}
