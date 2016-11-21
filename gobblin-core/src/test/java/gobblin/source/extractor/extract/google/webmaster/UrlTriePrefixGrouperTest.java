package gobblin.source.extractor.extract.google.webmaster;

import gobblin.source.extractor.extract.google.webmaster.GoogleWebmasterFilter.FilterOperator;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.tuple.Triple;
import org.testng.Assert;
import org.testng.annotations.Test;


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

    //The group is at www.linkedin.com/2 in the end.
    ArrayList<String> pages = UrlTriePrefixGrouper.groupToPages(group);
    Assert.assertEquals(pages.toArray(),
        new String[]{_property + "257", _property + "25", _property + "26", _property + "2"});
  }

  @Test
  public void testGroupToPages() {
    List<String> pages = Arrays.asList(_property + "13", _property + "14");
    UrlTrie trie = new UrlTrie(_property, pages);
    ArrayList<String> actual = UrlTriePrefixGrouper.groupToPages(_property, trie.getRoot());
    Assert.assertEquals(actual.toArray(), pages.toArray());
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
