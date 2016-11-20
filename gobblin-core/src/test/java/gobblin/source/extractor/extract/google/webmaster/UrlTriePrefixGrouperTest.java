package gobblin.source.extractor.extract.google.webmaster;

import java.util.ArrayList;
import java.util.Arrays;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;


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
  public void testVerticalTrie1WithGroupSize1() {
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
  public void testVerticalTrie1WithGroupSize2() {
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
  public void testTrie1WithGroupSize1() {
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
  public void testTrie2WithGroupSize1() {
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
  public void testTrie2WithGroupSize2() {
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
  public void testTrie2WithGroupSize3() {
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
}
