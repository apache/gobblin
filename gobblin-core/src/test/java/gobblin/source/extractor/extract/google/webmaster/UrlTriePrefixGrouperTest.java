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
  public void testEmptyTrieGrouper() {
    UrlTrie trie = new UrlTrie("", new ArrayList<String>());
    UrlTriePrefixGrouper grouper = new UrlTriePrefixGrouper(trie, 1);
    Assert.assertTrue(grouper.hasNext());
    Assert.assertTrue(grouper.hasNext());
    Assert.assertTrue(grouper.hasNext());
    Assert.assertEquals(grouper.next().getRight(), trie.getRoot());
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
   *    /
   *  0  1
   *    3 4
   */
  @Test
  public void testTrie2WithGroupSize1() {
    UrlTrie trie =
        new UrlTrie(_property, Arrays.asList(_property + "1", _property + "0", _property + "13", _property + "14"));
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

  /**
   * The trie is:
   *     /
   *  0  1  2
   * 3 4   5 6
   *       7
   */
  @Test
  public void testTrie1WithGroupSize2() {
    UrlTrie trie = new UrlTrie(_property,
        Arrays.asList(_property + "26", _property + "257", _property + "25", _property + "1", _property + "0",
            _property + "2", _property + "03", _property + "04"));
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
}
