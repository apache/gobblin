package gobblin.source.extractor.extract.google.webmaster;

import gobblin.source.extractor.extract.google.webmaster.GoogleWebmasterFilter.FilterOperator;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;


/**
 * Group the UrlTrie nodes into groups of given size by utilizing a UrlTriIterator.
 *
 * Iterating the groups with a Triple type return value:
 *
 * Triple.1 is this group's root URL. The full URL to the root node of the group.
 * Triple.2 is the FilterOperator type for this group.
 *      case 1. If the descendants of this group is <= groupSize, the operator is FilterOperator.CONTAINS. This is a real group.
 *      case 2. Otherwise, the node will only be returned if it exists, with the operator FilterOperator.EQUALS. This group is actually a single value.
 * Triple.3 is the root node of this group.
 */
public class UrlTriePrefixGrouper implements Iterator<Triple<String, FilterOperator, UrlTrieNode>> {

  private final int _groupSize;
  private final UrlTrie _trie;
  private final Iterator<Pair<String, UrlTrieNode>> _iterator;
  private Triple<String, FilterOperator, UrlTrieNode> _retVal;

  public UrlTriePrefixGrouper(UrlTrie trie, int groupSize) {
    _trie = trie;
    _groupSize = groupSize;
    _iterator = new UrlTriePostOrderIterator(trie, groupSize);
  }

  @Override
  public boolean hasNext() {
    if (_retVal != null) {
      return true;
    }

    while (_iterator.hasNext() && _retVal == null) {
      Pair<String, UrlTrieNode> nextPair = _iterator.next();
      UrlTrieNode nextNode = nextPair.getRight();
      if (nextNode.getSize() <= _groupSize) {
        _retVal = Triple.of(nextPair.getLeft(), FilterOperator.CONTAINS, nextNode);
        return true;
      } else if (nextNode.isExist()) {
        _retVal = Triple.of(nextPair.getLeft(), FilterOperator.EQUALS, nextNode);
        return true;
      }
    }
    return false;
  }

  @Override
  public Triple<String, FilterOperator, UrlTrieNode> next() {
    if (hasNext()) {
      Triple<String, FilterOperator, UrlTrieNode> retVal = _retVal;
      _retVal = null;
      return retVal;
    }
    throw new NoSuchElementException();
  }

  public UrlTrie getTrie() {
    return _trie;
  }

  /**
   * Get the detailed pages under this group
   */
  public static ArrayList<String> groupToPages(Triple<String, FilterOperator, UrlTrieNode> group) {
    ArrayList<String> ret = new ArrayList<>();
    if (group.getMiddle().equals(FilterOperator.EQUALS)) {
      if (group.getRight().isExist()) {
        ret.add(group.getLeft());
      }
    } else if (group.getMiddle().equals(FilterOperator.CONTAINS)) {
      UrlTrie trie = new UrlTrie(group.getLeft(), group.getRight());
      Iterator<Pair<String, UrlTrieNode>> iterator = new UrlTriePostOrderIterator(trie, 1);
      while (iterator.hasNext()) {
        Pair<String, UrlTrieNode> next = iterator.next();
        if (next.getRight().isExist()) {
          ret.add(next.getLeft());
        }
      }
    }
    return ret;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
