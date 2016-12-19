package gobblin.ingestion.google.webmaster;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;


public class UrlTriePrefixGrouper implements UrlGrouper {

  private final int _groupSize;
  private final UrlTrie _trie;
  private final Iterator<Pair<String, UrlTrieNode>> _iterator;
  private Triple<String, GoogleWebmasterFilter.FilterOperator, UrlTrieNode> _retVal;

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
        _retVal = Triple.of(nextPair.getLeft(), GoogleWebmasterFilter.FilterOperator.CONTAINS, nextNode);
        return true;
      } else if (nextNode.isExist()) {
        _retVal = Triple.of(nextPair.getLeft(), GoogleWebmasterFilter.FilterOperator.EQUALS, nextNode);
        return true;
      }
    }
    return false;
  }

  @Override
  public Triple<String, GoogleWebmasterFilter.FilterOperator, UrlTrieNode> next() {
    if (hasNext()) {
      Triple<String, GoogleWebmasterFilter.FilterOperator, UrlTrieNode> retVal = _retVal;
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
  public static ArrayList<String> groupToPages(Triple<String, GoogleWebmasterFilter.FilterOperator, UrlTrieNode> group) {
    ArrayList<String> ret = new ArrayList<>();
    if (group.getMiddle().equals(GoogleWebmasterFilter.FilterOperator.EQUALS)) {
      if (group.getRight().isExist()) {
        ret.add(group.getLeft());
      }
    } else if (group.getMiddle().equals(GoogleWebmasterFilter.FilterOperator.CONTAINS)) {
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

  @Override
  public int getGroupSize() {
    return _groupSize;
  }
}
