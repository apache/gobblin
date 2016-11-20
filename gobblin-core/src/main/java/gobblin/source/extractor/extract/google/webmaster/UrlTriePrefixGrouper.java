package gobblin.source.extractor.extract.google.webmaster;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.commons.lang3.tuple.Pair;
import gobblin.source.extractor.extract.google.webmaster.GoogleWebmasterFilter.FilterOperator;
import org.apache.commons.lang3.tuple.Triple;


/**
 * Transverse the trie in post order and group the leaves together into group size.
 *
 * If see a node with count <= groupsize, find a group.
 * If see a node with count > groupsize && node.isExist, find a single value.
 *
 * Group the Trie into those groups.
 */
public class UrlTriePrefixGrouper implements Iterator<Pair<String, UrlTrieNode>> {

  private final int _groupSize;
  private final StringBuilder _currentPrefixSb;
  private Deque<UrlTrieNode> _unprocessed = new ArrayDeque<>();
  private UrlTrieNode _currentNode;
  private UrlTrieNode _lastVisited = null;
  private UrlTrieNode _toReturn;

  public UrlTriePrefixGrouper(UrlTrie trie, int groupSize) {
    Preconditions.checkArgument(groupSize > 0);
    _currentNode = trie.getRoot();
    String prefix = trie.getPrefix();
    _currentPrefixSb = new StringBuilder();
    if (prefix != null) {
      _currentPrefixSb.append(prefix);
    }
    _groupSize = groupSize;
  }

  @Override
  public boolean hasNext() {
    if (_toReturn != null) {
      return true;
    }

    while (!_unprocessed.isEmpty() || !isStoppingNode(_currentNode)) {
      if (!isStoppingNode(_currentNode)) {
        //keep going down if not at leaf
        _unprocessed.push(_currentNode);
        _currentPrefixSb.append(_currentNode.getValue());

        Map.Entry<Character, UrlTrieNode> next = _currentNode.children.firstEntry();
        if (next == null) {
          _currentNode = null;
        } else {
          _currentNode = next.getValue();
        }
      } else {

        UrlTrieNode peekNode = _unprocessed.peek();
        if (_currentNode != null || peekNode.children.isEmpty()
            || peekNode.children.lastEntry().getValue() == _lastVisited) {
          //_currentNode is a returnable stopping node
          if (_currentNode != null) {
            _toReturn = _currentNode;
          } else {
            _toReturn = _unprocessed.pop();
            _currentPrefixSb.setLength(_currentPrefixSb.length() - 1);
          }

          //If there is no parent, it's the last one; otherwise, move to right
          UrlTrieNode parent = _unprocessed.peek();
          if (parent == null) {
            return true; //we've got the last one.
          }
          //move to the right sibling. Set to null, if there is no right sibling.
          Map.Entry<Character, UrlTrieNode> sibling = parent.children.higherEntry(_toReturn.getValue());
          if (sibling == null) {
            _currentNode = null;
          } else {
            _currentNode = sibling.getValue();
          }

          return true;
        } else {
          //hand over to the next loop to move right
          _currentNode = peekNode;
        }
      }
    }
    return false;
  }

  /**
   * A node is a stopping node, from which you cannot go deeper, if
   *   1. this node is null
   *   2. this node has descendants <= groupSize, but this node is returnable
   */
  private boolean isStoppingNode(UrlTrieNode node) {
    return node == null || node.getDescendants() <= _groupSize;
  }

  @Override
  public Pair<String, UrlTrieNode> next() {
    if (hasNext()) {
      _lastVisited = _toReturn;
      _toReturn = null;
      return Pair.of(_currentPrefixSb.toString(), _lastVisited);
    }
    throw new NoSuchElementException();
  }

  public Triple<String, FilterOperator, UrlTrieNode> nextGroup() {
    Triple<String, FilterOperator, UrlTrieNode> retVal = null;
    while (hasNext() && retVal == null) {
      Pair<String, UrlTrieNode> nextPair = next();
      UrlTrieNode nextNode = nextPair.getRight();
      if (nextNode.getDescendants() <= _groupSize) {
        retVal = Triple.of(nextPair.getLeft() + nextNode.getValue(), FilterOperator.CONTAINS, nextNode);
      } else if (nextNode.isExist()) {
        retVal = Triple.of(nextPair.getLeft() + nextNode.getValue(), FilterOperator.EQUALS, nextNode);
      }
    }
    return retVal;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
