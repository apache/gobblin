package gobblin.source.extractor.extract.google.webmaster;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.commons.lang3.tuple.Pair;


/**
 * Transverse the tree in post order.
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
    Preconditions.checkArgument(groupSize >= 0);
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

    while (!_unprocessed.isEmpty() || _currentNode != null) {
      if (_currentNode != null) {
        //push current and move to left.
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

        if (!peekNode.children.isEmpty() && peekNode.children.lastEntry().getValue() != _lastVisited) {
          _currentNode = peekNode;
        } else {
          _toReturn = _unprocessed.pop(); //we found the next one.
          _currentPrefixSb.setLength(_currentPrefixSb.length() - 1);

          UrlTrieNode parent = _unprocessed.peek();
          if (parent == null) {
            return true; //we've got the last one.
          }
          Map.Entry<Character, UrlTrieNode> sibling = parent.children.higherEntry(_toReturn.getValue());
          if (sibling != null) {
            _currentNode = sibling.getValue();
          }
          return true;
        }
      }
    }
    return false;
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

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
