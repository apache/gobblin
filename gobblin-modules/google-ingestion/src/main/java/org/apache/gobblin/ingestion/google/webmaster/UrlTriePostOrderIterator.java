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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.lang3.tuple.Pair;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;


/**
 * This is a post-order iterator that traverses the nodes on the URL trie with a stopping rule, which is, it will not go deeper into the nodes whose size(defined as the number of descendant URLs and itself if itself is a URL page) is less than or equal to the stopping size. In other words, those nodes with size less than or equal to the stopping size will be treated as leaf nodes.
 *
 * Iteration value:
 * Pair.1 is the full path to current node.
 * Pair.2 is current node.
 */
public class UrlTriePostOrderIterator implements Iterator<Pair<String, UrlTrieNode>> {

  private final int _groupSize;
  private final StringBuilder _currentPrefixSb;
  private Deque<UrlTrieNode> _unprocessed = new ArrayDeque<>();
  private UrlTrieNode _currentNode;
  private UrlTrieNode _lastVisited = null;
  private UrlTrieNode _toReturn;

  public UrlTriePostOrderIterator(UrlTrie trie, int stoppingSize) {
    Preconditions.checkArgument(stoppingSize > 0);
    _currentNode = trie.getRoot();
    String prefix = trie.getPrefix();
    _currentPrefixSb = new StringBuilder();
    if (prefix != null) {
      _currentPrefixSb.append(prefix);
    }
    _groupSize = stoppingSize;
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

    //This case happens when the whole trie has fewer URLs than the group size
    if (_lastVisited == null && _currentNode.getSize() > 0) {
      //_currentNode is now at the root node, which is a leaf by the iterator's definition
      _toReturn = _currentNode;
      return true;
    }
    return false;
  }

  /**
   * A node is a stopping node, from which you cannot go deeper, if
   *   1. this node is null
   *   2. this node has descendants <= groupSize, but this node is returnable
   */
  private boolean isStoppingNode(UrlTrieNode node) {
    return node == null || node.getSize() <= _groupSize;
  }

  @Override
  public Pair<String, UrlTrieNode> next() {
    if (hasNext()) {
      _lastVisited = _toReturn;
      _toReturn = null;
      return Pair.of(_currentPrefixSb.toString() + _lastVisited.getValue(), _lastVisited);
    }
    throw new NoSuchElementException();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
