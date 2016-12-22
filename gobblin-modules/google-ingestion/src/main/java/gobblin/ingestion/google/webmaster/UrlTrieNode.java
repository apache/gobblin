package gobblin.ingestion.google.webmaster;

import java.util.TreeMap;


public class UrlTrieNode {
  public TreeMap<Character, UrlTrieNode> children = new TreeMap<>();  //immediate, first level children.
  private Character _value;
  private boolean _exist = false;
  //the count/size for all nodes with actual values/pages starting from itself and include all children, grand-children, etc...
  private int _size = 0;

  public UrlTrieNode(Character value) {
    _value = value;
  }

  public void add(String path) {
    UrlTrieNode parent = this;
    parent.increaseCount();
    for (int i = 0; i < path.length(); ++i) {
      Character c = path.charAt(i);
      UrlTrieNode child = parent.children.get(c);
      if (child == null) {
        child = new UrlTrieNode(c);
        parent.children.put(c, child);
      }
      child.increaseCount();
      parent = child;
    }
    parent._exist = true;
  }

  public UrlTrieNode getChild(String path) {
    UrlTrieNode parent = this;
    for (int i = 0; i < path.length(); ++i) {
      Character c = path.charAt(i);
      UrlTrieNode child = parent.children.get(c);
      if (child == null) {
        return null;
      }
      parent = child;
    }
    return parent;
  }

//  public UrlTrieNode nextSibling() {
//    if (_parent == null) {
//      return null;
//    }
//    Map.Entry<Character, UrlTrieNode> sibling = _parent.children.higherEntry(_value);
//    if (sibling == null) {
//      return null;
//    }
//    return sibling.getValue();
//  }

  public Character getValue() {

    return _value;
  }

  public boolean isExist() {
    return _exist;
  }

  public int getSize() {
    return _size;
  }

  public void increaseCount() {
    ++_size;
  }

  @Override
  public String toString() {
    return "UrlTrieNode{" + "_value=" + _value + ", _exist=" + _exist + ", _size=" + _size + '}';
  }
}
