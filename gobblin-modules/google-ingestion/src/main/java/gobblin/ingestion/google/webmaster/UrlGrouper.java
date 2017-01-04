package gobblin.ingestion.google.webmaster;

import java.util.Iterator;

import org.apache.commons.lang3.tuple.Triple;


/**
 * Package the URL pages/nodes into groups given the group size while traversing the UrlTrie by utilizing a TrieIterator. If the current node is not a "leaf" node defined by the TrieIterator, then a "fake" group of size 1 will be created by only including this node.
 *
 * Iterating the groups with a Triple type return value:
 *
 * Triple.1 is this group's root URL. The full URL to the root node of the group.
 * Triple.2 is the FilterOperator type for this group.
 *      case 1. If the descendants of this group is <= groupSize, the operator is FilterOperator.CONTAINS. This is a real group.
 *      case 2. Otherwise, the node will only be returned if it exists, with the operator FilterOperator.EQUALS. This group is actually a single value.
 * Triple.3 is the root node of this group.
 */
public interface UrlGrouper extends Iterator<Triple<String, GoogleWebmasterFilter.FilterOperator, UrlTrieNode>> {
  int getGroupSize();
}
