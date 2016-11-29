package gobblin.source.extractor.extract.google.webmaster;

import java.util.Iterator;
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
public interface UrlGrouper extends Iterator<Triple<String, GoogleWebmasterFilter.FilterOperator, UrlTrieNode>> {
  int getGroupSize();
}
