package gobblin.source.extractor.extract.google.webmaster;

import org.apache.commons.lang3.tuple.Triple;


public class UrlGrouperBuilder {

  public static UrlGrouper build(Triple<String, GoogleWebmasterFilter.FilterOperator, UrlTrieNode> job, int groupSize) {
    if (job.getMiddle().equals(GoogleWebmasterFilter.FilterOperator.EQUALS)) {
      return new SingleUrlGrouper(job);
    } else {
      UrlTrie trie = new UrlTrie(job.getLeft(), job.getRight());
      return new UrlTriePrefixGrouper(trie, groupSize);
    }
  }
}
