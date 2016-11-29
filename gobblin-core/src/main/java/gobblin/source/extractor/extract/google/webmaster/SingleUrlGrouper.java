package gobblin.source.extractor.extract.google.webmaster;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import gobblin.source.extractor.extract.google.webmaster.GoogleWebmasterFilter.FilterOperator;
import java.util.NoSuchElementException;
import org.apache.commons.lang3.tuple.Triple;


public class SingleUrlGrouper implements UrlGrouper {

  private Triple<String, FilterOperator, UrlTrieNode> _job;

  public SingleUrlGrouper(Triple<String, FilterOperator, UrlTrieNode> job) {
    Preconditions.checkArgument(job.getMiddle().equals(FilterOperator.EQUALS));
    _job = job;
  }

  @Override
  public boolean hasNext() {
    return _job != null;
  }

  @Override
  public Triple<String, FilterOperator, UrlTrieNode> next() {
    if (hasNext()) {
      Triple<String, FilterOperator, UrlTrieNode> ret = _job;
      _job = null;
      return ret;
    }
    throw new NoSuchElementException();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getGroupSize() {
    return 1;
  }
}
