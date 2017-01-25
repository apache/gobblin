package gobblin.ingestion.google.adwords;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class GoogleAdWordsExtractorIterator implements Iterator<String[]> {
  private GoogleAdWordsReportDownloader _googleAdWordsReportDownloader;
  private Collection<String> _accounts;
  private Thread _producerThread;
  private LinkedBlockingDeque<String[]> _reportRows = new LinkedBlockingDeque<>(2000);

  public GoogleAdWordsExtractorIterator(GoogleAdWordsReportDownloader googleAdWordsReportDownloader,
      Collection<String> accounts) {
    _googleAdWordsReportDownloader = googleAdWordsReportDownloader;
    _accounts = accounts;
  }

  @Override
  public boolean hasNext() {
    initialize();
    if (!_reportRows.isEmpty()) {
      return true;
    }
    try {
      String[] next = _reportRows.poll(1, TimeUnit.SECONDS);
      while (next == null) {
        if (_producerThread.isAlive()) {
          //Job not done yet. Keep waiting.
          next = _reportRows.poll(1, TimeUnit.SECONDS);
        } else {
          log.info("Producer job has finished. No more query data in the queue.");
          return false;
        }
      }
      //Must put it back. Implement in this way because LinkedBlockingDeque doesn't support blocking peek.
      _reportRows.putFirst(next);
      return true;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void initialize() {
    if (_producerThread == null) {
      _producerThread = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            _googleAdWordsReportDownloader.downloadAllReports(_accounts, _reportRows);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      });
      _producerThread.start();
    }
  }

  @Override
  public String[] next() {
    if (hasNext()) {
      return _reportRows.remove();
    }
    throw new NoSuchElementException();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
