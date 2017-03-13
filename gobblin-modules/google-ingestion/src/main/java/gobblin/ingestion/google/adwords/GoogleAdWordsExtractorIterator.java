package gobblin.ingestion.google.adwords;

import java.util.Collection;

import lombok.extern.slf4j.Slf4j;

import gobblin.ingestion.google.AsyncIteratorWithDataSink;


@Slf4j
public class GoogleAdWordsExtractorIterator extends AsyncIteratorWithDataSink<String[]> {
  private final int _queueSize;
  private GoogleAdWordsReportDownloader _googleAdWordsReportDownloader;
  private Collection<String> _accounts;

  public GoogleAdWordsExtractorIterator(GoogleAdWordsReportDownloader googleAdWordsReportDownloader,
      Collection<String> accounts, int queueSize) {
    _googleAdWordsReportDownloader = googleAdWordsReportDownloader;
    _accounts = accounts;
    _queueSize = queueSize;
  }

  @Override
  protected Runnable getProducerRunnable() {
    return new Runnable() {
      @Override
      public void run() {
        try {
          _googleAdWordsReportDownloader.downloadAllReports(_accounts, _dataSink);
        } catch (InterruptedException e) {
          log.error(e.getMessage());
          throw new RuntimeException(e);
        }
      }
    };
  }

  @Override
  protected int getQueueSize() {
    return _queueSize;
  }
}
