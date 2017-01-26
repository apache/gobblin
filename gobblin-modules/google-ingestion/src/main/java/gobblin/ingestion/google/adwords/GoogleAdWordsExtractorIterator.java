package gobblin.ingestion.google.adwords;

import java.util.Collection;

import lombok.extern.slf4j.Slf4j;

import gobblin.ingestion.google.IteratorWithDataSink;


@Slf4j
public class GoogleAdWordsExtractorIterator extends IteratorWithDataSink<String[]> {
  private GoogleAdWordsReportDownloader _googleAdWordsReportDownloader;
  private Collection<String> _accounts;

  public GoogleAdWordsExtractorIterator(GoogleAdWordsReportDownloader googleAdWordsReportDownloader,
      Collection<String> accounts) {
    _googleAdWordsReportDownloader = googleAdWordsReportDownloader;
    _accounts = accounts;
  }

  @Override
  protected Runnable initializationRunnable() {
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
}
