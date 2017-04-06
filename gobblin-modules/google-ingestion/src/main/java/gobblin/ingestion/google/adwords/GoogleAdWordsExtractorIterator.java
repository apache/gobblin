package gobblin.ingestion.google.adwords;

import java.util.Collection;

import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.WorkUnitState;
import gobblin.ingestion.google.AsyncIteratorWithDataSink;
import gobblin.ingestion.google.GoggleIngestionConfigurationKeys;


@Slf4j
public class GoogleAdWordsExtractorIterator extends AsyncIteratorWithDataSink<String[]> {
  private GoogleAdWordsReportDownloader _googleAdWordsReportDownloader;
  private Collection<String> _accounts;

  public GoogleAdWordsExtractorIterator(GoogleAdWordsReportDownloader googleAdWordsReportDownloader,
      Collection<String> accounts, WorkUnitState state) {
    super(state.getPropAsInt(GoggleIngestionConfigurationKeys.SOURCE_ASYNC_ITERATOR_BLOCKING_QUEUE_SIZE, 2000),
        state.getPropAsInt(GoggleIngestionConfigurationKeys.SOURCE_ASYNC_ITERATOR_POLL_BLOCKING_TIME, 1));
    _googleAdWordsReportDownloader = googleAdWordsReportDownloader;
    _accounts = accounts;
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
}
