package gobblin.ingestion.google.adwords;

import java.util.Collection;

import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.WorkUnitState;
import gobblin.ingestion.google.AsyncIteratorWithDataSink;
import gobblin.ingestion.google.GoggleIngestionConfigurationKeys;


@Slf4j
public class GoogleAdWordsExtractorIterator extends AsyncIteratorWithDataSink<String[]> {
  private final int _queueSize;
  private final int _queueBlockingTime;
  private GoogleAdWordsReportDownloader _googleAdWordsReportDownloader;
  private Collection<String> _accounts;

  public GoogleAdWordsExtractorIterator(GoogleAdWordsReportDownloader googleAdWordsReportDownloader,
      Collection<String> accounts, WorkUnitState state) {
    _googleAdWordsReportDownloader = googleAdWordsReportDownloader;
    _accounts = accounts;
    _queueSize = state.getPropAsInt(GoggleIngestionConfigurationKeys.SOURCE_ASYNC_ITERATOR_BLOCKING_QUEUE_SIZE, 2000);
    _queueBlockingTime =
        state.getPropAsInt(GoggleIngestionConfigurationKeys.SOURCE_ASYNC_ITERATOR_POLL_BLOCKING_TIME, 1);
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

  @Override
  protected int getPollBlockingTime() {
    return _queueBlockingTime;
  }
}
