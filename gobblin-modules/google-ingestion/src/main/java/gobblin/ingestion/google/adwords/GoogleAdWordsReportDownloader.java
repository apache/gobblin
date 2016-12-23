package gobblin.ingestion.google.adwords;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.ads.adwords.lib.client.AdWordsSession;
import com.google.api.ads.adwords.lib.client.reporting.ReportingConfiguration;
import com.google.api.ads.adwords.lib.jaxb.v201609.DateRange;
import com.google.api.ads.adwords.lib.jaxb.v201609.DownloadFormat;
import com.google.api.ads.adwords.lib.jaxb.v201609.ReportDefinition;
import com.google.api.ads.adwords.lib.jaxb.v201609.ReportDefinitionDateRangeType;
import com.google.api.ads.adwords.lib.jaxb.v201609.ReportDefinitionReportType;
import com.google.api.ads.adwords.lib.jaxb.v201609.Selector;
import com.google.api.ads.adwords.lib.utils.ReportDownloadResponse;
import com.google.api.ads.adwords.lib.utils.ReportDownloadResponseException;
import com.google.api.ads.adwords.lib.utils.ReportException;
import com.google.api.ads.adwords.lib.utils.v201609.ReportDownloader;
import com.google.api.ads.common.lib.exception.ValidationException;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.extract.LongWatermark;


public class GoogleAdWordsReportDownloader {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleAdWordsReportDownloader.class);
  private final static Splitter splitter = Splitter.on(",").trimResults();
  private final boolean _skipReportHeader;
  private final boolean _skipColumnHeader;
  private final boolean _skipReportSummary;
  private final boolean _includeZeroImpressions;
  private final boolean _useRawEnumValues;

  private final AdWordsSession _rootSession;
  private final ExponentialBackOff.Builder backOffBuilder =
      new ExponentialBackOff.Builder().setMaxElapsedTimeMillis(1000 * 60 * 5);
  private final List<String> _columnNames;
  private final ReportDefinitionReportType _reportType;
  private final ReportDefinitionDateRangeType _dateRangeType;
  private final String _startDate;
  private final String _endDate;
  private final boolean _dailyPartition;

  public final static DateTimeFormatter dateFormatter = DateTimeFormat.forPattern("yyyyMMdd");
  private final static DateTimeFormatter watermarkFormatter = DateTimeFormat.forPattern("yyyyMMddHHmmss");

  public GoogleAdWordsReportDownloader(AdWordsSession rootSession, WorkUnitState state,
      ReportDefinitionReportType reportType, ReportDefinitionDateRangeType dateRangeType, String schema) {
    _rootSession = rootSession;
    _reportType = reportType;
    _dateRangeType = dateRangeType;
    _columnNames = getColumnNames(schema);
    LOG.info("Downloaded fields are: " + Arrays.toString(_columnNames.toArray()));

    long lowWatermark = state.getWorkunit().getLowWatermark(LongWatermark.class).getValue();
    _startDate = dateFormatter.print(watermarkFormatter.parseDateTime(Long.toString(lowWatermark)));

    _endDate = _startDate;
    _dailyPartition = state.getPropAsBoolean(GoogleAdWordsSource.KEY_CUSTOM_DATE_DAILY, false);

    _skipReportHeader = state.getPropAsBoolean(GoogleAdWordsSource.KEY_REPORTING_SKIP_REPORT_HEADER, true);
    _skipColumnHeader = state.getPropAsBoolean(GoogleAdWordsSource.KEY_REPORTING_SKIP_COLUMN_HEADER, true);
    _skipReportSummary = state.getPropAsBoolean(GoogleAdWordsSource.KEY_REPORTING_SKIP_REPORT_SUMMARY, true);
    _useRawEnumValues = state.getPropAsBoolean(GoogleAdWordsSource.KEY_REPORTING_USE_RAW_ENUM_VALUES, false);
    _includeZeroImpressions = state.getPropAsBoolean(GoogleAdWordsSource.KEY_REPORTING_INCLUDE_ZERO_IMPRESSION, false);
  }

  private List<String> getColumnNames(String schemaString) {
    JsonArray schemaArray = new JsonParser().parse(schemaString).getAsJsonArray();
    List<String> fields = new ArrayList<>();
    for (int i = 0; i < schemaArray.size(); i++) {
      fields.add(schemaArray.get(i).getAsJsonObject().get("columnName").getAsString());
    }
    return fields;
  }

  public void downloadAllReports(Collection<String> accounts, final LinkedBlockingDeque<String[]> reportRows)
      throws InterruptedException {
    ExecutorService threadPool = Executors.newFixedThreadPool(Math.min(8, accounts.size()));

    List<Pair<String, String>> dates = getDates();

    Map<String, Future<Void>> jobs = new HashMap<>();
    for (String acc : accounts) {
      final String account = acc;
      for (Pair<String, String> dateRange : dates) {
        final Pair<String, String> range = dateRange;
        final String jobName;
        if (_dateRangeType.equals(ReportDefinitionDateRangeType.ALL_TIME)) {
          jobName = String.format("'all-time report for %s'", account);
        } else {
          jobName = String.format("'report for %s from %s to %s'", account, range.getLeft(), range.getRight());
        }

        Future<Void> job = threadPool.submit(new Callable<Void>() {
          @Override
          public Void call()
              throws ReportDownloadResponseException, InterruptedException, IOException, ReportException {

            LOG.info("Start downloading " + jobName);

            ExponentialBackOff backOff = backOffBuilder.build();
            int numberOfAttempts = 0;
            while (true) {
              ++numberOfAttempts;
              try {
                downloadReport(account, _columnNames, _dateRangeType, range.getLeft(), range.getRight(), reportRows);
                LOG.info("Successfully downloaded " + jobName);
                return null;
              } catch (ReportException e) {
                long sleepMillis = backOff.nextBackOffMillis();
                LOG.info("Downloading %s failed #%d try: %s. Will sleep for %d milliseconds.", jobName,
                    numberOfAttempts, e.getMessage(), sleepMillis);
                if (sleepMillis == BackOff.STOP) {
                  throw new ReportException(String
                      .format("Downloading %s failed after maximum elapsed millis: %d", jobName,
                          backOff.getMaxElapsedTimeMillis()), e);
                } else {
                  Thread.sleep(sleepMillis);
                }
              }
            }
          }
        });
        jobs.put(jobName, job);
        Thread.sleep(100);
      }
    }

    Map<String, Exception> failedJobs = new HashMap<>();
    for (Map.Entry<String, Future<Void>> job : jobs.entrySet()) {
      String account = job.getKey();
      try {
        job.getValue().get();
      } catch (Exception e) {
        failedJobs.put(account, e);
      }
    }

    if (!failedJobs.isEmpty()) {
      System.out.println(String.format("%d downloading jobs failed:", failedJobs.size()));
      for (Map.Entry<String, Exception> fail : failedJobs.entrySet()) {
        LOG.error(String.format("%s => %s", fail.getKey(), fail.getValue().getMessage()));
      }
    }

    LOG.info("End of downloading all reports.");
    threadPool.shutdown();
  }

  /**
   *
   * @param account the account of the report you want to download.
   * @param fields the fields of the report you want to download.
   * @param dateRangeType specify the type of the date range. Provide startDate and endDate if you are using CUSTOM_DATE
   * @param startDate start date for custom_date range type
   * @param endDate end date for custom_date range type
   * @param reportRows
   * @throws ReportDownloadResponseException This is not retryable
   * @throws ReportException ReportException represents a potentially retryable error
   */
  private void downloadReport(String account, Collection<String> fields, ReportDefinitionDateRangeType dateRangeType,
      String startDate, String endDate, LinkedBlockingDeque<String[]> reportRows)
      throws ReportDownloadResponseException, ReportException, InterruptedException {
    Selector selector = new Selector();
    selector.getFields().addAll(fields);

    String reportName;
    if (dateRangeType.equals(ReportDefinitionDateRangeType.CUSTOM_DATE)) {
      DateRange value = new DateRange();
      value.setMin(startDate);
      value.setMax(endDate);
      selector.setDateRange(value);
      reportName = String.format("%s: %s to %s", _reportType.toString(), startDate, endDate);
    } else {
      reportName = String.format("%s: ALL TIME", _reportType.toString());
    }

    ReportDefinition reportDef = new ReportDefinition();
    reportDef.setReportName(reportName);
    reportDef.setDateRangeType(dateRangeType);
    reportDef.setReportType(_reportType);
    reportDef.setDownloadFormat(DownloadFormat.GZIPPED_CSV);
    reportDef.setSelector(selector);

    //API defaults all configurations to false
    ReportingConfiguration reportConfig =
        new ReportingConfiguration.Builder().skipReportHeader(_skipReportHeader).skipColumnHeader(_skipColumnHeader)
            .skipReportSummary(_skipReportSummary)
            .useRawEnumValues(_useRawEnumValues) //return enum field values as enum values instead of display values
            .includeZeroImpressions(_includeZeroImpressions).build();

    AdWordsSession.ImmutableAdWordsSession session;
    try {
      session = _rootSession.newBuilder().withClientCustomerId(account).withReportingConfiguration(reportConfig)
          .buildImmutable();
    } catch (ValidationException e) {
      LOG.error(e.getMessage());
      throw new RuntimeException(e);
    }

    ReportDownloader downloader = new ReportDownloader(session);
    ReportDownloadResponse response = downloader.downloadReport(reportDef);

    InputStream zippedStream = response.getInputStream();
    if (zippedStream == null) {
      String message = "Got empty stream for " + reportName;
      LOG.error(message);
      throw new RuntimeException(message);
    }

    try {
      processResponse(zippedStream, reportRows, account);
    } catch (IOException e) {
      LOG.error("Failed while unzipping and printing to console for ", reportName);
      throw new RuntimeException(e);
    }
  }

  private GZIPInputStream processResponse(InputStream zippedStream, LinkedBlockingDeque<String[]> reportRows,
      String account)
      throws IOException, InterruptedException {
    int SIZE = 4096;
    byte[] buffer = new byte[SIZE];

    FileWriter fw = new FileWriter("/Users/chguo/repos/forked/gobblin/downloaded/tmp" + account + ".csv");

    try (GZIPInputStream gzipInputStream = new GZIPInputStream(zippedStream)) {
      String partiallyConsumed = "";

      while (true) {
        int c = gzipInputStream.read(buffer, 0, SIZE);
        if (c == -1) {
          break;
        }
        if (c == 0) {
          continue;
        }
        //"c" can very likely be less than SIZE.
        String str = new String(buffer, 0, c, "UTF-8");
        fw.write(str);

        partiallyConsumed = addToQueue(reportRows, partiallyConsumed, str);
      }

      LOG.info("==END OF FILE==");
      fw.close();

      return gzipInputStream;
    }
  }

  static String addToQueue(LinkedBlockingDeque<String[]> reportRows, String previous, String current)
      throws InterruptedException {
    int start = -1;
    int i = -1;

    int len = current.length();
    while (++i < len) {
      if (current.charAt(i) != '\n') {
        continue;
      }
      String row;
      if (start < 0) {
        row = previous + current.substring(0, i);
      } else {
        row = current.substring(start, i);
      }

      List<String> columns = new ArrayList<>();
      for (String column : splitter.split(row)) {
        if (column.equals("--")) {
          columns.add(null);
        } else {
          columns.add(column);
        }
      }
      //TODO: create a converter for iterable, so that you don't need to convert to String[]
      reportRows.put(columns.toArray(new String[columns.size()]));
      start = i + 1;
    }

    if (start < 0) {
      return previous + current;
    }
    return current.substring(start);
  }

  public List<Pair<String, String>> getDates() {
    if (_dateRangeType.equals(ReportDefinitionDateRangeType.ALL_TIME)) {
      return Arrays.asList(Pair.of("", ""));
    } else {
      DateTimeFormatter dateFormatter = DateTimeFormat.forPattern("yyyyMMdd");
      if (_dailyPartition) {
        DateTime start = dateFormatter.parseDateTime(_startDate);
        DateTime end = dateFormatter.parseDateTime(_endDate);
        List<Pair<String, String>> ret = new ArrayList<>();
        while (start.compareTo(end) <= 0) {
          ret.add(Pair.of(dateFormatter.print(start), dateFormatter.print(start)));
          start = start.plusDays(1);
        }
        return ret;
      } else {
        return Arrays.asList(Pair.of(_startDate, _endDate));
      }
    }
  }
}
