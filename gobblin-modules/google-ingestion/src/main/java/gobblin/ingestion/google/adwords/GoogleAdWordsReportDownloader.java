package gobblin.ingestion.google.adwords;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.opencsv.CSVParser;

import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.extract.LongWatermark;


@Slf4j
public class GoogleAdWordsReportDownloader {
  private final static CSVParser splitter = new CSVParser(',', '"', '\\');
  private final static int SIZE = 4096; //Buffer size for unzipping stream.
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
  private final String _debugFileOutputPath;
  private final String _debugStringOutputPath;

  public GoogleAdWordsReportDownloader(AdWordsSession rootSession, WorkUnitState state,
      ReportDefinitionReportType reportType, ReportDefinitionDateRangeType dateRangeType, String schema) {
    _rootSession = rootSession;
    _reportType = reportType;
    _dateRangeType = dateRangeType;
    _columnNames = schemaToColumnNames(schema);
    log.info("Downloaded fields are: " + Arrays.toString(_columnNames.toArray()));

    long lowWatermark = state.getWorkunit().getLowWatermark(LongWatermark.class).getValue();
    _startDate = dateFormatter.print(watermarkFormatter.parseDateTime(Long.toString(lowWatermark)));

    _endDate = _startDate;
    _dailyPartition = state.getPropAsBoolean(GoogleAdWordsSource.KEY_CUSTOM_DATE_DAILY, false);
    _debugFileOutputPath = state.getProp(GoogleAdWordsSource.KEY_DEBUG_PATH_FILE, "");
    _debugStringOutputPath = state.getProp(GoogleAdWordsSource.KEY_DEBUG_PATH_STRING, "");

    _skipReportHeader = state.getPropAsBoolean(GoogleAdWordsSource.KEY_REPORTING_SKIP_REPORT_HEADER, true);
    _skipColumnHeader = state.getPropAsBoolean(GoogleAdWordsSource.KEY_REPORTING_SKIP_COLUMN_HEADER, true);
    _skipReportSummary = state.getPropAsBoolean(GoogleAdWordsSource.KEY_REPORTING_SKIP_REPORT_SUMMARY, true);
    _useRawEnumValues = state.getPropAsBoolean(GoogleAdWordsSource.KEY_REPORTING_USE_RAW_ENUM_VALUES, false);
    _includeZeroImpressions = state.getPropAsBoolean(GoogleAdWordsSource.KEY_REPORTING_INCLUDE_ZERO_IMPRESSION, false);
  }

  static List<String> schemaToColumnNames(String schemaString) {
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

            log.info("Start downloading " + jobName);

            ExponentialBackOff backOff = backOffBuilder.build();
            int numberOfAttempts = 0;
            while (true) {
              ++numberOfAttempts;
              try {
                downloadReport(account, _columnNames, _dateRangeType, range.getLeft(), range.getRight(), reportRows);
                log.info("Successfully downloaded " + jobName);
                return null;
              } catch (ReportException e) {
                long sleepMillis = backOff.nextBackOffMillis();
                log.info("Downloading %s failed #%d try: %s. Will sleep for %d milliseconds.", jobName,
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
        log.error(String.format("%s => %s", fail.getKey(), fail.getValue().getMessage()));
      }
    }

    log.info("End of downloading all reports.");
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
      throws ReportDownloadResponseException, ReportException, InterruptedException, IOException {
    Selector selector = new Selector();
    selector.getFields().addAll(fields);

    String reportName;
    if (dateRangeType.equals(ReportDefinitionDateRangeType.CUSTOM_DATE)) {
      DateRange value = new DateRange();
      value.setMin(startDate);
      value.setMax(endDate);
      selector.setDateRange(value);
      reportName = String.format("%s_%s/%s_%s.csv", startDate, endDate, _reportType.toString(), account);
    } else {
      reportName = String.format("all_time/%s_%s.csv", _reportType.toString(), account);
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
      log.error(e.getMessage());
      throw new RuntimeException(e);
    }

    ReportDownloader downloader = new ReportDownloader(session);
    ReportDownloadResponse response = downloader.downloadReport(reportDef);

    InputStream zippedStream = response.getInputStream();
    if (zippedStream == null) {
      String message = "Got empty stream for " + reportName;
      log.error(message);
      throw new RuntimeException(message);
    }

    if (!_debugFileOutputPath.trim().isEmpty()) {
      File debugFile = new File(_debugFileOutputPath, reportName);
      createPath(debugFile);
      writeToFile(zippedStream, debugFile);
    } else {
      FileWriter debugFileWriter = null;
      try {
        if (!_debugStringOutputPath.trim().isEmpty()) {
          File debugFile = new File(_debugStringOutputPath, reportName);
          createPath(debugFile);
          debugFileWriter = new FileWriter(debugFile);
        }
        processResponse(zippedStream, reportRows, debugFileWriter);
      } catch (IOException e) {
        log.error(
            String.format("Failed unzipping and processing records for %s. Reason is: %s", reportName, e.getMessage()));
        throw new RuntimeException(e);
      } finally {
        if (debugFileWriter != null) {
          debugFileWriter.close();
        }
      }
    }
  }

  private static void createPath(File file) {
    if (file.exists()) {
      boolean delete = file.delete();
      if (!delete) {
        throw new RuntimeException(String.format("Cannot delete debug file: %s", file));
      }
    }

    if (!file.exists()) {
      new File(file.getParent()).mkdirs();
    }
  }

  private static GZIPInputStream processResponse(InputStream zippedStream, LinkedBlockingDeque<String[]> reportRows,
      FileWriter debugFw)
      throws IOException, InterruptedException {
    byte[] buffer = new byte[SIZE];

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
        String str = new String(buffer, 0, c, "UTF-8"); //"c" can very likely be less than SIZE.
        if (debugFw != null) {
          debugFw.write(str);
        }
        partiallyConsumed = addToQueue(reportRows, partiallyConsumed, str);
      }
      return gzipInputStream;
    }
  }

  static String addToQueue(LinkedBlockingDeque<String[]> reportRows, String previous, String current)
      throws InterruptedException, IOException {
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

      String[] splits = splitter.parseLine(row);
      String[] transformed = new String[splits.length];
      for (int s = 0; s < splits.length; ++s) {
        String trimed = splits[s].trim();
        if (trimed.equals("--")) {
          transformed[s] = null;
        } else {
          transformed[s] = trimed;
        }
      }
      reportRows.put(transformed);
      start = i + 1;
    }

    if (start < 0) {
      return previous + current;
    }
    return current.substring(start);
  }

  private void writeToFile(InputStream zippedStream, File reportFile)
      throws IOException {
    byte[] buffer = new byte[SIZE];
    int c;
    try (GZIPInputStream gzipInputStream = new GZIPInputStream(zippedStream);
        OutputStream unzipped = new FileOutputStream(reportFile)) {
      while ((c = gzipInputStream.read(buffer, 0, SIZE)) >= 0) {
        unzipped.write(buffer, 0, c);
      }
    }
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
