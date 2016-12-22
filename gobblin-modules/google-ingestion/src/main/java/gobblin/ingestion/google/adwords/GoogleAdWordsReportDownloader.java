package gobblin.ingestion.google.adwords;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.ads.adwords.axis.factory.AdWordsServices;
import com.google.api.ads.adwords.axis.v201609.cm.ReportDefinitionField;
import com.google.api.ads.adwords.axis.v201609.cm.ReportDefinitionServiceInterface;
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


public class GoogleAdWordsReportDownloader {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleAdWordsReportDownloader.class);
  private final boolean _skipReportHeader;
  private final boolean _skipColumnHeader;
  private final boolean _skipReportSummary;
  private final boolean _includeZeroImpressions;
  private final boolean _useRawEnumValues;

  private final AdWordsSession _rootSession;
  private final ReportDefinitionReportType _reportType;
  private final ReportDefinitionDateRangeType _dateRangeType;
  private final List<String> _requestedColumns;
  private final ExponentialBackOff.Builder backOffBuilder =
      new ExponentialBackOff.Builder().setMaxElapsedTimeMillis(1000 * 60 * 5);
  private final String _destinationFolder;
  private final String _startDate;
  private final String _endDate;
  private final boolean _dailyPartition;

  public GoogleAdWordsReportDownloader(AdWordsSession rootSession, Properties properties) {
    _rootSession = rootSession;
    String reportName = properties.getProperty(GoogleAdWordsSource.KEY_REPORT).toUpperCase() + "_REPORT";
    _reportType = ReportDefinitionReportType.valueOf(reportName);
    _dateRangeType = ReportDefinitionDateRangeType
        .valueOf(properties.getProperty(GoogleAdWordsSource.KEY_DATE_RANGE).toUpperCase());
    if (!_dateRangeType.equals(ReportDefinitionDateRangeType.CUSTOM_DATE) && !_dateRangeType
        .equals(ReportDefinitionDateRangeType.ALL_TIME)) {
      throw new UnsupportedOperationException("Only support date range of custom_date or all_time");
    }
    //TODO: Chen - Update
    String columnNames = properties.getProperty(GoogleAdWordsSource.KEY_COLUMN_NAMES);
    if (columnNames == null) {
      _requestedColumns = null;
    } else {
      String[] columns = columnNames.split(",");
      _requestedColumns = Arrays.asList(columns);
    }
    _destinationFolder = properties.getProperty(GoogleAdWordsSource.KEY_DESTINATION_PATH);
    _startDate = properties.getProperty(GoogleAdWordsSource.KEY_CUSTOM_DATE_START);
    _endDate = properties.getProperty(GoogleAdWordsSource.KEY_CUSTOM_DATE_END);
    _dailyPartition = getConfig(properties, GoogleAdWordsSource.KEY_CUSTOM_DATE_DAILY, false);

    _skipReportHeader = getConfig(properties, GoogleAdWordsSource.KEY_REPORTING_SKIP_REPORT_HEADER, true);
    _skipColumnHeader = getConfig(properties, GoogleAdWordsSource.KEY_REPORTING_SKIP_COLUMN_HEADER, true);
    _skipReportSummary = getConfig(properties, GoogleAdWordsSource.KEY_REPORTING_SKIP_REPORT_SUMMARY, true);
    _useRawEnumValues = getConfig(properties, GoogleAdWordsSource.KEY_REPORTING_USE_RAW_ENUM_VALUES, false);
    _includeZeroImpressions =
        getConfig(properties, GoogleAdWordsSource.KEY_REPORTING_INCLUDE_ZERO_IMPRESSION, false);
  }

  private boolean getConfig(Properties configuration, String key, boolean defaultValue) {
    String property = configuration.getProperty(key);
    if (property == null || property.trim().isEmpty()) {
      return defaultValue;
    }
    return Boolean.valueOf(property);
  }

  public void downloadAllReports(Collection<String> accounts)
      throws InterruptedException {
    ExecutorService threadPool = Executors.newFixedThreadPool(Math.min(8, accounts.size()));
    final TreeSet<String> fields = getDownloadFields(_rootSession, _reportType, _requestedColumns);
    LOG.info("Downloaded fields are: " + Arrays.toString(fields.toArray()));

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
                downloadReport(account, fields, _dateRangeType, range.getLeft(), range.getRight());
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
   * @throws ReportDownloadResponseException This is not retryable
   * @throws ReportException ReportException represents a potentially retryable error
   */
  private void downloadReport(String account, TreeSet<String> fields, ReportDefinitionDateRangeType dateRangeType,
      String startDate, String endDate)
      throws ReportDownloadResponseException, ReportException {
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

    if (_destinationFolder != null) {
      String datePath;
      if (dateRangeType.equals(ReportDefinitionDateRangeType.CUSTOM_DATE)) {
        datePath = String.format("%s/%s_%s", _destinationFolder, startDate, endDate);
      } else {
        datePath = _destinationFolder + "/all_time";
      }
      File directory = new File(datePath);
      if (!directory.exists()) {
        try {
          boolean mkdir = directory.mkdir();
          if (!mkdir) {
            LOG.info("Failed creating path " + directory);
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      String reportFile = String.format("%s/%s_%s.csv", datePath, _reportType, account);
      try {
        writeToFile(zippedStream, reportFile);
      } catch (IOException e) {
        LOG.error("Failed while unzipping and writing to file for ", reportName);
        throw new RuntimeException(e);
      }
    } else {
      try {
        printToConsole(zippedStream);
      } catch (IOException e) {
        LOG.error("Failed while unzipping and printing to console for ", reportName);
        throw new RuntimeException(e);
      }
    }
  }

  private GZIPInputStream printToConsole(InputStream zippedStream)
      throws IOException {
    int SIZE = 4096;
    byte[] buffer = new byte[SIZE];

    try (GZIPInputStream gzipInputStream = new GZIPInputStream(zippedStream)) {
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
        System.out.print(str);
      }
      System.out.println("==END OF FILE==");
      return gzipInputStream;
    }
  }

  private void writeToFile(InputStream zippedStream, String reportFile)
      throws IOException {
    int SIZE = 4096;
    byte[] buffer = new byte[SIZE];
    int c;
    try (GZIPInputStream gzipInputStream = new GZIPInputStream(zippedStream);
        OutputStream unzipped = new FileOutputStream(reportFile)) {
      while ((c = gzipInputStream.read(buffer, 0, SIZE)) >= 0) {
        unzipped.write(buffer, 0, c);
      }
    }
  }

  private static TreeSet<String> getDownloadFields(AdWordsSession rootSession, ReportDefinitionReportType reportType,
      List<String> requestedColumns) {
    try {
      HashMap<String, String> allFields = getReportFields(rootSession, reportType);
      if (requestedColumns == null || requestedColumns.isEmpty()) {
        return new TreeSet<>(allFields.keySet());
      }

      TreeSet<String> ret = new TreeSet<>();
      for (String column : requestedColumns) {
        if (allFields.containsKey(column)) {
          ret.add(column);
        } else {
          throw new RuntimeException(String.format("Column %s doesn't exist in report %s", column, reportType));
        }
      }
      return ret;
    } catch (RemoteException e) {
      LOG.error(e.getMessage());
      throw new RuntimeException(e);
    }
  }

  private static HashMap<String, String> getReportFields(AdWordsSession rootSession,
      ReportDefinitionReportType reportType)
      throws RemoteException {
    AdWordsServices adWordsServices = new AdWordsServices();

    ReportDefinitionServiceInterface reportDefinitionService =
        adWordsServices.get(rootSession, ReportDefinitionServiceInterface.class);

    ReportDefinitionField[] reportDefinitionFields = reportDefinitionService.getReportFields(
        com.google.api.ads.adwords.axis.v201609.cm.ReportDefinitionReportType.fromString(reportType.toString()));
    HashMap<String, String> fields = new HashMap<>();
    for (ReportDefinitionField field : reportDefinitionFields) {
      fields.put(field.getFieldName(), field.getFieldType());
    }
    return fields;
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
