/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gobblin.source.extractor.extract.google;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.services.analytics.Analytics;
import com.google.api.services.analytics.model.UnsampledReport;
import com.google.api.services.analytics.Analytics.Management.UnsampledReports.Insert;
import com.google.api.services.drive.Drive;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import static gobblin.retry.RetryerFactory.*;
import static gobblin.configuration.ConfigurationKeys.*;
import static gobblin.source.extractor.extract.google.GoogleCommonKeys.*;
import static gobblin.source.extractor.extract.google.GoogleAnalyticsUnsampledSource.*;
import gobblin.config.ConfigBuilder;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.GobblinMetrics;
import gobblin.retry.RetryerFactory;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.LongWatermark;
import gobblin.source.extractor.filebased.CsvFileDownloader;
import gobblin.source.workunit.WorkUnit;
import gobblin.writer.exception.NonTransientException;

/**
 * Extracts Google Analytics(GA) unsampled report data.
 * GA provides unsampled report by client requesting it via GA asynchronous api and GA (server) creates unsampled report
 * on their background and put into Google drive by default.
 * (GoogleAnalyticsUnsampledExtractor currently does not support use case on Google cloud storage)
 *
 * While being created in background, GoogleAnalyticsExtractor will poll for status of the report request. Once report is generated,
 * GoogleAnalyticsUnsampledExtractor will use GoogleDriveExtractor to extract records.
 *
 * @param <S>
 * @param <D>
 */
public class GoogleAnalyticsUnsampledExtractor<S, D> implements Extractor<S, D> {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleAnalyticsUnsampledExtractor.class);
  static final String GA_UNSAMPLED_REPORT_PREFIX = GA_REPORT_PREFIX + "unsampled.";
  static final String GA_UNSAMPLED_REPORT_CREATION_TIMER = GA_UNSAMPLED_REPORT_PREFIX + "creation.timer";

  static final String REQUEST_RETRY_PREFIX = GA_REPORT_PREFIX + "request_retry.";

  static final String POLL_RETRY_PREFIX = GA_REPORT_PREFIX + "poll.";
  static final Config POLL_RETRY_DEFAULTS;
  static {
    Map<String, Object> configMap =
        ImmutableMap.<String, Object>builder()
        .put(RETRY_TIME_OUT_MS, TimeUnit.HOURS.toMillis(1L))   //Overall try to poll for 1 hour
        .put(RETRY_INTERVAL_MS, TimeUnit.MINUTES.toMillis(1L)) //Try to poll every 1 minutes
        .put(RETRY_TYPE, RetryType.FIXED.name())
        .build();
    POLL_RETRY_DEFAULTS = ConfigFactory.parseMap(configMap);
  };

  static final String WATERMARK_INPUTFORMAT = "yyyyMMddHHmmss";
  static final String DELETE_TEMP_UNSAMPLED_REPORT = GA_UNSAMPLED_REPORT_PREFIX + "delete_temp_unsampled_report";

  static enum ReportCreationStatus {
    FAILED,
    PENDING,
    COMPLETED
  }
  static final String DOWNLOAD_TYPE_GOOGLE_DRIVE = "GOOGLE_DRIVE";

  private final Closer closer = Closer.create();
  private final Analytics gaService;
  private final WorkUnitState wuState;
  private final Extractor<S, D> actualExtractor;
  private final DateTimeFormatter googleAnalyticsFormatter;
  private final DateTimeFormatter watermarkFormatter;
  private final long nextWatermark;

  /**
   * For unsampled report, it will call GA service to produce unsampled CSV report into GoogleDrive so that getExtractor will
   * use Google drive to extract record from CSV file.
   *
   * @param wuState
   * @param sampleRate
   * @throws IOException
   */
  public GoogleAnalyticsUnsampledExtractor(WorkUnitState wuState) throws IOException {
    this.wuState = wuState;
    this.googleAnalyticsFormatter = DateTimeFormat.forPattern(DATE_FORMAT)
        .withZone(DateTimeZone.forID(wuState.getProp(SOURCE_TIMEZONE, DEFAULT_SOURCE_TIMEZONE)));
    this.watermarkFormatter = DateTimeFormat.forPattern(WATERMARK_INPUTFORMAT)
        .withZone(DateTimeZone.forID(wuState.getProp(SOURCE_TIMEZONE, DEFAULT_SOURCE_TIMEZONE)));

    Credential credential = new GoogleCommon.CredentialBuilder(wuState.getProp(SOURCE_CONN_PRIVATE_KEY), wuState.getPropAsList(API_SCOPES))
                                            .fileSystemUri(wuState.getProp(PRIVATE_KEY_FILESYSTEM_URI))
                                            .proxyUrl(wuState.getProp(SOURCE_CONN_USE_PROXY_URL))
                                            .port(wuState.getProp(SOURCE_CONN_USE_PROXY_PORT))
                                            .serviceAccountId(wuState.getProp(SOURCE_CONN_USERNAME))
                                            .build();

    this.gaService = new Analytics.Builder(credential.getTransport(),
                                           GoogleCommon.getJsonFactory(),
                                           credential)
                                  .setApplicationName(Preconditions.checkNotNull(wuState.getProp(APPLICATION_NAME)))
                                  .build();

    Drive driveClient = new Drive.Builder(credential.getTransport(),
                                          GoogleCommon.getJsonFactory(),
                                          Preconditions.checkNotNull(credential, "Credential is required"))
                                 .setApplicationName(Preconditions.checkNotNull(wuState.getProp(APPLICATION_NAME), "ApplicationName is required"))
                                 .build();

    GoogleDriveFsHelper fsHelper = closer.register(new GoogleDriveFsHelper(wuState, driveClient));

    UnsampledReport request = new UnsampledReport()
                                     .setAccountId(Preconditions.checkNotNull(wuState.getProp(ACCOUNT_ID), ACCOUNT_ID + " is required"))
                                     .setWebPropertyId(Preconditions.checkNotNull(wuState.getProp(WEB_PROPERTY_ID), WEB_PROPERTY_ID + " is required"))
                                     .setProfileId(Preconditions.checkNotNull(wuState.getProp(VIEW_ID), VIEW_ID + " is required"))
                                     .setTitle(Preconditions.checkNotNull(wuState.getProp(SOURCE_ENTITY), SOURCE_ENTITY + " is required."))
                                     .setStartDate(convertFormat(wuState.getWorkunit().getLowWatermark(LongWatermark.class).getValue()))
                                     .setEndDate(convertFormat(wuState.getWorkunit().getExpectedHighWatermark(LongWatermark.class).getValue()))
                                     .setMetrics(Preconditions.checkNotNull(wuState.getProp(METRICS), METRICS + " is required."))
                                     .setDimensions(wuState.getProp(DIMENSIONS)) //Optional
                                     .setSegment(wuState.getProp(SEGMENTS)) //Optional
                                     .setFilters(wuState.getProp(FILTERS)); //Optional

    UnsampledReport createdReport = prepareUnsampledReport(request, fsHelper, wuState.getPropAsBoolean(DELETE_TEMP_UNSAMPLED_REPORT, true));

    DateTime nextWatermarkDateTime = googleAnalyticsFormatter.parseDateTime(createdReport.getEndDate()).plusDays(1);
    nextWatermark = Long.parseLong(watermarkFormatter.print(nextWatermarkDateTime));

    this.actualExtractor = closer.register(new GoogleDriveExtractor<S, D>(copyOf(wuState), fsHelper));
  }

  @VisibleForTesting
  GoogleAnalyticsUnsampledExtractor(WorkUnitState state, Extractor<S, D> actualExtractor, Analytics gaService) throws IOException {
    this.wuState = state;
    this.googleAnalyticsFormatter = DateTimeFormat.forPattern(DATE_FORMAT)
        .withZone(DateTimeZone.forID(state.getProp(SOURCE_TIMEZONE, DEFAULT_SOURCE_TIMEZONE)));
    this.watermarkFormatter = DateTimeFormat.forPattern(WATERMARK_INPUTFORMAT)
        .withZone(DateTimeZone.forID(state.getProp(SOURCE_TIMEZONE, DEFAULT_SOURCE_TIMEZONE)));
    this.actualExtractor = actualExtractor;
    this.gaService = gaService;
    this.nextWatermark = -1;
  }

  /**
   * Copy WorkUnitState so that work unit also contains job state. FileBasedExtractor needs properties from job state (mostly source.* properties),
   * where it has been already removed when reached here.
   *
   * @param src
   * @return
   */
  private WorkUnitState copyOf(WorkUnitState src) {
    WorkUnit copiedWorkUnit = WorkUnit.copyOf(src.getWorkunit());
    copiedWorkUnit.addAllIfNotExist(src.getJobState());

    WorkUnitState workUnitState = new WorkUnitState(copiedWorkUnit, src.getJobState());
    workUnitState.addAll(src);
    return workUnitState;
  }

  /**
   * Create unsampled report in Google drive and add google drive file id into state so that Google drive extractor
   * can extract record from it. Also, update the state to use CsvFileDownloader unless other downloader is defined.
   *
   * It also register closer to delete the file from Google Drive unless explicitly requested to not deleting it.
   * @return documentID of unsampled report in Google drive
   * @throws IOException
   *
   */
  @VisibleForTesting
  UnsampledReport prepareUnsampledReport(UnsampledReport request, final GoogleDriveFsHelper fsHelper, boolean isDeleteTempReport) throws IOException {
    UnsampledReport createdReport = createUnsampledReports(request);

    final String fileId = createdReport.getDriveDownloadDetails().getDocumentId();
    LOG.info("Temporary unsampled report created in Google Drive: " + fileId);

    if (isDeleteTempReport) {
      closer.register(new Closeable() {
        @Override public void close() throws IOException {
          LOG.info("Deleting created temporary unsampled report from Google drive " + fileId);
          fsHelper.deleteFile(fileId);
        }
      });
    } else {
      LOG.warn("Temporary unsampled report will not be deleted as requested. File ID: " + fileId);
    }


    wuState.setProp(SOURCE_FILEBASED_FILES_TO_PULL, fileId);
    if (!wuState.contains(SOURCE_FILEBASED_OPTIONAL_DOWNLOADER_CLASS)) {
      wuState.setProp(SOURCE_FILEBASED_OPTIONAL_DOWNLOADER_CLASS, CsvFileDownloader.class.getName());
    }

    return createdReport;
  }

  @VisibleForTesting
  UnsampledReport createUnsampledReports(UnsampledReport request) throws IOException {
    long startTimeInMillis = System.currentTimeMillis();
    try {
      UnsampledReport requestedReport = requestUnsampledReport(request);
      UnsampledReport createdReport = pollForCompletion(wuState, gaService, requestedReport);

      createdReport.setEndDate(requestedReport.getEndDate());
      return createdReport;
    } finally {
      long delta = System.currentTimeMillis() - startTimeInMillis;
      if (GobblinMetrics.isEnabled(wuState)) {
        Timer timer = Instrumented.getMetricContext(wuState, getClass()).timer(GA_UNSAMPLED_REPORT_CREATION_TIMER);
        Instrumented.updateTimer(Optional.of(timer), delta, TimeUnit.MILLISECONDS);
      }
    }
  }

  @VisibleForTesting
  UnsampledReport requestUnsampledReport(UnsampledReport request) throws IOException {
    String accountId = request.getAccountId();
    String webPropertyId = request.getWebPropertyId();
    String profileId = request.getProfileId();
    request.setAccountId(null).setWebPropertyId(null).setProfileId(null); //GA somehow does not allow these values in it.

    final String endDate = request.getEndDate();
    final Insert insertRequest = gaService.management()
                                          .unsampledReports()
                                          .insert(accountId, webPropertyId, profileId, request);

    Config config = ConfigBuilder.create().loadProps(wuState.getProperties(), REQUEST_RETRY_PREFIX).build();
    Retryer<UnsampledReport> retryer = RetryerFactory.newInstance(config);

    LOG.info("Requesting to create unsampled report " + request);
    try {
      return retryer.call(new Callable<UnsampledReport>() {
        @Override
        public UnsampledReport call() throws Exception {
          UnsampledReport response = insertRequest.execute();
          if (ReportCreationStatus.FAILED.name().equals(response.getStatus())) { //No retry if it's explicitly failed from server
            throw new NonTransientException("Failed to create unsampled report " + response);
          }
          response.setEndDate(endDate); //response does not have end date where we need it later for next watermark calculation.
          return response;
        }
      });
    } catch (ExecutionException e) {
      throw new IOException(e);
    } catch (RetryException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Converts date format from watermark format to Google analytics format
   * @param watermark
   * @return
   */
  private String convertFormat(long watermark) {
    Preconditions.checkArgument(watermark > 0, "Watermark should be positive number.");
    return googleAnalyticsFormatter.print(watermarkFormatter.parseDateTime(Long.toString(watermark)));
  }

  @VisibleForTesting
  UnsampledReport pollForCompletion(State state, final Analytics gaService, final UnsampledReport requestedReport) throws IOException {

    Config config = ConfigBuilder.create()
                                 .loadProps(state.getProperties(), POLL_RETRY_PREFIX)
                                 .build()
                                 .withFallback(POLL_RETRY_DEFAULTS);

    Retryer<UnsampledReport> retryer = RetryerFactory.newInstance(config);
    LOG.info("Will poll for completion on unsampled report with retry config: " + config);

    final Stopwatch stopwatch = Stopwatch.createStarted();
    UnsampledReport result = null;
    try {
      result = retryer.call(new Callable<UnsampledReport>() {

        @Override
        public UnsampledReport call() throws Exception {
          UnsampledReport response = null;
          try {
            response = gaService.management()
                .unsampledReports()
                .get(requestedReport.getAccountId(),
                     requestedReport.getWebPropertyId(),
                     requestedReport.getProfileId(),
                     requestedReport.getId())
                .execute();
          } catch (Exception e) {
            LOG.warn("Encountered exception while polling for unsampled report. Will keep polling. " +
                     "Elasped so far: " + stopwatch.elapsed(TimeUnit.SECONDS) + " seconds", e);
            throw e;
          }

          ReportCreationStatus status = ReportCreationStatus.valueOf(response.getStatus());
          switch(status) {
            case FAILED:
              //Stop retrying if it explicitly failed from server.
              throw new NonTransientException("Unsampled report has failed to be generated. " + response);
            case PENDING:
              LOG.info("Waiting for report completion. Elasped so far: " + stopwatch.elapsed(TimeUnit.SECONDS)
                  + " seconds for unsampled report: " + response);
              //Throw so that Retryer will retry
              throw new RuntimeException("Not completed yet. This will be retried. " + response);
            case COMPLETED:
              return response;
            default:
              throw new NonTransientException(status + " is not supported. " + response);
          }
        }
      });
    } catch (ExecutionException e) {
      throw new IOException(e);
    } catch (RetryException e) {
      throw new RuntimeException(e);
    }
    LOG.info("Unsampled report creation has been completed. " + result);
    Preconditions.checkArgument(DOWNLOAD_TYPE_GOOGLE_DRIVE.equals(result.getDownloadType()),
                                result.getDownloadType() + " DownloadType is not supported.");

    return result;
  }

  @Override
  public void close() throws IOException {
    LOG.info("Updating the current state high water mark with " + nextWatermark);
    this.wuState.setActualHighWatermark(new LongWatermark(nextWatermark));
    closer.close();
  }

  @Override
  public S getSchema() throws IOException {
    return actualExtractor.getSchema();
  }

  @Override
  public D readRecord(D reuse) throws DataRecordException, IOException {
    return actualExtractor.readRecord(reuse);
  }

  @Override
  public long getExpectedRecordCount() {
    return actualExtractor.getExpectedRecordCount();
  }

  @Override
  public long getHighWatermark() {
    return actualExtractor.getHighWatermark();
  }
}
