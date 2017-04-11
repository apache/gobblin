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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;
import org.apache.commons.lang.mutable.MutableInt;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.api.services.analytics.Analytics;
import com.google.api.services.analytics.Analytics.Management;
import com.google.api.services.analytics.Analytics.Management.UnsampledReports;
import com.google.api.services.analytics.Analytics.Management.UnsampledReports.Get;
import com.google.api.services.analytics.model.UnsampledReport;
import com.google.api.services.analytics.model.UnsampledReport.DriveDownloadDetails;

import static gobblin.retry.RetryerFactory.*;
import static gobblin.source.extractor.extract.google.GoogleAnalyticsUnsampledExtractor.*;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.writer.exception.NonTransientException;

@Test(groups = { "gobblin.source.extractor.google" })
public class GoogleAnalyticsUnsampledExtractorTest {
  private WorkUnitState wuState;
  private Analytics gaService;
  private Get getReq;
  private static final String EXPECTED_FILE_ID = "testFileId";

  public void testPollForCompletion() throws IOException {
    wuState = new WorkUnitState();
    wuState.setProp(POLL_RETRY_PREFIX + RETRY_TIME_OUT_MS, TimeUnit.SECONDS.toMillis(30L));
    wuState.setProp(POLL_RETRY_PREFIX + RETRY_INTERVAL_MS, 1L);
    GoogleAnalyticsUnsampledExtractor extractor = setup(ReportCreationStatus.COMPLETED, wuState, false);

    UnsampledReport requestedReport = new UnsampledReport()
    .setAccountId("testAccountId")
    .setWebPropertyId("testWebPropertyId")
    .setProfileId("testProfileId")
    .setId("testId");

    String actualFileId = extractor.pollForCompletion(wuState, gaService, requestedReport).getDriveDownloadDetails().getDocumentId();
    Assert.assertEquals(actualFileId, EXPECTED_FILE_ID);
    verify(getReq, atLeast(5)).execute();
  }

  public void testPollForCompletionFailure() throws IOException {
    wuState = new WorkUnitState();
    wuState.setProp(POLL_RETRY_PREFIX + RETRY_TIME_OUT_MS, TimeUnit.SECONDS.toMillis(30L));
    wuState.setProp(POLL_RETRY_PREFIX + RETRY_INTERVAL_MS, 1L);
    GoogleAnalyticsUnsampledExtractor extractor = setup(ReportCreationStatus.FAILED, wuState, false);

    UnsampledReport requestedReport = new UnsampledReport()
    .setAccountId("testAccountId")
    .setWebPropertyId("testWebPropertyId")
    .setProfileId("testProfileId")
    .setId("testId");

    try {
      extractor.pollForCompletion(wuState, gaService, requestedReport);
      Assert.fail("Should have failed with failed status");
    } catch (Exception e) {
      Assert.assertTrue(e.getCause().getCause() instanceof NonTransientException);
    }
    verify(getReq, atLeast(5)).execute();
  }

  public void testPollForCompletionWithException() throws IOException {
    wuState = new WorkUnitState();
    wuState.setProp(POLL_RETRY_PREFIX + RETRY_TIME_OUT_MS, TimeUnit.SECONDS.toMillis(30L));
    wuState.setProp(POLL_RETRY_PREFIX + RETRY_INTERVAL_MS, 1L);
    GoogleAnalyticsUnsampledExtractor extractor = setup(ReportCreationStatus.COMPLETED, wuState, true);

    UnsampledReport requestedReport = new UnsampledReport()
    .setAccountId("testAccountId")
    .setWebPropertyId("testWebPropertyId")
    .setProfileId("testProfileId")
    .setId("testId");

    String actualFileId = extractor.pollForCompletion(wuState, gaService, requestedReport).getDriveDownloadDetails().getDocumentId();
    Assert.assertEquals(actualFileId, EXPECTED_FILE_ID);
    verify(getReq, atLeast(5)).execute();
  }

  private GoogleAnalyticsUnsampledExtractor setup(final ReportCreationStatus status, WorkUnitState wuState, final boolean throwException) throws IOException {
    Extractor actualExtractor = mock(Extractor.class);
    gaService = mock(Analytics.class);
    Management mgmt = mock(Management.class);
    when(gaService.management()).thenReturn(mgmt);
    UnsampledReports req = mock(UnsampledReports.class);
    when(mgmt.unsampledReports()).thenReturn(req);
    getReq = mock(Get.class);
    when(req.get(anyString(), anyString(), anyString(), anyString())).thenReturn(getReq);


    int pollCount = 10;
    final MutableInt countDown = new MutableInt(pollCount);
    when(getReq.execute()).then(new Answer<UnsampledReport>() {
      @Override
      public UnsampledReport answer(InvocationOnMock invocation) throws Throwable {
        countDown.decrement();
        if (countDown.intValue() == 0) {
          UnsampledReport response = new UnsampledReport();
          DriveDownloadDetails details = new DriveDownloadDetails();
          details.setDocumentId(EXPECTED_FILE_ID);

          response.setStatus(status.name())
                  .setDownloadType(DOWNLOAD_TYPE_GOOGLE_DRIVE)
                  .setDriveDownloadDetails(details);
          return response;
        } else if (throwException) {
          throw new RuntimeException("Dummy exception.");
        }
        return new UnsampledReport();
      }
    });

    return new GoogleAnalyticsUnsampledExtractor<>(wuState, actualExtractor, gaService);
  }
}
