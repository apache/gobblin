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

package org.apache.gobblin.multistage.source;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import org.apache.commons.io.IOUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.message.BasicStatusLine;

import static org.mockito.Mockito.*;


/*
 * Helper class to create mock test objects
 */
public class GobblinMultiStageTestHelpers {

  public GobblinMultiStageTestHelpers() {}

  public static WorkUnitState prepareMockWorkUnitState() {

    WorkUnit unit = mock(WorkUnit.class);

    /*
     * mock watermark with default values
     */
    LongWatermark low = mock(LongWatermark.class);
    when(low.getValue()).thenReturn(-1L);
    LongWatermark high = mock(LongWatermark.class);
    when(high.getValue()).thenReturn(1L);


    when(unit.getLowWatermark(any())).thenReturn(low);
    when(unit.getExpectedHighWatermark(any())).thenReturn(high);

    WorkUnitState state = mock(WorkUnitState.class);
    /*
     * mocking properties in state that can be used in the extractor.
     * If a property is used and not mocked, NPE will be thrown.
     */
    when(state.getWorkunit()).thenReturn(unit);
    when(state.getProp("source.conn.username")).thenReturn("dummy_username");
    when(state.getProp("source.conn.password")).thenReturn("dummy_password");
    when(state.getProp("PLAIN_PASSWORD", "")).thenReturn("");
    return state;
  }

  public static SourceState prepareSourceStateWithoutWaterMark() {
    SourceState state = mock(SourceState.class);
    when(state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY)).thenReturn("dumyExtractNamespace");
    when(state.getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY)).thenReturn("dumyExtractTableNameKey");
    when(state.getProp(ConfigurationKeys.DATASET_URN_KEY)).thenReturn("dumyDataSetUrnKey");
    when(state.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY, "SNAPSHOT_ONLY")).thenReturn("SNAPSHOT_ONLY");
    when(state.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY, "")).thenReturn("SNAPSHOT_ONLY");
    when(state.getProp(ConfigurationKeys.DATASET_URN_KEY)).thenReturn("dumyDataSetUrnKey");
    return state;
  }

  public static SourceState prepareSourceStateWithWaterMark() {
    SourceState state = prepareSourceStateWithoutWaterMark();
    when(state.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY, "SNAPSHOT_ONLY")).thenReturn("SNAPSHOT_ONLY");
    when(state.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY, "")).thenReturn("SNAPSHOT_ONLY");
    when(state.getProp(ConfigurationKeys.DATASET_URN_KEY)).thenReturn("dumyDataSetUrnKey");
    return state;
  }

  /*
   * Preparing mocked response for a valid use case.
   */
  public static HttpResponse getBaseMockHttpResponse() {
    HttpResponse resp = mock(CloseableHttpResponse.class);
    BasicStatusLine line = new BasicStatusLine(new ProtocolVersion("http", 1, 1),
        200, "success");
    when(resp.getStatusLine()).thenReturn(line);
    BasicHttpEntity entity = new BasicHttpEntity();
    entity.setContent(IOUtils.toInputStream(MockedResponseStrings.mockedStringResponse));
    when(resp.getEntity()).thenReturn(entity);
    return resp;
  }

  /*
   * Preparing mocked response for a invalid use case.
   */
  public static HttpResponse getMockHttpResponseInvalidStatus() {
    HttpResponse resp = getBaseMockHttpResponse();
    BasicStatusLine line = new BasicStatusLine(new ProtocolVersion("http", 1, 1),
        400, "success");
    when(resp.getStatusLine()).thenReturn(line);
    return resp;
  }

  /*
   * Preparing mocked response for a valid use case with multiple records.
   */
  public static HttpResponse getBaseMockHttpResponseMultipleRecords() {
    HttpResponse resp = mock(CloseableHttpResponse.class);
    BasicStatusLine line = new BasicStatusLine(new ProtocolVersion("http", 1, 1),
        200, "success");
    when(resp.getStatusLine()).thenReturn(line);
    BasicHttpEntity entity = new BasicHttpEntity();
    entity.setContent(IOUtils.toInputStream(MockedResponseStrings.mockedStringResponseMultipleRecords));
    when(resp.getEntity()).thenReturn(entity);
    return resp;
  }

  public static String getDateFromTimeStamp(long timestamp) {
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    df.setTimeZone(TimeZone.getTimeZone("GMT"));
    return df.format(new Date(timestamp));
  }
}
