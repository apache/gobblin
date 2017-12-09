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

package org.apache.gobblin.eventhub.writer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


import org.apache.gobblin.writer.Batch;
import org.apache.gobblin.writer.WriteCallback;
import org.apache.gobblin.writer.WriteResponse;

import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.*;


public class EventhubDataWriterTest {

  private CloseableHttpClient mockHttpClient;

  public EventhubDataWriterTest() throws IOException{
    // mock httpclient
    CloseableHttpResponse mockHttpResponse = mock(CloseableHttpResponse.class);
    mockHttpClient = mock(CloseableHttpClient.class);
    StatusLine status = mock (StatusLine.class);
    Mockito.when(mockHttpClient.execute(Mockito.any(HttpPost.class))).thenReturn(mockHttpResponse);
    Mockito.when(status.getStatusCode()).thenReturn(201);
    Mockito.when(mockHttpResponse.getEntity()).thenReturn(null);
    Mockito.when(mockHttpResponse.getStatusLine()).thenReturn(status);
  }

  @Test
  public void testSingleBatch() {
    // mock eventhub data writer
    Properties props = new Properties();
    EventhubDataWriter eventhubDataWriter = Mockito.spy(new EventhubDataWriter(props, mockHttpClient));
    Mockito.doNothing().when(eventhubDataWriter).refreshSignature();

    List<String> records = new LinkedList<>();
    for (int i=0; i<50; ++i)
      records.add(new String("abcdefgh"));

    Batch<String> batch = mock(Batch.class);
    WriteCallback callback = mock(WriteCallback.class);
    Mockito.when(batch.getRecords()).thenReturn(records);

    Future<WriteResponse> future = eventhubDataWriter.write(batch,callback);
    verify(callback, times(1)).onSuccess(isA(WriteResponse.class));
    verify(callback, never()).onFailure(isA(Exception.class));
    Assert.assertTrue(future.isDone(), "Future should be done");
  }

  @Test
  public void testSingleRecord() throws IOException {

    // mock eventhub data writer
    Properties props = new Properties();
    EventhubDataWriter eventhubDataWriter = Mockito.spy(new EventhubDataWriter(props, mockHttpClient));
    Mockito.doNothing().when(eventhubDataWriter).refreshSignature();

    String record = "abcdefgh";
    WriteResponse<Integer> writeResponse = eventhubDataWriter.write(record);
    int returnCode = writeResponse.getRawResponse();
    Assert.assertEquals(returnCode, 201);
  }
}
