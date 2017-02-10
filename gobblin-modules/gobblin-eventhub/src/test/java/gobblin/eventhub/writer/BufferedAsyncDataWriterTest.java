/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package gobblin.eventhub.writer;

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

import gobblin.writer.BufferedAsyncDataWriter;
import gobblin.writer.RecordMetadata;
import gobblin.writer.WriteCallback;
import gobblin.writer.WriteResponse;

import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.*;


public class BufferedAsyncDataWriterTest {

  private CloseableHttpClient mockHttpClient;

  public BufferedAsyncDataWriterTest() throws IOException{
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
  public void testBufferedRecords()
      throws IOException, InterruptedException {

    // mock buffered async data writer
    Properties props = new Properties();
    EventhubDataWriter eventhubDataWriter = Mockito.spy(new EventhubDataWriter(props, mockHttpClient));
    EventhubBatchAccumulator accumulator = new EventhubBatchAccumulator();
    BufferedAsyncDataWriter dataWriter = new BufferedAsyncDataWriter(accumulator, eventhubDataWriter);
    Mockito.doNothing().when(eventhubDataWriter).refreshSignature();

    // mock record and callback
    WriteCallback callback = mock(WriteCallback.class);
    List<Future<RecordMetadata>> futures = new LinkedList<>();

    int totalTimes = 500;
    try {
      for (int i=0; i<totalTimes; ++i) {
        byte[] record = new byte[8];
        futures.add(dataWriter.write(record, callback));
      }

      dataWriter.flush();
    }
    finally
    {
      dataWriter.close();
    }

    // verify all the callbacks are invoked
    verify(callback, times(totalTimes)).onSuccess(isA(WriteResponse.class));
    verify(callback, never()).onFailure(isA(Exception.class));

    // verify all the futures are completed
    for (Future future: futures) {
      Assert.assertTrue(future.isDone(), "Future should be done");
    }
  }
}
