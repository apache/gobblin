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
package org.apache.gobblin.writer.http;

import static org.mockito.Mockito.*;
import static org.apache.gobblin.writer.http.SalesForceRestWriterBuilder.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.converter.http.RestEntry;
import org.apache.gobblin.writer.http.SalesforceRestWriter.Operation;

import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

@Test(groups = { "gobblin.writer" })
public class SalesforceRestWriterTest {

  private SalesforceRestWriter writer;
  private CloseableHttpClient client;

  private void setup(Operation operation) throws ClientProtocolException, IOException, URISyntaxException {
    setup(operation, new State());
  }

  private void setup(Operation operation, State state) throws ClientProtocolException, IOException, URISyntaxException {
    state.appendToSetProp(CONF_PREFIX + STATIC_SVC_ENDPOINT, "test");
    state.appendToSetProp(CONF_PREFIX + CLIENT_ID, "test");
    state.appendToSetProp(CONF_PREFIX + CLIENT_SECRET, "test");
    state.appendToSetProp(CONF_PREFIX + USER_ID, "test");
    state.appendToSetProp(CONF_PREFIX + PASSWORD, "test");
    state.appendToSetProp(CONF_PREFIX + USE_STRONG_ENCRYPTION, "test");
    state.appendToSetProp(CONF_PREFIX + SECURITY_TOKEN, "test");
    state.appendToSetProp(CONF_PREFIX + OPERATION, operation.name());

    SalesForceRestWriterBuilder builder = new SalesForceRestWriterBuilder();
    builder = spy(builder);
    HttpClientBuilder httpClientBuilder = mock(HttpClientBuilder.class);
    doReturn(httpClientBuilder).when(builder).getHttpClientBuilder();

    client = mock(CloseableHttpClient.class);
    when(httpClientBuilder.build()).thenReturn(client);

    builder.fromState(state);
    writer = new SalesforceRestWriter(builder, "test");
    writer.setCurServerHost(new URI("http://test.nowhere.com"));
  }

  public void testInsertSuccess() throws IOException, URISyntaxException {
    setup(SalesforceRestWriter.Operation.INSERT_ONLY_NOT_EXIST);

    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);
    when(client.execute(any(HttpUriRequest.class))).thenReturn(response);
    when(response.getStatusLine()).thenReturn(statusLine);
    when(statusLine.getStatusCode()).thenReturn(200);

    RestEntry<JsonObject> restEntry = new RestEntry<JsonObject>("test", new JsonObject());
    Optional<HttpUriRequest> request = writer.onNewRecord(restEntry);
    Assert.assertTrue(request.isPresent(), "No HttpUriRequest from onNewRecord");
    Assert.assertEquals("POST", request.get().getMethod());

    writer = spy(writer);
    writer.write(restEntry);

    verify(writer, times(1)).writeImpl(restEntry);
    verify(writer, times(1)).onNewRecord(restEntry);

    verify(writer, times(1)).sendRequest(any(HttpUriRequest.class));
    verify(client, times(1)).execute(any(HttpUriRequest.class));
    verify(writer, times(1)).waitForResponse(any(ListenableFuture.class));
    verify(writer, times(1)).processResponse(any(CloseableHttpResponse.class));
    verify(writer, never()).onConnect(any(URI.class));
  }

  public void testBatchInsertSuccess() throws IOException, URISyntaxException {
    final int recordSize = 113;
    final int batchSize = 25;

    State state = new State();
    state.appendToSetProp(CONF_PREFIX + BATCH_SIZE, Integer.toString(batchSize));
    state.appendToSetProp(CONF_PREFIX + BATCH_RESOURCE_PATH, "test");
    setup(SalesforceRestWriter.Operation.INSERT_ONLY_NOT_EXIST, state);

    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);
    when(client.execute(any(HttpUriRequest.class))).thenReturn(response);
    when(response.getStatusLine()).thenReturn(statusLine);
    when(statusLine.getStatusCode()).thenReturn(200);
    HttpEntity entity = mock(HttpEntity.class);
    when(response.getEntity()).thenReturn(entity);

    JsonObject jsonResponse = new JsonObject();
    jsonResponse.addProperty("hasErrors", false);

    ByteArrayInputStream[] streams = new ByteArrayInputStream[recordSize];
    for (int i=0; i < recordSize-1; i++) {
      streams[i] = new ByteArrayInputStream(jsonResponse.toString().getBytes());
    }
    when(entity.getContent()).thenReturn(new ByteArrayInputStream(jsonResponse.toString().getBytes()), streams);

    RestEntry<JsonObject> restEntry = new RestEntry<JsonObject>("test", new JsonObject());
    writer = spy(writer);
    for (int i = 0; i < recordSize; i++) {
      writer.write(restEntry);
    }
    writer.commit();

    Assert.assertEquals(writer.recordsWritten(), recordSize);

    verify(writer, times(recordSize)).writeImpl(restEntry);
    verify(writer, times(recordSize)).onNewRecord(restEntry);

    double sendCount = ((double)recordSize) / ((double)batchSize);
    sendCount = Math.ceil(sendCount);
    verify(writer, times((int)sendCount)).sendRequest(any(HttpUriRequest.class));

    verify(client, times((int)sendCount)).execute(any(HttpUriRequest.class));
    verify(writer, times((int)sendCount)).waitForResponse(any(ListenableFuture.class));
    verify(writer, times((int)sendCount)).processResponse(any(CloseableHttpResponse.class));
    verify(writer, times(1)).flush();

    verify(writer, never()).onConnect(any(URI.class));
  }

  public void testBatchInsertFailure() throws IOException, URISyntaxException {
    final int recordSize = 25;
    final int batchSize = recordSize;

    State state = new State();
    state.appendToSetProp(CONF_PREFIX + BATCH_SIZE, Integer.toString(batchSize));
    state.appendToSetProp(CONF_PREFIX + BATCH_RESOURCE_PATH, "test");
    setup(SalesforceRestWriter.Operation.INSERT_ONLY_NOT_EXIST, state);

    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);
    when(client.execute(any(HttpUriRequest.class))).thenReturn(response);
    when(response.getStatusLine()).thenReturn(statusLine);
    when(statusLine.getStatusCode()).thenReturn(200);
    HttpEntity entity = mock(HttpEntity.class);
    when(response.getEntity()).thenReturn(entity);

    JsonObject jsonResponse = new JsonObject();
    jsonResponse.addProperty("hasErrors", true);
    JsonArray resultJsonArr = new JsonArray();
    jsonResponse.add("results", resultJsonArr);
    JsonObject subResult1 = new JsonObject();
    subResult1.addProperty("statusCode", 201); //Success
    JsonObject subResult2 = new JsonObject();
    subResult2.addProperty("statusCode", 500); //Failure

    resultJsonArr.add(subResult1);
    resultJsonArr.add(subResult2);

    when(entity.getContent()).thenReturn(new ByteArrayInputStream(jsonResponse.toString().getBytes()));

    RestEntry<JsonObject> restEntry = new RestEntry<JsonObject>("test", new JsonObject());
    writer = spy(writer);
    for (int i = 0; i < recordSize-1; i++) {
      writer.write(restEntry);
    }
    try {
      writer.write(restEntry);
      Assert.fail("Should have failed with failed response. " + jsonResponse.toString());
    } catch (Exception e) {
      Assert.assertTrue(e instanceof RuntimeException);
    }

    Assert.assertEquals(writer.recordsWritten(), (long) 0L);

    verify(writer, times(recordSize)).writeImpl(restEntry);
    verify(writer, times(recordSize)).onNewRecord(restEntry);

    double sendCount = ((double)recordSize) / ((double)batchSize);
    sendCount = Math.ceil(sendCount);
    verify(writer, times((int)sendCount)).sendRequest(any(HttpUriRequest.class));

    verify(client, times((int)sendCount)).execute(any(HttpUriRequest.class));
    verify(writer, times((int)sendCount)).waitForResponse(any(ListenableFuture.class));
    verify(writer, times((int)sendCount)).processResponse(any(CloseableHttpResponse.class));
    verify(writer, never()).flush();

    verify(writer, never()).onConnect(any(URI.class));
  }

  public void testBatchInsertDuplicate() throws IOException, URISyntaxException {
    final int recordSize = 25;
    final int batchSize = recordSize;

    State state = new State();
    state.appendToSetProp(CONF_PREFIX + BATCH_SIZE, Integer.toString(batchSize));
    state.appendToSetProp(CONF_PREFIX + BATCH_RESOURCE_PATH, "test");
    setup(SalesforceRestWriter.Operation.INSERT_ONLY_NOT_EXIST, state);

    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);
    when(client.execute(any(HttpUriRequest.class))).thenReturn(response);
    when(response.getStatusLine()).thenReturn(statusLine);
    when(statusLine.getStatusCode()).thenReturn(200);
    HttpEntity entity = mock(HttpEntity.class);
    when(response.getEntity()).thenReturn(entity);

    JsonObject jsonResponse = new JsonObject();
    jsonResponse.addProperty("hasErrors", true);
    JsonArray resultJsonArr = new JsonArray();

    jsonResponse.add("results", resultJsonArr);
    JsonObject subResult1 = new JsonObject();
    subResult1.addProperty("statusCode", 400);
    JsonArray subResultArr = new JsonArray();
    JsonObject errJson = new JsonObject();
    errJson.addProperty("errorCode", SalesforceRestWriter.DUPLICATE_VALUE_ERR_CODE);
    subResultArr.add(errJson);
    subResult1.add("result", subResultArr);

    JsonObject subResult2 = new JsonObject();
    subResult2.addProperty("statusCode", 400);
    subResult2.add("result", subResultArr);

    resultJsonArr.add(subResult1);
    resultJsonArr.add(subResult2);

    when(entity.getContent()).thenReturn(new ByteArrayInputStream(jsonResponse.toString().getBytes()));

    RestEntry<JsonObject> restEntry = new RestEntry<JsonObject>("test", new JsonObject());
    writer = spy(writer);
    for (int i = 0; i < recordSize; i++) {
      writer.write(restEntry);
    }
    writer.commit();

    Assert.assertEquals(writer.recordsWritten(), recordSize);

    verify(writer, times(recordSize)).writeImpl(restEntry);
    verify(writer, times(recordSize)).onNewRecord(restEntry);

    double sendCount = ((double)recordSize) / ((double)batchSize);
    sendCount = Math.ceil(sendCount);
    verify(writer, times((int)sendCount)).sendRequest(any(HttpUriRequest.class));

    verify(client, times((int)sendCount)).execute(any(HttpUriRequest.class));
    verify(writer, times((int)sendCount)).waitForResponse(any(ListenableFuture.class));
    verify(writer, times((int)sendCount)).processResponse(any(CloseableHttpResponse.class));
    verify(writer, times(1)).flush();

    verify(writer, never()).onConnect(any(URI.class));
  }

  public void testUpsertSuccess() throws IOException, URISyntaxException {
    setup(SalesforceRestWriter.Operation.UPSERT);

    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);
    when(client.execute(any(HttpUriRequest.class))).thenReturn(response);
    when(response.getStatusLine()).thenReturn(statusLine);
    when(statusLine.getStatusCode()).thenReturn(200);

    RestEntry<JsonObject> restEntry = new RestEntry<JsonObject>("test", new JsonObject());
    Optional<HttpUriRequest> request = writer.onNewRecord(restEntry);
    Assert.assertTrue(request.isPresent(), "No HttpUriRequest from onNewRecord");
    Assert.assertEquals("PATCH", request.get().getMethod());

    writer = spy(writer);
    writer.write(restEntry);

    verify(writer, times(1)).writeImpl(restEntry);
    verify(writer, times(1)).onNewRecord(restEntry);
    verify(writer, times(1)).sendRequest(any(HttpUriRequest.class));
    verify(client, times(1)).execute(any(HttpUriRequest.class));
    verify(writer, times(1)).waitForResponse(any(ListenableFuture.class));
    verify(writer, times(1)).processResponse(any(CloseableHttpResponse.class));
    verify(writer, never()).onConnect(any(URI.class));
  }

  public void testInsertDuplicate() throws IOException, URISyntaxException {
    setup(SalesforceRestWriter.Operation.INSERT_ONLY_NOT_EXIST);

    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);
    when(client.execute(any(HttpUriRequest.class))).thenReturn(response);
    when(response.getStatusLine()).thenReturn(statusLine);
    when(statusLine.getStatusCode()).thenReturn(400);
    HttpEntity entity = mock(HttpEntity.class);
    when(response.getEntity()).thenReturn(entity);
    JsonObject json = new JsonObject();
    json.addProperty("errorCode", SalesforceRestWriter.DUPLICATE_VALUE_ERR_CODE);
    JsonArray jsonArray = new JsonArray();
    jsonArray.add(json);
    when(entity.getContent()).thenReturn(new ByteArrayInputStream(jsonArray.toString().getBytes()));

    RestEntry<JsonObject> restEntry = new RestEntry<JsonObject>("test", new JsonObject());
    Optional<HttpUriRequest> request = writer.onNewRecord(restEntry);
    Assert.assertTrue(request.isPresent(), "No HttpUriRequest from onNewRecord");
    Assert.assertEquals("POST", request.get().getMethod());

    writer = spy(writer);
    writer.write(restEntry);

    verify(writer, times(1)).writeImpl(restEntry);
    verify(writer, times(1)).onNewRecord(restEntry);

    verify(writer, times(1)).sendRequest(any(HttpUriRequest.class));
    verify(client, times(1)).execute(any(HttpUriRequest.class));
    verify(writer, times(1)).waitForResponse(any(ListenableFuture.class));
    verify(writer, times(1)).processResponse(any(CloseableHttpResponse.class));
    verify(writer, never()).onConnect(any(URI.class));
  }

  public void testFailure() throws IOException, URISyntaxException {
    setup(SalesforceRestWriter.Operation.INSERT_ONLY_NOT_EXIST);

    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);
    when(client.execute(any(HttpUriRequest.class))).thenReturn(response);
    when(response.getStatusLine()).thenReturn(statusLine);
    when(statusLine.getStatusCode()).thenReturn(400);

    RestEntry<JsonObject> restEntry = new RestEntry<JsonObject>("test", new JsonObject());
    Optional<HttpUriRequest> request = writer.onNewRecord(restEntry);
    Assert.assertTrue(request.isPresent(), "No HttpUriRequest from onNewRecord");
    Assert.assertEquals("POST", request.get().getMethod());

    writer = spy(writer);
    try {
      writer.write(restEntry);
      Assert.fail("Should fail on 400 status code");
    } catch (Exception e) {}


    verify(writer, times(1)).writeImpl(restEntry);
    verify(writer, times(1)).onNewRecord(restEntry);

    verify(writer, times(1)).sendRequest(any(HttpUriRequest.class));
    verify(client, times(1)).execute(any(HttpUriRequest.class));
    verify(writer, times(1)).waitForResponse(any(ListenableFuture.class));
    verify(writer, times(1)).processResponse(any(CloseableHttpResponse.class));
    verify(writer, never()).onConnect(any(URI.class));
  }

  public void testAccessTokenReacquire() throws IOException, URISyntaxException {
    setup(SalesforceRestWriter.Operation.INSERT_ONLY_NOT_EXIST);

    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);
    when(client.execute(any(HttpUriRequest.class))).thenReturn(response);
    when(response.getStatusLine()).thenReturn(statusLine);
    when(statusLine.getStatusCode()).thenReturn(401);

    RestEntry<JsonObject> restEntry = new RestEntry<JsonObject>("test", new JsonObject());
    Optional<HttpUriRequest> request = writer.onNewRecord(restEntry);
    Assert.assertTrue(request.isPresent(), "No HttpUriRequest from onNewRecord");
    Assert.assertEquals("POST", request.get().getMethod());

    writer = spy(writer);
    try {
      writer.write(restEntry);
    } catch (Exception e) { }


    verify(writer, times(1)).writeImpl(restEntry);
    verify(writer, times(1)).onNewRecord(restEntry);

    verify(writer, times(1)).sendRequest(any(HttpUriRequest.class));
    verify(client, times(1)).execute(any(HttpUriRequest.class));
    verify(writer, times(1)).waitForResponse(any(ListenableFuture.class));
    verify(writer, times(1)).processResponse(any(CloseableHttpResponse.class));
    verify(writer, times(1)).onConnect(any(URI.class));
  }
}
