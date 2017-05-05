package gobblin.writer.http;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import gobblin.http.ResponseStatus;
import gobblin.http.SalesforceClient;
import gobblin.http.StatusType;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@Test
public class SalesforceResponseHandlerTest {
  /**
   * Test handle response for single record request
   */
  public void testHandleResponse()
      throws IOException {
    SalesforceClient client = mock(SalesforceClient.class);
    SalesforceResponseHandler handler = new SalesforceResponseHandler(client, SalesforceWriterBuilder.Operation.INSERT_ONLY_NOT_EXIST, false);
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);
    when(response.getStatusLine()).thenReturn(statusLine);

    // Success 200
    when(statusLine.getStatusCode()).thenReturn(200);
    ResponseStatus status = handler.handleResponse(response);
    Assert.assertEquals(status.getType().toString(), StatusType.OK.toString());
    // Success 300
    when(statusLine.getStatusCode()).thenReturn(300);
    status = handler.handleResponse(response);
    Assert.assertEquals(status.getType().toString(), StatusType.OK.toString());

    // Server error 401
    when(statusLine.getStatusCode()).thenReturn(401);
    status = handler.handleResponse(response);
    Assert.assertEquals(status.getType().toString(), StatusType.SERVER_ERROR.toString());
    verify(client, times(1)).setAccessToken(null);
    // Server error 403
    when(statusLine.getStatusCode()).thenReturn(403);
    status = handler.handleResponse(response);
    Assert.assertEquals(status.getType().toString(), StatusType.SERVER_ERROR.toString());

    when(statusLine.getStatusCode()).thenReturn(400);
    HttpEntity entity = mock(HttpEntity.class);
    when(response.getEntity()).thenReturn(entity);
    // Server error 400
    when(statusLine.getStatusCode()).thenReturn(400);
    status = handler.handleResponse(response);
    Assert.assertEquals(status.getType().toString(), StatusType.SERVER_ERROR.toString());
    // Success 400 duplicate
    JsonObject json = new JsonObject();
    json.addProperty("errorCode", SalesforceResponseHandler.DUPLICATE_VALUE_ERR_CODE);
    JsonArray jsonArray = new JsonArray();
    jsonArray.add(json);
    when(entity.getContent()).thenReturn(new ByteArrayInputStream(jsonArray.toString().getBytes()));
    status = handler.handleResponse(response);
    Assert.assertEquals(status.getType().toString(), StatusType.OK.toString());

    // Fail 400
    handler = new SalesforceResponseHandler(client, SalesforceWriterBuilder.Operation.UPSERT, false);
    status = handler.handleResponse(response);
    Assert.assertEquals(status.getType().toString(), StatusType.CLIENT_ERROR.toString());
  }

  /**
   * Test handle response for successful batch request
   */
  public void testHandleSuccessBatchResponse()
      throws IOException {
    final int recordSize = 2;
    SalesforceClient client = mock(SalesforceClient.class);
    SalesforceResponseHandler handler = new SalesforceResponseHandler(client, SalesforceWriterBuilder.Operation.INSERT_ONLY_NOT_EXIST, true);

    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);
    when(response.getStatusLine()).thenReturn(statusLine);
    HttpEntity entity = mock(HttpEntity.class);
    when(response.getEntity()).thenReturn(entity);

    JsonObject jsonResponse = new JsonObject();
    jsonResponse.addProperty("hasErrors", false);

    ByteArrayInputStream[] streams = new ByteArrayInputStream[recordSize];
    for (int i=0; i < recordSize-1; i++) {
      streams[i] = new ByteArrayInputStream(jsonResponse.toString().getBytes());
    }
    when(entity.getContent()).thenReturn(new ByteArrayInputStream(jsonResponse.toString().getBytes()), streams);


    // Success 200
    when(statusLine.getStatusCode()).thenReturn(200);
    ResponseStatus status = handler.handleResponse(response);
    Assert.assertEquals(status.getType().toString(), StatusType.OK.toString());
    // Success 300
    when(statusLine.getStatusCode()).thenReturn(300);
    status = handler.handleResponse(response);
    Assert.assertEquals(status.getType().toString(), StatusType.OK.toString());
  }

  /**
   * Test handle response for batch request with duplicate
   */
  public void testHandleBatchResponseWithDuplicate()
      throws IOException {
    SalesforceClient client = mock(SalesforceClient.class);
    SalesforceResponseHandler handler = new SalesforceResponseHandler(client, SalesforceWriterBuilder.Operation.INSERT_ONLY_NOT_EXIST, true);

    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);
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

    // Success
    when(entity.getContent()).thenReturn(new ByteArrayInputStream(jsonResponse.toString().getBytes()));
    ResponseStatus status = handler.handleResponse(response);
    Assert.assertEquals(status.getType().toString(), StatusType.OK.toString());
    // Fail
    when(entity.getContent()).thenReturn(new ByteArrayInputStream(jsonResponse.toString().getBytes()));
    handler = new SalesforceResponseHandler(client, SalesforceWriterBuilder.Operation.UPSERT, true);
    status = handler.handleResponse(response);
    Assert.assertEquals(status.getType().toString(), StatusType.CLIENT_ERROR.toString());
  }

  /**
   * Test handle response for failed batch request
   */
  public void testHandleFailedBatchResponse()
      throws IOException {
    SalesforceClient client = mock(SalesforceClient.class);
    SalesforceResponseHandler handler = new SalesforceResponseHandler(client, SalesforceWriterBuilder.Operation.INSERT_ONLY_NOT_EXIST, true);

    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);
    when(response.getStatusLine()).thenReturn(statusLine);
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

    // Fail 200
    when(statusLine.getStatusCode()).thenReturn(200);
    ResponseStatus status = handler.handleResponse(response);
    Assert.assertEquals(status.getType().toString(), StatusType.CLIENT_ERROR.toString());
    // Fail 405
    when(statusLine.getStatusCode()).thenReturn(405);
    status = handler.handleResponse(response);
    Assert.assertEquals(status.getType().toString(), StatusType.CLIENT_ERROR.toString());
  }
}
