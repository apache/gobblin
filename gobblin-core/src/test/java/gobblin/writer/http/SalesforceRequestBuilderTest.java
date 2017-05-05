package gobblin.writer.http;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Queue;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import gobblin.converter.http.RestEntry;
import gobblin.http.SalesforceClient;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@Test
public class SalesforceRequestBuilderTest {
  static final Gson GSON = new Gson();

  /**
   * Test build request with a single record
   */
  public void testBuildRequest()
      throws URISyntaxException, IOException {
    SalesforceClient client = mock(SalesforceClient.class);
    when(client.getAccessToken()).thenReturn("token");
    when(client.getServerHost()).thenReturn(new URI("http://host"));

    SalesforceRequestBuilder builder =
        new SalesforceRequestBuilder(client, SalesforceWriterBuilder.Operation.INSERT_ONLY_NOT_EXIST,
            Optional.<String>absent(), 1);
    doSingleRecordTest(builder, RequestBuilder.post());

    builder =
        new SalesforceRequestBuilder(client, SalesforceWriterBuilder.Operation.UPSERT,
            Optional.<String>absent(), 1);
    doSingleRecordTest(builder, RequestBuilder.patch());
  }

  public void testBuildBatchRequest()
      throws URISyntaxException, IOException {
    SalesforceClient client = mock(SalesforceClient.class);
    when(client.getAccessToken()).thenReturn("token");
    when(client.getServerHost()).thenReturn(new URI("http://host"));
    Optional<String> batchResourcePath = Optional.of("batchResource");
    SalesforceRequestBuilder builder =
        new SalesforceRequestBuilder(client, SalesforceWriterBuilder.Operation.INSERT_ONLY_NOT_EXIST,
            batchResourcePath, 2);
    doBatchTest(builder, RequestBuilder.post(),
        "{\"batchRequests\":[{\"url\":\"resource\",\"richInput\":{\"id\":0},\"method\":\"POST\"},{\"url\":\"resource\",\"richInput\":{\"id\":1},\"method\":\"POST\"}]}");
    builder =
        new SalesforceRequestBuilder(client, SalesforceWriterBuilder.Operation.UPSERT,
            batchResourcePath, 2);
    doBatchTest(builder, RequestBuilder.post(),
        "{\"batchRequests\":[{\"url\":\"resource\",\"richInput\":{\"id\":0},\"method\":\"PATCH\"},{\"url\":\"resource\",\"richInput\":{\"id\":1},\"method\":\"PATCH\"}]}");
  }

  private void doBatchTest(SalesforceRequestBuilder salesforceBuilder, RequestBuilder expected, String payloadStr)
      throws IOException {
    SalesforceRequestBuilder builder = spy(salesforceBuilder);
    Queue<BufferedRecord<RestEntry<JsonObject>>> queue = createQueue(4);
    AsyncWriteRequest<RestEntry<JsonObject>, HttpUriRequest> request = builder.buildRequest(queue);
    ArgumentCaptor<RequestBuilder> requestBuilderArgument = ArgumentCaptor.forClass(RequestBuilder.class);
    verify(builder).build(requestBuilderArgument.capture());

    // Construct expected raw request
    expected.setUri("http://host/batchResource");
    JsonObject payload = GSON.fromJson(payloadStr, JsonObject.class);
    expected.addHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType())
        .setEntity(new StringEntity(payload.toString(), ContentType.APPLICATION_JSON));
    // Compare HttpUriRequest
    assertEqual(requestBuilderArgument.getValue(), expected);
    Assert.assertEquals(request.getRecordCount(), 2);
    Assert.assertEquals(queue.size(), 2);
  }

  private void doSingleRecordTest(SalesforceRequestBuilder salesforceBuilder, RequestBuilder expected)
      throws IOException {
    SalesforceRequestBuilder builder = spy(salesforceBuilder);
    AsyncWriteRequest<RestEntry<JsonObject>, HttpUriRequest> request = builder.buildRequest(createQueue(1));
    ArgumentCaptor<RequestBuilder> requestBuilderArgument = ArgumentCaptor.forClass(RequestBuilder.class);
    verify(builder).build(requestBuilderArgument.capture());

    // Construct expected raw request
    expected.setUri("http://host/resource");
    String payloadStr = "{\"id\":0}";
    JsonObject payload = GSON.fromJson(payloadStr, JsonObject.class);
    expected.addHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType())
        .setEntity(new StringEntity(payload.toString(), ContentType.APPLICATION_JSON));
    // Compare HttpUriRequest
    assertEqual(requestBuilderArgument.getValue(), expected);
    Assert.assertEquals(request.bytesWritten, payload.toString().length());
    Assert.assertEquals(request.getRecordCount(), 1);
  }

  private Queue<BufferedRecord<RestEntry<JsonObject>>> createQueue(int size) {
    Queue<BufferedRecord<RestEntry<JsonObject>>> queue = new ArrayDeque<>(1);
    for (int i = 0; i < size; i++) {
      JsonObject json = new JsonObject();
      json.addProperty("id", i);
      RestEntry<JsonObject> entry = new RestEntry<>("resource", json);
      BufferedRecord<RestEntry<JsonObject>> record = new BufferedRecord<>(entry, null);
      queue.add(record);
    }
    return queue;
  }

  private void assertEqual(RequestBuilder actual, RequestBuilder expect)
      throws IOException {
    // Check entity
    HttpEntity actualEntity = actual.getEntity();
    HttpEntity expectedEntity = expect.getEntity();
    Assert.assertEquals(actualEntity.getContentLength(), expectedEntity.getContentLength());
    String actualContent = IOUtils.toString(actualEntity.getContent(), StandardCharsets.UTF_8);
    String expectedContent = IOUtils.toString(expectedEntity.getContent(), StandardCharsets.UTF_8);
    Assert.assertEquals(actualContent, expectedContent);

    // Check request
    HttpUriRequest actualRequest = actual.build();
    HttpUriRequest expectedRequest = expect.build();
    Assert.assertEquals(actualRequest.getMethod(), expectedRequest.getMethod());
    Assert.assertEquals(actualRequest.getURI().toString(), expectedRequest.getURI().toString());

    Header[] actualHeaders = actualRequest.getAllHeaders();
    Header[] expectedHeaders = expectedRequest.getAllHeaders();
    Assert.assertEquals(actualHeaders.length, expectedHeaders.length);
    for (int i = 0; i < actualHeaders.length; i++) {
      Assert.assertEquals(actualHeaders[i].toString(), expectedHeaders[i].toString());
    }
  }
}
