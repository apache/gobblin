package gobblin.writer.http;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.mockito.Matchers;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.gson.JsonObject;
import com.typesafe.config.Config;

import gobblin.configuration.State;
import gobblin.converter.http.RestEntry;
import gobblin.http.SalesforceAuth;
import gobblin.writer.AsyncWriterManager;

import static gobblin.http.ApacheHttpClient.STATIC_SVC_ENDPOINT;
import static gobblin.writer.http.AsyncHttpWriterBuilder.CONF_PREFIX;
import static gobblin.writer.http.SalesForceRestWriterBuilder.OPERATION;
import static gobblin.writer.http.SalesforceWriterBuilder.BATCH_RESOURCE_PATH;
import static gobblin.writer.http.SalesforceWriterBuilder.BATCH_SIZE;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


@Test
public class SalesforceWriterTest {
  /**
   * Integration test for batch insert success
   */
  public void testBatchInsertSuccess()
      throws URISyntaxException, IOException {
    final int recordSize = 20;
    final int batchSize = 10;

    SalesforceWriterBuilder.Operation operation = SalesforceWriterBuilder.Operation.INSERT_ONLY_NOT_EXIST;
    State state = new State();
    state.appendToSetProp(CONF_PREFIX + BATCH_SIZE, Integer.toString(batchSize));
    state.appendToSetProp(CONF_PREFIX + BATCH_RESOURCE_PATH, "test");

    state.appendToSetProp(CONF_PREFIX + STATIC_SVC_ENDPOINT, "test");
    state.appendToSetProp(CONF_PREFIX + SalesforceAuth.CLIENT_ID, "test");
    state.appendToSetProp(CONF_PREFIX + SalesforceAuth.CLIENT_SECRET, "test");
    state.appendToSetProp(CONF_PREFIX + SalesforceAuth.USER_ID, "test");
    state.appendToSetProp(CONF_PREFIX + SalesforceAuth.PASSWORD, "test");
    state.appendToSetProp(CONF_PREFIX + SalesforceAuth.USE_STRONG_ENCRYPTION, "test");
    state.appendToSetProp(CONF_PREFIX + SalesforceAuth.SECURITY_TOKEN, "test");
    state.appendToSetProp(CONF_PREFIX + OPERATION, operation.name());

    SalesforceWriterBuilder builder = new SalesforceWriterBuilder();
    builder = spy(builder);
    HttpClientBuilder httpClientBuilder = mock(HttpClientBuilder.class);
    doReturn(httpClientBuilder).when(builder).createHttpClientBuilder(Matchers.any(State.class));

    SalesforceAuth auth = mock(SalesforceAuth.class);
    when(auth.authenticate()).thenReturn(true);
    when(auth.getAccessToken()).thenReturn("token");
    when(auth.getInstanceUrl()).thenReturn(new URI("http://data"));
    doReturn(auth).when(builder).createSalesforceAuth(Matchers.any(Config.class));

    CloseableHttpClient client = mock(CloseableHttpClient.class);
    when(httpClientBuilder.build()).thenReturn(client);

    AsyncWriterManager<RestEntry<JsonObject>> writer =
        (AsyncWriterManager<RestEntry<JsonObject>>) builder.fromState(state).build();

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
    Assert.assertEquals(recordSize, writer.recordsWritten());
    writer.close();
  }
}
