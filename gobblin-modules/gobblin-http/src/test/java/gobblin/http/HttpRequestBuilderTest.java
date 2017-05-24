package gobblin.http;

import java.io.IOException;
import java.util.Queue;

import org.apache.avro.generic.GenericRecord;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.HttpTestUtils;
import gobblin.writer.http.AsyncWriteRequest;
import gobblin.writer.http.BufferedRecord;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;


@Test
public class HttpRequestBuilderTest {
  /**
   * Build a {@link HttpUriRequest} from a {@link GenericRecord}
   */
  public void testBuildWriteRequest()
      throws IOException {
    String urlTemplate = "a/part1:${part1}/a/part2:${part2}";
    String verb = "post";
    HttpRequestBuilder builder = spy(new HttpRequestBuilder(urlTemplate, verb, "application/json"));
    ArgumentCaptor<RequestBuilder> requestBuilderArgument = ArgumentCaptor.forClass(RequestBuilder.class);

    Queue<BufferedRecord<GenericRecord>> queue = HttpTestUtils.createQueue(1, false);
    AsyncWriteRequest<GenericRecord, HttpUriRequest> request = builder.buildWriteRequest(queue);
    verify(builder).build(requestBuilderArgument.capture());

    RequestBuilder expected = RequestBuilder.post();
    expected.setUri("a/part1:01/a/part2:02?param1=01");
    String payloadStr = "{\"id\":\"id0\"}";
    expected.addHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType())
        .setEntity(new StringEntity(payloadStr, ContentType.APPLICATION_JSON));

    // Compare HttpUriRequest
    HttpTestUtils.assertEqual(requestBuilderArgument.getValue(), expected);
    Assert.assertEquals(request.getRecordCount(), 1);
    Assert.assertEquals(queue.size(), 0);
  }
}
