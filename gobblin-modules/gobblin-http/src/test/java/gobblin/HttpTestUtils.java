package gobblin;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.testng.Assert;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestRequestBuilder;

import gobblin.http.HttpOperation;
import gobblin.async.BufferedRecord;


public class HttpTestUtils {
  public static Queue<BufferedRecord<GenericRecord>> createQueue(int size, boolean isHttpOperation) {
    Queue<BufferedRecord<GenericRecord>> queue = new ArrayDeque<>(size);
    for (int i = 0; i < size; i++) {
      Map<String, String> keys = new HashMap<>();
      keys.put("part1", i + "1");
      keys.put("part2", i + "2");
      Map<String, String> queryParams = new HashMap<>();
      queryParams.put("param1", i + "1");
      GenericRecord record = isHttpOperation ? new HttpOperation() : new MockGenericRecord();
      record.put("keys", keys);
      record.put("queryParams", queryParams);
      record.put("body", "{\"id\":\"id" + i + "\"}");
      BufferedRecord<GenericRecord> item = new BufferedRecord<>(record, null);
      queue.add(item);
    }
    return queue;
  }

  public static void assertEqual(RequestBuilder actual, RequestBuilder expect)
      throws IOException {
    // Check entity
    HttpEntity actualEntity = actual.getEntity();
    HttpEntity expectedEntity = expect.getEntity();
    if (actualEntity == null) {
      Assert.assertTrue(expectedEntity == null);
    } else {
      Assert.assertEquals(actualEntity.getContentLength(), expectedEntity.getContentLength());
      String actualContent = IOUtils.toString(actualEntity.getContent(), StandardCharsets.UTF_8);
      String expectedContent = IOUtils.toString(expectedEntity.getContent(), StandardCharsets.UTF_8);
      Assert.assertEquals(actualContent, expectedContent);
    }

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

  public static void assertEqual(RestRequestBuilder actual, RestRequestBuilder expect)
      throws IOException {
    // Check entity
    ByteString actualEntity = actual.getEntity();
    ByteString expectedEntity = expect.getEntity();
    if (actualEntity == null) {
      Assert.assertTrue(expectedEntity == null);
    } else {
      Assert.assertEquals(actualEntity.length(), expectedEntity.length());
      Assert.assertEquals(actualEntity.asString(StandardCharsets.UTF_8),expectedEntity.asString(StandardCharsets.UTF_8));
    }

    // Check request
    RestRequest actualRequest = actual.build();
    RestRequest expectedRequest = expect.build();
    Assert.assertEquals(actualRequest.getMethod(), expectedRequest.getMethod());
    Assert.assertEquals(actualRequest.getURI().toString(), expectedRequest.getURI().toString());

    Map<String, String> actualHeaders = actualRequest.getHeaders();
    Map<String, String> expectedHeaders = expectedRequest.getHeaders();
    Assert.assertEquals(actualHeaders.size(), expectedHeaders.size());
    for (String key: actualHeaders.keySet()) {
      Assert.assertEquals(actualHeaders.get(key), expectedHeaders.get(key));
    }
  }
}
