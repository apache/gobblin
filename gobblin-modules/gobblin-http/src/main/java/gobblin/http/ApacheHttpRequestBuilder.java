package gobblin.http;

import java.net.URI;
import java.util.Map;
import java.util.Queue;

import org.apache.avro.generic.GenericRecord;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import gobblin.utils.HttpUtils;
import gobblin.async.AsyncRequestBuilder;
import gobblin.async.BufferedRecord;


/**
 * Build {@link HttpUriRequest} that can talk to http services. Now only text/plain and application/json are supported
 *
 * <p>
 *   This basic implementation builds a write request from a single record. However, it has the extensibility to build
 *   a write request from batched records, depending on specific implementation of {@link #buildRequest(Queue)}
 * </p>
 */
public class ApacheHttpRequestBuilder implements AsyncRequestBuilder<GenericRecord, HttpUriRequest> {
  private static final Logger LOG = LoggerFactory.getLogger(ApacheHttpRequestBuilder.class);

  private final String urlTemplate;
  private final String verb;
  private final ContentType contentType;

  public ApacheHttpRequestBuilder(String urlTemplate, String verb, String contentType) {
    this.urlTemplate = urlTemplate;
    this.verb = verb;
    this.contentType = createContentType(contentType);
  }

  @Override
  public ApacheHttpRequest<GenericRecord> buildRequest(Queue<BufferedRecord<GenericRecord>> buffer) {
    return buildWriteRequest(buffer.poll());
  }

  /**
   * Build a write request from a single record
   */
  private ApacheHttpRequest<GenericRecord> buildWriteRequest(BufferedRecord<GenericRecord> record) {
    if (record == null) {
      return null;
    }

    ApacheHttpRequest<GenericRecord> request = new ApacheHttpRequest<>();
    HttpOperation httpOperation = HttpUtils.toHttpOperation(record.getRecord());

    // Set uri
    URI uri = HttpUtils.buildURI(urlTemplate, httpOperation.getKeys(), httpOperation.getQueryParams());
    if (uri == null) {
      return null;
    }

    RequestBuilder builder = RequestBuilder.create(verb.toUpperCase());
    builder.setUri(uri);

    // Set headers
    Map<String, String> headers = httpOperation.getHeaders();
    if (headers != null && headers.size() != 0) {
      for (Map.Entry<String, String> header : headers.entrySet()) {
        builder.setHeader(header.getKey(), header.getValue());
      }
    }

    // Add payload
    int bytesWritten = addPayload(builder, httpOperation.getBody());
    if (bytesWritten == -1) {
      throw new RuntimeException("Fail to write payload into request");
    }

    request.setRawRequest(build(builder));
    request.markRecord(record, bytesWritten);
    return request;
  }

  /**
   * Add payload to request. By default, payload is sent as application/json
   */
  protected int addPayload(RequestBuilder builder, String payload) {
    if (payload == null || payload.length() == 0) {
      return 0;
    }


    builder.setHeader(HttpHeaders.CONTENT_TYPE, contentType.getMimeType());
    builder.setEntity(new StringEntity(payload, contentType));
    return payload.length();
  }

  public static ContentType createContentType(String contentType) {
    switch (contentType) {
      case "application/json":
        return ContentType.APPLICATION_JSON;
      case "text/plain":
        return ContentType.TEXT_PLAIN;
      default:
        throw new RuntimeException("contentType not supported: " + contentType);
    }
  }

  /**
   * Add this method for argument capture in test
   */
  @VisibleForTesting
  public HttpUriRequest build(RequestBuilder builder) {
    return builder.build();
  }
}
