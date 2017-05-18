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

import gobblin.utils.HttpConstants;
import gobblin.utils.HttpUtils;
import gobblin.writer.http.AsyncWriteRequest;
import gobblin.writer.http.AsyncWriteRequestBuilder;
import gobblin.writer.http.BufferedRecord;


/**
 * Build {@link HttpUriRequest} that can talk to http services
 *
 * <p>
 *   This basic implementation builds a write request from a single record. However, it has the extensibility to build
 *   a write request from batched records, depending on specific implementation of {@link #buildWriteRequest(Queue)}
 * </p>
 */
public class HttpRequestBuilder implements AsyncWriteRequestBuilder<GenericRecord, HttpUriRequest> {
  private static final Logger LOG = LoggerFactory.getLogger(HttpRequestBuilder.class);
  private final String urlTemplate;
  private final String verb;

  public HttpRequestBuilder(String urlTemplate, String verb) {
    this.urlTemplate = urlTemplate;
    this.verb = verb;
  }

  @Override
  public AsyncWriteRequest<GenericRecord, HttpUriRequest> buildWriteRequest(Queue<BufferedRecord<GenericRecord>> buffer) {
    return buildWriteRequest(buffer.poll());
  }

  /**
   * Build a write request from a single record
   */
  private AsyncWriteRequest<GenericRecord, HttpUriRequest> buildWriteRequest(BufferedRecord<GenericRecord> record) {
    if (record == null) {
      return null;
    }

    AsyncWriteRequest<GenericRecord, HttpUriRequest> request = new AsyncWriteRequest<>();
    GenericRecord httpOperation = record.getRecord();

    // Set uri
    URI uri = HttpUtils.buildURI(urlTemplate, HttpUtils.toStringMap(httpOperation.get(HttpConstants.KEYS)),
        HttpUtils.toStringMap(httpOperation.get(HttpConstants.QUERY_PARAMS)));
    if (uri == null) {
      return null;
    }

    RequestBuilder builder = RequestBuilder.create(verb);
    builder.setUri(uri);

    // Set headers
    for (Map.Entry<String, String> header : HttpUtils.toStringMap(httpOperation.get(HttpConstants.HEADERS))
        .entrySet()) {
      builder.setHeader(header.getKey(), header.getValue());
    }

    // Add payload
    int bytesWritten = addPayload(builder, httpOperation.get(HttpConstants.BODY).toString());
    if (bytesWritten == -1) {
      return null;
    }

    request.setRawRequest(builder.build());
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

    builder.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
    builder.setEntity(new StringEntity(payload, ContentType.APPLICATION_JSON));
    return payload.length();
  }
}
