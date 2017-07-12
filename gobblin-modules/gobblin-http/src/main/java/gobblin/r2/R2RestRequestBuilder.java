package gobblin.r2;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Queue;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.data.DataMap;
import com.linkedin.data.codec.JacksonDataCodec;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.restli.common.ResourceMethod;
import com.linkedin.restli.common.RestConstants;

import gobblin.http.HttpOperation;
import gobblin.utils.HttpUtils;
import gobblin.async.AsyncRequestBuilder;
import gobblin.async.BufferedRecord;


/**
 * Build {@link RestRequest} that can talk to restli services
 *
 * <p>
 *   This basic implementation builds a write request from a single record
 * </p>
 */
public class R2RestRequestBuilder implements AsyncRequestBuilder<GenericRecord, RestRequest> {
  private static final Logger LOG = LoggerFactory.getLogger(R2RestRequestBuilder.class);
  private static final JacksonDataCodec JACKSON_DATA_CODEC = new JacksonDataCodec();

  private final String urlTemplate;
  private final ResourceMethod method;
  private final String protocolVersion;

  public R2RestRequestBuilder(String urlTemplate, String verb, String protocolVersion) {
    this.urlTemplate = urlTemplate;
    method = ResourceMethod.fromString(verb);
    this.protocolVersion = protocolVersion;
  }

  @Override
  public R2Request<GenericRecord> buildRequest(Queue<BufferedRecord<GenericRecord>> buffer) {
    return buildWriteRequest(buffer.poll());
  }

  /**
   * Build a request from a single record
   */
  private R2Request<GenericRecord> buildWriteRequest(BufferedRecord<GenericRecord> record) {
    if (record == null) {
      return null;
    }

    R2Request<GenericRecord> request = new R2Request<>();
    HttpOperation httpOperation = HttpUtils.toHttpOperation(record.getRecord());
    // Set uri
    URI uri = HttpUtils.buildURI(urlTemplate, httpOperation.getKeys(), httpOperation.getQueryParams());
    if (uri == null) {
      return null;
    }

    RestRequestBuilder builder = new RestRequestBuilder(uri).setMethod(method.getHttpMethod().toString());
    // Set headers
    Map<String, String> headers = httpOperation.getHeaders();
    if (headers != null && headers.size() != 0) {
      builder.setHeaders(headers);
    }
    builder.setHeader(RestConstants.HEADER_RESTLI_PROTOCOL_VERSION, protocolVersion);
    builder.setHeader(RestConstants.HEADER_RESTLI_REQUEST_METHOD, method.toString());

    // Add payload
    int bytesWritten = addPayload(builder, httpOperation.getBody());
    if (bytesWritten == -1) {
      throw new RuntimeException("Fail to write payload into request");
    }

    request.markRecord(record, bytesWritten);
    request.setRawRequest(build(builder));
    return request;
  }

  /**
   * Add payload to request. By default, payload is sent as application/json
   */
  protected int addPayload(RestRequestBuilder builder, String payload) {
    if (payload == null || payload.length() == 0) {
      return 0;
    }

    builder.setHeader(RestConstants.HEADER_CONTENT_TYPE, RestConstants.HEADER_VALUE_APPLICATION_JSON);
    try {
      DataMap data = JACKSON_DATA_CODEC.stringToMap(payload);
      byte[] bytes = JACKSON_DATA_CODEC.mapToBytes(data);
      builder.setEntity(bytes);
      return bytes.length;
    } catch (IOException e) {
      throw new RuntimeException("Fail to convert payload: " + payload, e);
    }
  }

  /**
   * Add this method for argument capture in test
   */
  @VisibleForTesting
  public RestRequest build(RestRequestBuilder builder) {
    return builder.build();
  }
}
