package gobblin.writer.http;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Queue;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import lombok.extern.log4j.Log4j;

import gobblin.converter.http.RestEntry;
import gobblin.http.SalesforceClient;


@Log4j
public class SalesforceRequestBuilder implements AsyncWriteRequestBuilder<RestEntry<JsonObject>, HttpUriRequest> {
  private final SalesforceClient client;
  private final SalesforceWriterBuilder.Operation operation;
  private final Optional<String> batchResourcePath;
  private final int maxRecordsInBatch;

  SalesforceRequestBuilder(SalesforceClient client, SalesforceWriterBuilder.Operation operation,
      Optional<String> batchResourcePath, int maxRecordsInBatch) {
    this.client = client;
    this.operation = operation;
    this.batchResourcePath = batchResourcePath;
    this.maxRecordsInBatch = maxRecordsInBatch;
  }

  @Override
  public AsyncWriteRequest<RestEntry<JsonObject>, HttpUriRequest> buildRequest(
      Queue<BufferedRecord<RestEntry<JsonObject>>> buffer) {
    if (maxRecordsInBatch > 1) {
      return buildBatchRequest(buffer);
    } else {
      return buildRequest(buffer.poll());
    }
  }

  private AsyncWriteRequest<RestEntry<JsonObject>, HttpUriRequest> buildRequest(
      BufferedRecord<RestEntry<JsonObject>> record) {
    if (record == null) {
      return null;
    }

    RequestBuilder builder;
    switch (operation) {
      case INSERT_ONLY_NOT_EXIST:
        builder = RequestBuilder.post();
        break;
      case UPSERT:
        builder = RequestBuilder.patch();
        break;
      default:
        throw new IllegalArgumentException(operation + " is not supported.");
    }

    RestEntry<JsonObject> rawRecord = record.getRecord();
    builder.setUri(combineUrl(client.getServerHost(), rawRecord.getResourcePath()));
    JsonObject payload = rawRecord.getRestEntryVal();
    AsyncWriteRequest<RestEntry<JsonObject>, HttpUriRequest> request = new AsyncWriteRequest<>();
    request.markRecord(record, payload.toString().length());
    request.setRawRequest(newRequest(builder, payload));
    return request;
  }

  private AsyncWriteRequest<RestEntry<JsonObject>, HttpUriRequest> buildBatchRequest(
      Queue<BufferedRecord<RestEntry<JsonObject>>> buffer) {
    if (buffer.size() == 0) {
      return null;
    }

    AsyncWriteRequest<RestEntry<JsonObject>, HttpUriRequest> request = new AsyncWriteRequest<>();
    BufferedRecord<RestEntry<JsonObject>> record;
    int count = maxRecordsInBatch;
    JsonArray batchRequests = new JsonArray();
    while (count > 0) {
      if ((record = buffer.poll()) == null) {
        break;
      }
      RestEntry<JsonObject> rawRecord = record.getRecord();
      batchRequests.add(newSubrequest(rawRecord));
      request.markRecord(record, rawRecord.getRestEntryVal().toString().length());
      count--;
    }

    JsonObject payload = new JsonObject();
    payload.add("batchRequests", batchRequests);
    RequestBuilder builder = RequestBuilder.post().setUri(combineUrl(client.getServerHost(), batchResourcePath));
    request.setRawRequest(newRequest(builder, payload));
    return request;
  }

  /**
   * Create batch subrequest. For more detail @link https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/requests_composite_batch.htm
   *
   * @param record
   * @return
   */
  private JsonObject newSubrequest(RestEntry<JsonObject> record) {
    Preconditions.checkArgument(record.getResourcePath().isPresent(), "Resource path is not defined");
    JsonObject subReq = new JsonObject();
    subReq.addProperty("url", record.getResourcePath().get());
    subReq.add("richInput", record.getRestEntryVal());

    switch (operation) {
      case INSERT_ONLY_NOT_EXIST:
        subReq.addProperty("method", "POST");
        break;
      case UPSERT:
        subReq.addProperty("method", "PATCH");
        break;
      default:
        throw new IllegalArgumentException(operation + " is not supported.");
    }
    return subReq;
  }

  private HttpUriRequest newRequest(RequestBuilder builder, JsonElement payload) {
    try {
      builder.addHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType())
          .setEntity(new StringEntity(payload.toString(), ContentType.APPLICATION_JSON));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    log.debug("Request builder: " + ToStringBuilder.reflectionToString(builder, ToStringStyle.SHORT_PREFIX_STYLE));
    return build(builder);
  }

  /**
   * Add this method for argument capture in test
   */
  @VisibleForTesting
  public HttpUriRequest build(RequestBuilder builder) {
    return builder.build();
  }

  static URI combineUrl(URI uri, Optional<String> resourcePath) {
    if (!resourcePath.isPresent()) {
      return uri;
    }

    try {
      return new URL(uri.toURL(), resourcePath.get()).toURI();
    } catch (MalformedURLException | URISyntaxException e) {
      throw new RuntimeException("Failed combining URL", e);
    }
  }
}
