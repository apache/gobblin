package gobblin.writer.http;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collection;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import lombok.extern.log4j.Log4j;

import gobblin.converter.http.RestEntry;
import gobblin.http.BatchRequestBuilder;


@Log4j
public class SalesforceRequestBuilder implements gobblin.http.RequestBuilder<RestEntry<JsonObject>, HttpUriRequest>,
                                                 BatchRequestBuilder<RestEntry<JsonObject>, HttpUriRequest> {
  private final SalesforceClient client;
  private final SalesforceWriterBuilder.Operation operation;
  private final Optional<String> batchResourcePath;

  SalesforceRequestBuilder(SalesforceClient client, SalesforceWriterBuilder.Operation operation,
      Optional<String> batchResourcePath) {
    this.client = client;
    this.operation = operation;
    this.batchResourcePath = batchResourcePath;
  }

  @Override
  public HttpUriRequest buildRequest(RestEntry<JsonObject> record) {
    Preconditions.checkNotNull(record, "Record should not be null");

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

    builder.setUri(combineUrl(client.getServerHost(), record.getResourcePath()));
    JsonObject payload = record.getRestEntryVal();
    return newRequest(builder, payload);
  }

  @Override
  public HttpUriRequest buildRequest(Collection<RestEntry<JsonObject>> records) {
    if (records == null && records.size() == 0) {
      return null;
    }

    if (records.size() > 1) {
      JsonObject payload = newPayloadForBatch(records);
      RequestBuilder builder = RequestBuilder.post().setUri(combineUrl(client.getServerHost(), batchResourcePath));
      return newRequest(builder, payload);
    } else {
      return buildRequest(records.iterator().next());
    }
  }

  /**
   * @return JsonObject contains batch records
   */
  private JsonObject newPayloadForBatch(Collection<RestEntry<JsonObject>> batchRecords) {
    JsonArray batchRequests = new JsonArray();
    for (RestEntry<JsonObject> record : batchRecords) {
      batchRequests.add(newSubrequest(record));
    }
    JsonObject payload = new JsonObject();
    payload.add("batchRequests", batchRequests);
    return payload;
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
