package gobblin.converter;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;

import gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.http.ApacheHttpAsyncClient;
import gobblin.http.ApacheHttpResponseHandler;
import gobblin.http.ApacheHttpResponseStatus;
import gobblin.http.ApacheHttpRequestBuilder;
import gobblin.http.HttpRequestResponseRecord;
import gobblin.http.ResponseStatus;
import gobblin.utils.HttpConstants;

/**
 * Apache version of http join converter
 */
@Slf4j
public class AvroApacheHttpJoinConverter extends AvroHttpJoinConverter<HttpUriRequest, HttpResponse> {
  @Override
  public ApacheHttpAsyncClient createHttpClient(Config config, SharedResourcesBroker<GobblinScopeTypes> broker) {
    return new ApacheHttpAsyncClient(HttpAsyncClientBuilder.create(), config, broker);
  }

  @Override
  public ApacheHttpResponseHandler createResponseHandler(Config config) {
    return new ApacheHttpResponseHandler();
  }

  @Override
  protected ApacheHttpRequestBuilder createRequestBuilder(Config config) {
    String urlTemplate = config.getString(HttpConstants.URL_TEMPLATE);
    String verb = config.getString(HttpConstants.VERB);
    String contentType = config.getString(HttpConstants.CONTENT_TYPE);

    return new ApacheHttpRequestBuilder(urlTemplate, verb, contentType);
  }

  @Override
  protected void fillHttpOutputData(Schema httpOutputSchema, GenericRecord outputRecord, HttpUriRequest rawRequest,
      ResponseStatus status) throws IOException {

    ApacheHttpResponseStatus apacheStatus = (ApacheHttpResponseStatus) status;
    HttpRequestResponseRecord record = new HttpRequestResponseRecord();
    record.setRequestUrl(rawRequest.getURI().toASCIIString());
    record.setMethod(rawRequest.getMethod());
    record.setStatusCode(apacheStatus.getStatusCode());
    record.setContentType(apacheStatus.getContentType());
    record.setBody(apacheStatus.getContent() == null? null: ByteBuffer.wrap(apacheStatus.getContent()));
    outputRecord.put(HTTP_REQUEST_RESPONSE_FIELD, record);
  }
}
