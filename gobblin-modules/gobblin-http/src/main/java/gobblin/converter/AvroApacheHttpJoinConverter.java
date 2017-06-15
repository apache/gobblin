package gobblin.converter;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.HttpClientBuilder;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import gobblin.config.ConfigBuilder;
import gobblin.configuration.WorkUnitState;
import gobblin.http.ApacheHttpClient;
import gobblin.http.ApacheHttpResponseHandler;
import gobblin.http.ApacheHttpResponseStatus;
import gobblin.http.HttpClient;
import gobblin.http.ApacheHttpRequestBuilder;
import gobblin.http.HttpRequestResponseRecord;
import gobblin.http.ResponseStatus;
import gobblin.utils.HttpConstants;

/**
 * Apache version of http join converter
 */
@Slf4j
public class AvroApacheHttpJoinConverter extends AvroHttpJoinConverter<HttpUriRequest, CloseableHttpResponse> {
  @Override
  public HttpClient<HttpUriRequest, CloseableHttpResponse> createHttpClient(WorkUnitState workUnitState) {
    Config config = ConfigBuilder.create().loadProps(workUnitState.getProperties(), CONF_PREFIX).build();
    return new ApacheHttpClient(HttpClientBuilder.create(), config, workUnitState.getTaskBroker());
  }

  @Override
  public ApacheHttpResponseHandler createResponseHandler(WorkUnitState workUnit) {
    return new ApacheHttpResponseHandler();
  }

  @Override
  protected ApacheHttpRequestBuilder createRequestBuilder(WorkUnitState workUnitState) {

    Config config = ConfigBuilder.create().loadProps(workUnitState.getProperties(), CONF_PREFIX).build();
    config.withFallback(DEFAULT_FALLBACK);
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
