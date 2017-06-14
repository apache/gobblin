package gobblin.converter;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import gobblin.config.ConfigBuilder;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.AvroHttpJoinConverter;
import gobblin.http.ApacheHttpClient;
import gobblin.http.HttpClient;
import gobblin.http.HttpRequestBuilder;
import gobblin.http.HttpRequestResponseRecord;
import gobblin.http.HttpResponseHandler;
import gobblin.http.ResponseHandler;
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
  public ResponseHandler<CloseableHttpResponse> createResponseHandler(WorkUnitState workUnit) {
    return new HttpResponseHandler();
  }

  @Override
  protected HttpRequestBuilder createRequestBuilder(WorkUnitState workUnitState) {

    Config config = ConfigBuilder.create().loadProps(workUnitState.getProperties(), CONF_PREFIX).build();
    String urlTemplate = config.getString(HttpConstants.URL_TEMPLATE);
    String verb = config.getString(HttpConstants.VERB);
    String contentType = config.getString(HttpConstants.CONTENT_TYPE);

    return new HttpRequestBuilder(urlTemplate, verb, contentType);
  }

  @Override
  public void fillHttpOutputData(Schema httpOutputSchema, GenericRecord outputRecord, HttpUriRequest rawRequest,
      CloseableHttpResponse response) throws IOException {

    HttpRequestResponseRecord record = new HttpRequestResponseRecord();

    record.setRequestUrl(rawRequest.getURI().toASCIIString());
    record.setMethod(rawRequest.getMethod());
    record.setStatusCode(response.getStatusLine().getStatusCode());
    record.setContentType(response.getEntity().getContentType().getValue());
    record.setBody(ByteBuffer.wrap(EntityUtils.toByteArray(response.getEntity())));
    outputRecord.put(HTTP_REQUEST_RESPONSE, record);
  }
}
