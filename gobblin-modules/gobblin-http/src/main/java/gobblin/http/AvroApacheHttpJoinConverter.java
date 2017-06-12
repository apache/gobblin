package gobblin.http;

import java.net.URI;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import gobblin.async.BufferedRecord;
import gobblin.config.ConfigBuilder;
import gobblin.configuration.WorkUnitState;
import gobblin.utils.HttpConstants;
import gobblin.utils.HttpUtils;


/**
 * Apache version of http join converter
 */
@Slf4j
public class AvroApacheHttpJoinConverter extends AvroHttpJoinConverter<HttpUriRequest, CloseableHttpResponse> {
  @Override
  public HttpClient<HttpUriRequest, CloseableHttpResponse> createHttpClient(WorkUnitState workUnit)
      throws IllegalStateException {
    Config config = ConfigBuilder.create().loadProps(workUnit.getProperties(), CONF_PREFIX).build();
    return new ApacheHttpClient(HttpClientBuilder.create(), config, workUnit.getTaskBroker());
  }

  @Override
  public ResponseHandler<CloseableHttpResponse> createResponseHandler(WorkUnitState workUnit)
      throws IllegalStateException {
    return new HttpResponseHandler();
  }

  @Override
  protected ConverterRequestBuilder<HttpUriRequest> createRequestBuilder(WorkUnitState workUnit)
      throws IllegalStateException {
    return new ConverterApacheHttpRequestBuilder(workUnit);
  }

  @Override
  public void fillHttpOutputData(Schema httpOutputSchema, GenericRecord outputRecord, HttpUriRequest rawRequest,
      CloseableHttpResponse response, ResponseStatus status) {
    // TODO: add response to the output record
    GenericRecord httpOutput = new GenericData.Record(httpOutputSchema);

    httpOutput.put("urlRequest", null);

    outputRecord.put("httpOutput", httpOutput);
  }

  public static class ConverterApacheHttpRequestBuilder extends ConverterRequestBuilder<HttpUriRequest> {
    private final WorkUnitState workUnit;

    public ConverterApacheHttpRequestBuilder (WorkUnitState workUnitState) {
      this.workUnit = workUnitState;
    }


    @Override
    public HttpUriRequest buildHttpRequest(BufferedRecord<HttpOperation> bufferedRecord) {
      Config config = ConfigBuilder.create().loadProps(workUnit.getProperties(), CONF_PREFIX).build();
      HttpOperation httpOperation = bufferedRecord.getRecord();

      String urlTemplate = config.getString(HttpConstants.URL_TEMPLATE);
      String verb = config.getString(HttpConstants.VERB);
      String contentType = config.getString(HttpConstants.CONTENT_TYPE);


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
      String payload = httpOperation.getBody();
      if (payload == null || payload.length() == 0) {
        return builder.build();
      }

      builder.setHeader(HttpHeaders.CONTENT_TYPE, HttpRequestBuilder.createContentType(contentType).getMimeType());
      builder.setEntity(new StringEntity(payload, contentType));

      return builder.build();
    }

  }
}
