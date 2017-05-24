package gobblin.writer;

import org.apache.avro.generic.GenericRecord;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClientBuilder;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.http.ApacheHttpClient;
import gobblin.http.HttpRequestBuilder;
import gobblin.http.HttpResponseHandler;
import gobblin.utils.HttpConstants;
import gobblin.writer.http.AsyncHttpWriterBuilder;


public class AvroHttpWriterBuilder extends AsyncHttpWriterBuilder<GenericRecord, HttpUriRequest, CloseableHttpResponse> {
  private static final Config FALLBACK =
      ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
          .put(HttpConstants.CONTENT_TYPE, "application/json")
          .build());

  @Override
  public AvroHttpWriterBuilder fromConfig(Config config) {
    config = config.withFallback(FALLBACK);
    ApacheHttpClient client = new ApacheHttpClient(HttpClientBuilder.create(), config);
    this.client = client;

    String urlTemplate = config.getString(HttpConstants.URL_TEMPLATE);
    String verb = config.getString(HttpConstants.VERB);
    String contentType = config.getString(HttpConstants.CONTENT_TYPE);
    this.asyncRequestBuilder = new HttpRequestBuilder(urlTemplate, verb, contentType);
    this.responseHandler = new HttpResponseHandler();
    return this;
  }
}
