package gobblin.writer;

import org.apache.avro.generic.GenericRecord;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.HttpClientBuilder;

import com.typesafe.config.Config;

import gobblin.http.ApacheHttpClient;
import gobblin.http.HttpRequestBuilder;
import gobblin.http.HttpResponseHandler;
import gobblin.utils.HttpConstants;
import gobblin.writer.http.AsyncHttpWriterBuilder;


public class AvroHttpWriterBuilder extends AsyncHttpWriterBuilder<GenericRecord, HttpUriRequest, CloseableHttpResponse> {

  @Override
  public AvroHttpWriterBuilder fromConfig(Config config) {
    ApacheHttpClient client = new ApacheHttpClient(HttpClientBuilder.create(), config);
    this.client = client;

    String urlTemplate = config.getString(HttpConstants.URL_TEMPLATE);
    String verb = config.getString(HttpConstants.VERB);
    this.asyncRequestBuilder = new HttpRequestBuilder(urlTemplate, verb);
    this.responseHandler = new HttpResponseHandler();
    return this;
  }
}
