package gobblin.converter;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.transport.common.Client;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import gobblin.async.AsyncRequestBuilder;
import gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.http.HttpClient;
import gobblin.http.HttpRequestResponseRecord;
import gobblin.http.ResponseHandler;
import gobblin.http.ResponseStatus;
import gobblin.r2.R2ClientFactory;
import gobblin.r2.R2Client;
import gobblin.r2.R2ResponseStatus;
import gobblin.r2.R2RestRequestBuilder;
import gobblin.r2.R2RestResponseHandler;
import gobblin.utils.HttpConstants;


@Slf4j
public class AvroR2JoinConverter extends AvroHttpJoinConverter<RestRequest, RestResponse>{

  @Override
  protected void fillHttpOutputData(Schema schema, GenericRecord outputRecord, RestRequest restRequest,
      ResponseStatus status)
      throws IOException {
    R2ResponseStatus r2ResponseStatus = (R2ResponseStatus) status;
    HttpRequestResponseRecord record = new HttpRequestResponseRecord();
    record.setRequestUrl(restRequest.getURI().toASCIIString());
    record.setMethod(restRequest.getMethod());
    record.setStatusCode(r2ResponseStatus.getStatusCode());
    record.setContentType(r2ResponseStatus.getContentType());
    record.setBody(r2ResponseStatus.getContent() == null? null: r2ResponseStatus.getContent().asByteBuffer());
    outputRecord.put("HttpRequestResponse", record);
  }

  @Override
  protected HttpClient<RestRequest, RestResponse> createHttpClient(Config config, SharedResourcesBroker<GobblinScopeTypes> broker) {
    String urlTemplate = config.getString(HttpConstants.URL_TEMPLATE);

    // By default, use http schema
    R2ClientFactory.Schema schema = R2ClientFactory.Schema.HTTP;
    if (urlTemplate.startsWith(HttpConstants.SCHEMA_D2)) {
      schema = R2ClientFactory.Schema.D2;
    }

    R2ClientFactory factory = new R2ClientFactory(schema);
    Client client = factory.createInstance(config);
    return new R2Client(client, config, broker);
  }

  @Override
  protected ResponseHandler<RestRequest, RestResponse> createResponseHandler(Config config) {
    return new R2RestResponseHandler();
  }

  @Override
  protected AsyncRequestBuilder<GenericRecord, RestRequest> createRequestBuilder(Config config) {
    String urlTemplate = config.getString(HttpConstants.URL_TEMPLATE);
    String verb = config.getString(HttpConstants.VERB);
    String contentType = config.getString(HttpConstants.CONTENT_TYPE);

    return new R2RestRequestBuilder(urlTemplate, verb, contentType);
  }

}
