package gobblin.converter;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import gobblin.async.AsyncRequestBuilder;
import gobblin.config.ConfigBuilder;
import gobblin.configuration.WorkUnitState;
import gobblin.http.HttpClient;
import gobblin.http.HttpRequestResponseRecord;
import gobblin.http.ResponseHandler;
import gobblin.http.ResponseStatus;
import gobblin.restli.R2ResponseStatus;
import gobblin.restli.R2RestRequestBuilder;
import gobblin.restli.R2RestResponseHandler;
import gobblin.utils.HttpConstants;


@Slf4j
public abstract class AvroD2JoinConverter extends AvroHttpJoinConverter<RestRequest, RestResponse>{
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
  protected HttpClient<RestRequest, RestResponse> createHttpClient(WorkUnitState workUnitState) {
    //TODO: should use R2ClientFactory to initilize R2Client
    return null;
  }

  @Override
  protected ResponseHandler<RestResponse> createResponseHandler(WorkUnitState workUnitState) {
    return new R2RestResponseHandler();
  }

  @Override
  protected AsyncRequestBuilder<GenericRecord, RestRequest> createRequestBuilder(WorkUnitState workUnitState) {
    Config config = ConfigBuilder.create().loadProps(workUnitState.getProperties(), CONF_PREFIX).build();
    config.withFallback(DEFAULT_FALLBACK);
    String urlTemplate = config.getString(HttpConstants.URL_TEMPLATE);
    String verb = config.getString(HttpConstants.VERB);
    String contentType = config.getString(HttpConstants.CONTENT_TYPE);

    return new R2RestRequestBuilder(urlTemplate, verb, contentType);
  }

}
