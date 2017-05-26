package gobblin.writer;

import org.apache.avro.generic.GenericRecord;

import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.transport.common.Client;
import com.typesafe.config.Config;

import gobblin.restli.R2Client;
import gobblin.restli.R2RestRequestBuilder;
import gobblin.restli.R2RestResponseHandler;
import gobblin.utils.HttpConstants;
import gobblin.writer.http.AsyncHttpWriterBuilder;


public abstract class R2RestWriterBuilder extends AsyncHttpWriterBuilder<GenericRecord, RestRequest, RestResponse> {
  @Override
  public R2RestWriterBuilder fromConfig(Config config) {
    R2Client client = new R2Client(createRawClient(config));
    this.client = client;

    String urlTemplate = config.getString(HttpConstants.URL_TEMPLATE);
    String verb = config.getString(HttpConstants.VERB);
    String protocolVersion = config.getString(HttpConstants.PROTOCOL_VERSION);
    asyncRequestBuilder = new R2RestRequestBuilder(urlTemplate, verb, protocolVersion);

    responseHandler = new R2RestResponseHandler();

    return this;
  }

  public abstract Client createRawClient(Config config);
}
