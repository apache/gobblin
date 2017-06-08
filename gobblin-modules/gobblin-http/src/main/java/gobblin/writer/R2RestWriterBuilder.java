package gobblin.writer;

import org.apache.avro.generic.GenericRecord;

import com.google.common.collect.ImmutableMap;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.restli.R2Client;
import gobblin.restli.R2RestRequestBuilder;
import gobblin.restli.R2RestResponseHandler;
import gobblin.utils.HttpConstants;


public abstract class R2RestWriterBuilder extends AsyncHttpWriterBuilder<GenericRecord, RestRequest, RestResponse> {
  private static final Config FALLBACK =
      ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
          .put(HttpConstants.PROTOCOL_VERSION, "2.0.0")
          .build());

  @Override
  public R2RestWriterBuilder fromConfig(Config config) {
    config = config.withFallback(FALLBACK);
    this.client = createClient(config);

    String urlTemplate = config.getString(HttpConstants.URL_TEMPLATE);
    String verb = config.getString(HttpConstants.VERB);
    String protocolVersion = config.getString(HttpConstants.PROTOCOL_VERSION);
    asyncRequestBuilder = new R2RestRequestBuilder(urlTemplate, verb, protocolVersion);

    responseHandler = new R2RestResponseHandler();

    return this;
  }

  protected abstract R2Client createClient(Config config);

}

