package gobblin.writer.http;

import java.io.IOException;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.HttpClientBuilder;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.configuration.State;
import gobblin.converter.http.RestEntry;
import gobblin.http.HttpClientConfiguratorLoader;
import gobblin.http.SalesforceAuth;
import gobblin.http.SalesforceClient;
import gobblin.util.ConfigUtils;
import gobblin.writer.AsyncWriterManager;
import gobblin.writer.DataWriter;


/**
 * Builder of Salesforce http writer
 */
public class SalesforceWriterBuilder extends AsyncHttpWriterBuilder<RestEntry<JsonObject>, HttpUriRequest, CloseableHttpResponse> {
  static final String SFDC_PREFIX = "salesforce.";
  static final String OPERATION = SFDC_PREFIX + "operation";
  static final String BATCH_SIZE = SFDC_PREFIX + "batch_size";
  static final String BATCH_RESOURCE_PATH = SFDC_PREFIX + "batch_resource_path";

  public enum Operation {
    INSERT_ONLY_NOT_EXIST,
    UPSERT
  }

  private static final Config FALLBACK = ConfigFactory.parseMap(
      ImmutableMap.<String, String>builder()
          .put(BATCH_SIZE, "1")
          .build()
  );

  @Override
  public SalesforceWriterBuilder fromConfig(Config config) {
    config = config.withFallback(FALLBACK);
    Operation operation = Operation.valueOf(config.getString(OPERATION).toUpperCase());
    int maxRecordsInBatch = config.getInt(BATCH_SIZE);
    Optional<String> batchResourcePath = Optional.of(config.getString(BATCH_RESOURCE_PATH));

    HttpClientBuilder builder = createHttpClientBuilder(getState());
    SalesforceClient client = new SalesforceClient(createSalesforceAuth(config), builder, config);
    this.client = client;
    this.asyncRequestBuilder = new SalesforceRequestBuilder(client, operation, batchResourcePath, maxRecordsInBatch);
    responseHandler = new SalesforceResponseHandler(client, operation, maxRecordsInBatch > 1);
    return this;
  }

  @VisibleForTesting
  public HttpClientBuilder createHttpClientBuilder(State state) {
    HttpClientConfiguratorLoader clientConfiguratorLoader = new HttpClientConfiguratorLoader(state);
    clientConfiguratorLoader.getConfigurator().setStatePropertiesPrefix(AsyncHttpWriterBuilder.CONF_PREFIX);
    return clientConfiguratorLoader.getConfigurator().configure(getState()).getBuilder();
  }

  @VisibleForTesting
  public SalesforceAuth createSalesforceAuth(Config config) {
    return SalesforceAuth.create(config);
  }

  @Override
  public DataWriter<RestEntry<JsonObject>> build()
      throws IOException {
    validate();
      return AsyncWriterManager.builder()
          .config(ConfigUtils.propertiesToConfig(getState().getProperties()))
          .asyncDataWriter(new AsyncHttpWriter(this))
          .retriesEnabled(false) // retries are done in HttpBatchDispatcher
          .failureAllowanceRatio(0).build();
  }
}