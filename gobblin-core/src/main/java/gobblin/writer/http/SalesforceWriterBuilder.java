package gobblin.writer.http;

import java.io.IOException;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.Getter;

import gobblin.configuration.State;
import gobblin.converter.http.RestEntry;
import gobblin.writer.DataWriter;


/**
 * Builder of Salesforce http writer
 */
public class SalesforceWriterBuilder extends BatchHttpWriterBaseBuilder<RestEntry<JsonObject>, HttpUriRequest, CloseableHttpResponse, SalesforceWriterBuilder> {
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

  @Getter
  private Operation operation;
  @Getter
  private Optional<String> batchResourcePath;

  @Override
  SalesforceWriterBuilder fromConfig(Config config) {
    operation = Operation.valueOf(config.getString(OPERATION).toUpperCase());
    maxBatchSize = config.getInt(BATCH_SIZE);
    batchResourcePath = Optional.of(config.getString(BATCH_RESOURCE_PATH));

    return super.fromConfig(config);
  }

  @Override
  protected void createComponents(State state, Config config) {
    SalesforceClient client = new SalesforceClient(state, config);
    requestBuilder = new SalesforceRequestBuilder(client, operation, batchResourcePath);
    responseHandler = new SalesforceResponseHandler(client, operation, true);
    this.client = client;
  }

  @Override
  public DataWriter<RestEntry<JsonObject>> build()
      throws IOException {
    validate();
    if (maxBatchSize > 1) {
      return new BatchAndSendSyncHttpWriter<RestEntry<JsonObject>, HttpUriRequest, CloseableHttpResponse>(this);
    } else {
      return new RecordSyncHttpWriter<RestEntry<JsonObject>, HttpUriRequest, CloseableHttpResponse>(this);
    }
  }
}