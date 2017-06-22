package gobblin.converter;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.avro.generic.GenericRecord;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import gobblin.async.AsyncRequest;
import gobblin.async.AsyncRequestBuilder;
import gobblin.async.BufferedRecord;
import gobblin.async.Callback;
import gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.config.ConfigBuilder;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.http.HttpClient;
import gobblin.http.HttpOperation;
import gobblin.http.ResponseHandler;
import gobblin.http.ResponseStatus;
import gobblin.utils.HttpConstants;
import gobblin.writer.WriteCallback;

/**
 * This converter converts an input record (DI) to an output record (DO) which
 * contains original input data and http request & response info.
 *
 * Sequence:
 * Convert DI to HttpOperation
 * Convert HttpOperation to RQ (by internal AsyncRequestBuilder)
 * Execute http request, get response RP (by HttpClient)
 * Combine info (DI, RQ, RP, status, etc..) to generate output DO
 */
@Slf4j
public abstract class HttpJoinConverter<SI, SO, DI, DO, RQ, RP> extends AsyncConverter1to1<SI, SO, DI, DO> {
  public static final String CONF_PREFIX = "gobblin.converter.http.";
  public static final Config DEFAULT_FALLBACK =
      ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
          .put(HttpConstants.CONTENT_TYPE, "application/json")
          .put(HttpConstants.VERB, "GET")
          .build());

  protected HttpClient<RQ, RP> httpClient = null;
  protected ResponseHandler<RP> responseHandler = null;
  protected AsyncRequestBuilder<GenericRecord, RQ> requestBuilder = null;

  public HttpJoinConverter init(WorkUnitState workUnitState) {
    super.init(workUnitState);
    Config config = ConfigBuilder.create().loadProps(workUnitState.getProperties(), CONF_PREFIX).build();
    config = config.withFallback(DEFAULT_FALLBACK);

    httpClient = createHttpClient(config, workUnitState.getTaskBroker());
    responseHandler = createResponseHandler(config);
    requestBuilder = createRequestBuilder(config);
    return this;
  }

  @Override
  public final SO convertSchema(SI inputSchema, WorkUnitState workUnitState)
      throws SchemaConversionException {
    return convertSchemaImpl(inputSchema, workUnitState);
  }

  protected abstract HttpClient<RQ, RP>   createHttpClient(Config config, SharedResourcesBroker<GobblinScopeTypes> broker);
  protected abstract ResponseHandler<RP> createResponseHandler(Config config);
  protected abstract AsyncRequestBuilder<GenericRecord, RQ> createRequestBuilder(Config config);
  protected abstract HttpOperation generateHttpOperation (DI inputRecord, State state);
  protected abstract SO convertSchemaImpl (SI inputSchema, WorkUnitState workUnitState) throws SchemaConversionException;
  protected abstract DO convertRecordImpl (SO outputSchema, DI input, RQ rawRequest, ResponseStatus status) throws DataConversionException;

  private class HttpJoinConverterContext<SO, DI, DO, RP, RQ> {
    private final CompletableFuture<DO> future;
    private final HttpJoinConverter<SI, SO, DI, DO, RQ, RP> converter;

    @Getter
    private final Callback<RP> callback;

    public HttpJoinConverterContext(HttpJoinConverter converter, SO outputSchema, DI input, RQ rawRequest) {
      this.future = new CompletableFuture();
      this.converter = converter;
      this.callback = new Callback<RP>() {
        @Override
        public void onSuccess(RP result) {
          try {
            ResponseStatus status = HttpJoinConverterContext.this.converter.responseHandler.handleResponse(result);
            switch (status.getType()) {
              case OK:
              case CLIENT_ERROR:
                // Convert (DI, RQ, RP etc..) to output DO
                log.debug("{} send with status type {}", rawRequest, status.getType());
                DO output = HttpJoinConverterContext.this.converter.convertRecordImpl(outputSchema, input, rawRequest, status);
                HttpJoinConverterContext.this.future.complete(output);
              case SERVER_ERROR:
                // Server side error. Retry
                throw new DataConversionException(rawRequest + " send failed due to server error");
              default:
                throw new DataConversionException(rawRequest + " Should not reach here");
            }
          } catch (Exception e) {
            HttpJoinConverterContext.this.future.completeExceptionally(e);
          }
        }

        @Override
        public void onFailure(Throwable throwable) {
          HttpJoinConverterContext.this.future.completeExceptionally(throwable);
        }
      };
    }
  }

  /**
   * Convert input (DI) to an http request, get http response, and convert it to output (DO) wrapped in a {@link CompletableFuture} object.
   */
  @Override
  public final CompletableFuture<DO> convertRecordAsync(SO outputSchema, DI inputRecord, WorkUnitState workUnitState)
      throws DataConversionException {

    // Convert DI to HttpOperation
    HttpOperation operation = generateHttpOperation(inputRecord, workUnitState);
    BufferedRecord<GenericRecord> bufferedRecord = new BufferedRecord<>(operation, WriteCallback.EMPTY);

    // Convert HttpOperation to RQ
    Queue<BufferedRecord<GenericRecord>> buffer = new LinkedBlockingDeque<>();
    buffer.add(bufferedRecord);
    AsyncRequest<GenericRecord, RQ> request = this.requestBuilder.buildRequest(buffer);
    RQ rawRequest = request.getRawRequest();

    // Execute query and get response
    HttpJoinConverterContext context = new HttpJoinConverterContext(this, outputSchema, inputRecord, rawRequest);

    try {
      httpClient.sendAsyncRequest(rawRequest, context.getCallback());
    } catch (IOException e) {
      throw new DataConversionException(e);
    }

    return context.future;
  }

  public void close() throws IOException {
    this.httpClient.close();
  }
}
