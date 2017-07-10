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
import gobblin.net.Request;
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
public abstract class AsyncHttpJoinConverter<SI, SO, DI, DO, RQ, RP> extends AsyncConverter1to1<SI, SO, DI, DO> {
  public static final String CONF_PREFIX = "gobblin.converter.http.";
  public static final Config DEFAULT_FALLBACK =
      ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
          .put(HttpConstants.CONTENT_TYPE, "application/json")
          .put(HttpConstants.VERB, "GET")
          .build());

  protected HttpClient<RQ, RP> httpClient = null;
  protected ResponseHandler<RQ, RP> responseHandler = null;
  protected AsyncRequestBuilder<GenericRecord, RQ> requestBuilder = null;

  public AsyncHttpJoinConverter init(WorkUnitState workUnitState) {
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
  protected abstract ResponseHandler<RQ, RP> createResponseHandler(Config config);
  protected abstract AsyncRequestBuilder<GenericRecord, RQ> createRequestBuilder(Config config);
  protected abstract HttpOperation generateHttpOperation (DI inputRecord, State state);
  protected abstract SO convertSchemaImpl (SI inputSchema, WorkUnitState workUnitState) throws SchemaConversionException;
  protected abstract DO convertRecordImpl (SO outputSchema, DI input, RQ rawRequest, ResponseStatus status) throws DataConversionException;

  /**
   * A helper class which performs the conversion from http response to DO type output, saved as a {@link CompletableFuture}
   */
  private class AsyncHttpJoinConverterContext<SO, DI, DO, RP, RQ> {
    private final CompletableFuture<DO> future;
    private final AsyncHttpJoinConverter<SI, SO, DI, DO, RQ, RP> converter;

    @Getter
    private final Callback<RP> callback;

    public AsyncHttpJoinConverterContext(AsyncHttpJoinConverter converter, SO outputSchema, DI input, Request<RQ> request) {
      this.future = new CompletableFuture();
      this.converter = converter;
      this.callback = new Callback<RP>() {
        @Override
        public void onSuccess(RP result) {
          try {
            ResponseStatus status = AsyncHttpJoinConverterContext.this.converter.responseHandler.handleResponse(request, result);
            switch (status.getType()) {
              case OK:
                AsyncHttpJoinConverterContext.this.onSuccess(request.getRawRequest(), status, outputSchema, input);
                break;
              case CLIENT_ERROR:
                AsyncHttpJoinConverterContext.this.onSuccess(request.getRawRequest(), status, outputSchema, input);
                break;
              case SERVER_ERROR:
                // Server side error. Retry
                throw new DataConversionException(request.getRawRequest() + " send failed due to server error");
              default:
                throw new DataConversionException(request.getRawRequest() + " Should not reach here");
            }
          } catch (Exception e) {
            AsyncHttpJoinConverterContext.this.future.completeExceptionally(e);
          }
        }

        @Override
        public void onFailure(Throwable throwable) {
          AsyncHttpJoinConverterContext.this.future.completeExceptionally(throwable);
        }
      };
    }

    private void onSuccess(RQ rawRequest, ResponseStatus status, SO outputSchema, DI input) throws DataConversionException {
      log.debug("{} send with status type {}", rawRequest, status.getType());
      DO output = this.converter.convertRecordImpl(outputSchema, input, rawRequest, status);
      AsyncHttpJoinConverterContext.this.future.complete(output);
    }
  }

  /**
   * Convert an input record to a future object where an output record will be filled in sometime later
   * Sequence:
   *    Convert input (DI) to an http request
   *    Send http request asynchronously, and registers an http callback
   *    Create an {@link CompletableFuture} object. When the callback is invoked, this future object is filled in by an output record which is converted from http response.
   *    Return the future object.
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
    AsyncHttpJoinConverterContext context = new AsyncHttpJoinConverterContext(this, outputSchema, inputRecord, request);

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
