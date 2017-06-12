package gobblin.http;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

import lombok.extern.slf4j.Slf4j;

import gobblin.async.AsyncRequest;
import gobblin.async.AsyncRequestBuilder;
import gobblin.async.BufferedRecord;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;
import gobblin.writer.WriteCallback;

/**
 * This converter converts an input record (DI) to an output record (DO) which
 * contains original input data and http response.
 *
 * Sequence:
 * Convert DI to HttpOperation
 * Convert HttpOperation to RQ (by internal ConverterRequestBuilder)
 * Execute http request, get response RP (by HttpClient)
 * Combine info (DI, RQ, RP, status, etc..) to generate output DO
 */
@Slf4j
public abstract class HttpJoinConverter<SI, SO, DI, DO, RQ, RP> extends Converter<SI, SO, DI, DO> {
  public static final String CONF_PREFIX = "gobblin.converter.http.";

  protected HttpClient<RQ, RP> httpClient = null;
  protected ResponseHandler<RP> responseHandler = null;
  protected AsyncRequestBuilder<HttpOperation, RQ> requestBuilder = null;

  @Override
  public final SO convertSchema(SI inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    httpClient = createHttpClient(workUnit);
    responseHandler = createResponseHandler(workUnit);
    requestBuilder = createRequestBuilder(workUnit);
    return convertSchemaImpl(inputSchema, workUnit);
  }

  protected abstract HttpClient<RQ, RP>   createHttpClient(WorkUnitState workUnit) throws IllegalStateException;
  protected abstract ResponseHandler<RP>  createResponseHandler(WorkUnitState workUnit) throws IllegalStateException;
  protected abstract ConverterRequestBuilder<RQ> createRequestBuilder(WorkUnitState workUnit) throws IllegalStateException;
  protected abstract HttpOperation generateHttpOperation (DI inputRecord, WorkUnitState state);
  protected abstract SO convertSchemaImpl (SI inputSchema, WorkUnitState workUnit) throws SchemaConversionException;
  protected abstract DO convertResponse (SO outputSchema, DI input, RQ rawRequest, RP response, ResponseStatus status);

  @Override
  public final Iterable<DO> convertRecord(SO outputSchema, DI inputRecord, WorkUnitState workUnit)
      throws DataConversionException {

    // Convert DI to HttpOperation
    HttpOperation operation = generateHttpOperation(inputRecord, workUnit);
    BufferedRecord<HttpOperation> bufferedRecord = new BufferedRecord<>(operation, WriteCallback.EMPTY);

    // Convert HttpOperation to RQ
    Queue<BufferedRecord<HttpOperation>> buffer = new LinkedBlockingDeque<>();
    buffer.add(bufferedRecord);
    AsyncRequest<HttpOperation, RQ> request = this.requestBuilder.buildRequest(buffer);
    RQ rawRequest = request.getRawRequest();

    // Execute query and get response
    RP response;
    try {
      response = httpClient.sendRequest(rawRequest);
    } catch (IOException e) {
      throw new RuntimeException();
    }

    ResponseStatus status = responseHandler.handleResponse(response);

    switch (status.getType()) {
      case OK:
        // Write succeeds
        log.info ("{} send successfully", rawRequest);
        break;
      case CLIENT_ERROR:
        // Client error. Fail!
        throw new DataConversionException(rawRequest + " send failed due to client error");
      case SERVER_ERROR:
        // Server side error. Retry
        throw new DataConversionException(rawRequest + " send failed due to server error");
    }

    // Combine info (DI, RQ, RP, status, etc..) to generate output DO
    DO output = convertResponse (outputSchema, inputRecord, rawRequest, response, status);

    return new SingleRecordIterable<>(output);
  }

  public static abstract class ConverterRequestBuilder<RQ> implements AsyncRequestBuilder<HttpOperation, RQ> {

    @Override
    public final AsyncRequest<HttpOperation, RQ> buildRequest(Queue<BufferedRecord<HttpOperation>> buffer) {
      AsyncRequest<HttpOperation, RQ> request = new AsyncRequest<>();
      RQ rawRequest = buildHttpRequest (buffer.poll());
      request.setRawRequest(rawRequest);
      return request;
    }

    public abstract RQ buildHttpRequest (BufferedRecord<HttpOperation> bufferedRecord);
  }
}
