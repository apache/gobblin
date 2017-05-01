package gobblin.writer.http;

import com.google.common.base.Preconditions;

import lombok.Getter;


public abstract class AsyncHttpWriterBaseBuilder<D, RQ, RP> extends HttpWriterBaseBuilder<D, RQ, RP> {
  @Getter
  protected AsyncWriteRequestBuilder<D, RQ> asyncRequestBuilder;
  @Getter
  protected int queueCapacity = AbstractAsyncDataWriter.DEFAULT_BUFFER_CAPACITY;
  @Getter
  protected int maxTries = AsyncHttpWriter.DEFAULT_MAX_TRIES;

  @Override
  protected void validate() {
    Preconditions.checkNotNull(getState(), "State is required for " + this.getClass().getSimpleName());
    Preconditions.checkNotNull(getClient(), "Client is required for " + this.getClass().getSimpleName());
    Preconditions.checkNotNull(getAsyncRequestBuilder(),
        "AsyncWriteRequestBuilder is required for " + this.getClass().getSimpleName());
    Preconditions
        .checkNotNull(getResponseHandler(), "ResponseHandler is required for " + this.getClass().getSimpleName());
  }
}
