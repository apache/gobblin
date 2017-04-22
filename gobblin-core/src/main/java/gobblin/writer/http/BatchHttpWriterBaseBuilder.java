package gobblin.writer.http;

import com.google.common.base.Preconditions;

import lombok.Getter;

/**
 * Base builder for batch http writers
 *
 * @param <D> type of record
 * @param <RQ> type of request
 * @param <RP> type of response
 * @param <B> type of builder
 */
@Getter
public abstract class BatchHttpWriterBaseBuilder <D, RQ, RP, B extends BatchHttpWriterBaseBuilder<D, RQ, RP, B>>
    extends HttpWriterBaseBuilder<D, RQ, RP, B> {
  // No max batch size enforcement
  public static final int DEFAULT_MAX_BATCH_SIZE = 1000;
  protected int maxBatchSize = DEFAULT_MAX_BATCH_SIZE;

  protected void validate() {
    super.validate();
    Preconditions
        .checkNotNull(getMaxBatchSize(), "maxBatchSize is required for " + this.getClass().getSimpleName());
  }
}