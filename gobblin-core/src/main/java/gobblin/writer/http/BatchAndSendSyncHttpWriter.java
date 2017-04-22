package gobblin.writer.http;

import java.io.IOException;
import java.util.Collection;

import com.google.common.collect.Sets;

import gobblin.http.BatchRequestBuilder;


/**
 * Synchronous HTTP writer which buffers and sends a batch at a time
 *
 * @param <D> type of record
 * @param <RQ> type of request
 * @param <RP> type of response
 */
public class BatchAndSendSyncHttpWriter<D, RQ, RP> extends HttpWriterBase<D, RQ, RP> {
  BatchRequestBuilder<D, RQ> batchRequestBuilder;
  protected final Collection<D> recordSet;
  protected final int maxBatchSize;
  protected long numRecordsWritten = 0L;

  public BatchAndSendSyncHttpWriter(BatchHttpWriterBaseBuilder builder) {
    super(builder);
    recordSet = createEmptyCollection();
    client = builder.getClient();
    batchRequestBuilder = builder.getBatchRequestBuilder();
    responseHandler = builder.getResponseHandler();
    maxBatchSize = builder.getMaxBatchSize();
  }

  protected Collection<D> createEmptyCollection() {
    return Sets.newHashSet();
  }

  /**
   * Process the record in batch
   * {@inheritDoc}
   */
  @Override
  public void writeImpl(D record) throws IOException {
    batchRecord(record);
    if (recordSet.size() < maxBatchSize) {
      return;
    }
    writeBatch(recordSet);
  }

  protected void batchRecord(D record) {
    recordSet.add(record);
  }

  private void writeBatch(Collection<D> records)
      throws IOException {
    RQ request = batchRequestBuilder.buildRequest(records);
    if (request != null) {
      RP response = client.sendRequest(request);
      responseHandler.handleResponse(response);
      numRecordsWritten += records.size();
      recordSet.clear();
    }
  }

  /**
   * Prior to commit, it will invoke flush method to flush any remaining item if writer uses batch
   * {@inheritDoc}
   * @see gobblin.instrumented.writer.InstrumentedDataWriterBase#commit()
   */
  @Override
  public void commit() throws IOException {
    flush();
    super.commit();
  }

  /**
   * Flush and send remaining items
   */
  private void flush() {
    try {
      writeBatch(recordSet);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
