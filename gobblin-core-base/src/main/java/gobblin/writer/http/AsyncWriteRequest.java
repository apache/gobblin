package gobblin.writer.http;

import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.Setter;

import gobblin.writer.WriteCallback;
import gobblin.writer.WriteResponse;


public class AsyncWriteRequest<D, RQ> {
  @Getter
  private int recordCount = 0;
  @Getter
  protected long bytesWritten = 0;
  @Getter @Setter
  private RQ rawRequest;

  private final List<Thunk> thunks = new ArrayList<>();

  /**
   * Callback on sending the batch successfully
   */
  public void onSuccess(final WriteResponse response) {
    for (final Thunk thunk : this.thunks) {
      thunk.callback.onSuccess(new WriteResponse() {
        @Override
        public Object getRawResponse() {
          return response.getRawResponse();
        }

        @Override
        public String getStringResponse() {
          return response.getStringResponse();
        }

        @Override
        public long bytesWritten() {
          return thunk.sizeInBytes;
        }
      });
    }
  }

  /**
   * Callback on failing to send the batch
   */
  public void onFailure(Throwable throwable) {
    for (Thunk thunk : this.thunks) {
      thunk.callback.onFailure(throwable);
    }
  }

  /**
   * Mark the record associated with this request
   */
  public void markRecord(BufferedRecord<D> record, int bytesWritten) {
    if (record.getCallback() != null) {
      thunks.add(new Thunk(record.getCallback(), bytesWritten));
    }
    recordCount++;
    this.bytesWritten += bytesWritten;
  }

  /**
   * A helper class which wraps the callback
   * It may contain more information related to each individual record
   */
  final private static class Thunk {
    final WriteCallback callback;
    final int sizeInBytes;

    Thunk(WriteCallback callback, int sizeInBytes) {
      this.callback = callback;
      this.sizeInBytes = sizeInBytes;
    }
  }
}
