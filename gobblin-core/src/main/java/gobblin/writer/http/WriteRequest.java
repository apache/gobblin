package gobblin.writer.http;

import lombok.Getter;
import lombok.Setter;


/**
 * A type of write request which encodes a record at a time. It encapsulates the raw request and
 * some statistics about the request
 *
 * @param <RQ> type of raw request
 */
public class WriteRequest<RQ> {
  @Getter
  protected long bytesWritten = 0;
  @Getter @Setter
  private RQ rawRequest;

  public void markRecord(int bytesWritten) {
    this.bytesWritten = bytesWritten;
  }
}
