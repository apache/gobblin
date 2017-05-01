package gobblin.writer.http;

import lombok.Getter;
import lombok.Setter;


public class WriteRequest<RQ> {
  @Getter
  protected long bytesWritten = 0;
  @Getter @Setter
  private RQ rawRequest;

  public void markRecord(int bytesWritten) {
    this.bytesWritten = bytesWritten;
  }
}
