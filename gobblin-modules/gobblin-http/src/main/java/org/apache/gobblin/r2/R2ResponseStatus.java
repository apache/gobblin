package org.apache.gobblin.r2;

import com.linkedin.data.ByteString;

import lombok.Getter;
import lombok.Setter;

import org.apache.gobblin.http.ResponseStatus;
import org.apache.gobblin.http.StatusType;

@Getter @Setter
public class R2ResponseStatus extends ResponseStatus {
  private int statusCode;
  private ByteString content = null;
  private String contentType = null;
  public R2ResponseStatus(StatusType type) {
    super(type);
  }
}
