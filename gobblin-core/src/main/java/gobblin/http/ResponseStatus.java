package gobblin.http;

import lombok.Getter;
import lombok.Setter;


/**
 * This class represents a result of handling a response
 */
public class ResponseStatus {
  @Getter @Setter
  StatusType type;

  public ResponseStatus(StatusType type) {
    this.type = type;
  }
}
