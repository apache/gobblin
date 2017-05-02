package gobblin.http;

import lombok.Getter;


/**
 * This class represents a result of handling a response
 */
public class ResponseStatus {
  @Getter
  StatusType type;

  ResponseStatus(StatusType type) {
    this.type = type;
  }
}
