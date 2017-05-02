package gobblin.http;

import lombok.Getter;


/**
 * This class represents a result of handling a response
 */
public class ResponseStatus {
  @Getter
  int statusCode;

  ResponseStatus(int statusCode) {
    this.statusCode = statusCode;
  }
}
