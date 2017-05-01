package gobblin.http;

import lombok.Getter;


public class ResponseStatus {
  @Getter
  int statusCode;

  ResponseStatus(int statusCode) {
    this.statusCode = statusCode;
  }
}
