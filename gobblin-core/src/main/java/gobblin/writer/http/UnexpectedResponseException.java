package gobblin.writer.http;

import java.io.IOException;

/** Denotes that the HTTP Writer observed an unexpected response.*/
public class UnexpectedResponseException extends IOException {
  private static final long serialVersionUID = 1L;

  public UnexpectedResponseException() {
    super();
  }

  public UnexpectedResponseException(String message) {
    super(message);
  }

  public UnexpectedResponseException(Throwable cause) {
    super(cause);
  }

  public UnexpectedResponseException(String message, Throwable cause) {
    super(message, cause);
  }

}
