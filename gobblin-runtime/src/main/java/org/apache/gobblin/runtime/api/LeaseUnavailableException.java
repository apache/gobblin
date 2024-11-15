package org.apache.gobblin.runtime.api;

public class LeaseUnavailableException extends RuntimeException {
  public LeaseUnavailableException(String message) {
    super(message);
  }
}
