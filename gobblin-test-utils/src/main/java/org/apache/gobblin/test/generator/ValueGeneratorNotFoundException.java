package org.apache.gobblin.test.generator;

public class ValueGeneratorNotFoundException extends Exception {

  public ValueGeneratorNotFoundException(String message, Throwable t) {
    super(message, t);
  }

  public ValueGeneratorNotFoundException(String message) {
    super(message);
  }

}
