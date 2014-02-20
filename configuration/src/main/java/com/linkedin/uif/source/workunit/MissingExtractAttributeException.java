package com.linkedin.uif.source.workunit;

public class MissingExtractAttributeException extends Exception {

  /**
   * Thrown if a required attributes hasn't been set for an extract.
   */
  private static final long serialVersionUID = -8835394514140834103L;

  public MissingExtractAttributeException(String arg0) {
    super(arg0);
  }

}
