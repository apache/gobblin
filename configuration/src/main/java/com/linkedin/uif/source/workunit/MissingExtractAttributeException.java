package com.linkedin.uif.source.workunit;

public class MissingExtractAttributeException extends Exception {

  /**
   * Thrown if a required attributes hasn't been set for an extract.
   *
   * @author kgoodhop
   */
  public MissingExtractAttributeException(String arg0) {
    super(arg0);
  }

}
