package org.apache.gobblin.configuration;

import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;

/**
 * Represents an error category used to classify errors within the Gobblin framework.
 * Each category has a name and a priority, which can be used to determine the severity
 * or importance of the error. This class is primarily used for error categorisation and handling.
 */
@Getter
@Setter
public class ErrorCategory implements Serializable {
  private String categoryName;
  private int priority;

  public ErrorCategory(String categoryName, int priority) {
    this.categoryName = categoryName;
    this.priority = priority;
  }

}

