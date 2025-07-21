package org.apache.gobblin.configuration;

import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;

/**
 * Represents a profile for error patterns, containing a regex to match error descriptions and a category name for classification.
 */
@Getter
@Setter
public class ErrorPatternProfile implements Serializable {
  private String descriptionRegex;
  private String categoryName;

  public ErrorPatternProfile(String descriptionRegex, String categoryName) {
    this.descriptionRegex = descriptionRegex;
    this.categoryName = categoryName;
  }
}
