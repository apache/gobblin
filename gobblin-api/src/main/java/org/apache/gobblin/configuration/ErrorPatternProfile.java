package org.apache.gobblin.configuration;

import java.io.Serializable;


/**
 * POJO representing an ErrorPatternProfile, similar to Spec or State.
 */
public class ErrorPatternProfile implements Serializable {
  private String descriptionRegex;
  private String categoryName;

  public ErrorPatternProfile(String descriptionRegex, String categoryName) {
    this.descriptionRegex = descriptionRegex;
    this.categoryName = categoryName;
  }

  public String getDescriptionRegex() {
    return descriptionRegex;
  }

  public void setDescriptionRegex(String descriptionRegex) {
    this.descriptionRegex = descriptionRegex;
  }

  public String getCategoryName() {
    return categoryName;
  }

  public void setCategoryName(String categoryName) {
    this.categoryName = categoryName;
  }

}
