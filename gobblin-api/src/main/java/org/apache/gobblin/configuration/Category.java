package org.apache.gobblin.configuration;

import java.io.Serializable;

/**
 * POJO representing a Category, similar to Spec or State.
 */
public class Category implements Serializable {
  private String categoryName;
  private int priority;

  public Category(String categoryName, int priority) {
    this.categoryName = categoryName;
    this.priority = priority;
  }

  public String getCategoryName() { return categoryName; }
  public void setCategoryName(String categoryName) { this.categoryName = categoryName; }
  public int getPriority() { return priority; }
  public void setPriority(int priority) { this.priority = priority; }
}

