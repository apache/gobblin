package org.apache.gobblin.metastore;

import org.apache.gobblin.configuration.ErrorIssue;
import org.apache.gobblin.configuration.Category;

import java.io.IOException;
import java.util.List;


/**
 * Interface for a store that persists Issues and Categories, similar to StateStore.
 */
public interface ErrorPatternStore {
  void addErrorPattern(ErrorIssue issue)
      throws IOException;

  boolean deleteErrorPattern(String descriptionRegex)
      throws IOException;

  ErrorIssue getErrorPattern(String descriptionRegex)
      throws IOException;

  List<ErrorIssue> getAllErrorPatterns()
      throws IOException;

  List<ErrorIssue> getErrorPatternsByCategory(String categoryName)
      throws IOException;

  void addErrorCategory(Category category)
      throws IOException;

  Category getErrorCategory(String categoryName)
      throws IOException;

  int getErrorCategoryPriority(String categoryName)
      throws IOException;

  List<Category> getAllErrorCategories()
      throws IOException;

  List<ErrorIssue> getAllErrorIssuesOrderedByCategoryPriority()
      throws IOException;

  /**
   * Returns the default error category. Implementation should define what is considered default. Should return null if no default is set.
   */
  Category getDefaultCategory()
      throws IOException;
}
