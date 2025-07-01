package org.apache.gobblin.metastore;

import org.apache.gobblin.configuration.ErrorIssue;
import org.apache.gobblin.configuration.Category;

import java.io.IOException;
import java.util.List;


/**
 * Interface for a store that persists Issues and Categories, similar to StateStore.
 */
public interface ErrorIssueStore {
  void addErrorIssue(ErrorIssue issue)
      throws IOException;

  boolean deleteErrorIssue(String descriptionRegex)
      throws IOException;

  ErrorIssue getErrorIssue(String descriptionRegex)
      throws IOException;

  List<ErrorIssue> getAllErrorIssues()
      throws IOException;

  List<ErrorIssue> getErrorIssuesByCategory(String categoryName)
      throws IOException;

  void addErrorCategory(Category category)
      throws IOException;

  Category getErrorCategory(String categoryName)
      throws IOException;

  int getErrorCategoryPriority(String categoryName)
      throws IOException;

  List<Category> getAllErrorCategories()
      throws IOException;

  /**
   * Returns the default error category. Implementation should define what is considered default. Should return null if no default is set.
   */
  Category getDefaultCategory()
      throws IOException;
}
