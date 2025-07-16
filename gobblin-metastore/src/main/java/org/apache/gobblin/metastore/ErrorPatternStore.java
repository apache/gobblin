package org.apache.gobblin.metastore;

import org.apache.gobblin.configuration.ErrorPatternProfile;
import org.apache.gobblin.configuration.Category;

import java.io.IOException;
import java.util.List;


/**
 * Interface for a store that persists Errors and Categories.
 */
public interface ErrorPatternStore {
  void addErrorPattern(ErrorPatternProfile issue)
      throws IOException;

  boolean deleteErrorPattern(String descriptionRegex)
      throws IOException;

  ErrorPatternProfile getErrorPattern(String descriptionRegex)
      throws IOException;

  List<ErrorPatternProfile> getAllErrorPatterns()
      throws IOException;

  List<ErrorPatternProfile> getErrorPatternsByCategory(String categoryName)
      throws IOException;

  void addErrorCategory(Category category)
      throws IOException;

  Category getErrorCategory(String categoryName)
      throws IOException;

  int getErrorCategoryPriority(String categoryName)
      throws IOException;

  List<Category> getAllErrorCategories()
      throws IOException;

  List<ErrorPatternProfile> getAllErrorIssuesOrderedByCategoryPriority()
      throws IOException;

  Category getDefaultCategory()
      throws IOException;
}
