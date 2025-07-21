package org.apache.gobblin.metastore;

import org.apache.gobblin.configuration.ErrorCategory;
import org.apache.gobblin.configuration.ErrorPatternProfile;

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

  void addErrorCategory(ErrorCategory errorCategory)
      throws IOException;

  ErrorCategory getErrorCategory(String categoryName)
      throws IOException;

  int getErrorCategoryPriority(String categoryName)
      throws IOException;

  List<ErrorCategory> getAllErrorCategories()
      throws IOException;

  List<ErrorPatternProfile> getAllErrorPatternsOrderedByCategoryPriority()
      throws IOException;

  ErrorCategory getDefaultCategory()
      throws IOException;
}
