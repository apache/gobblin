package org.apache.gobblin.metastore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.gobblin.configuration.Category;
import org.apache.gobblin.configuration.ErrorPatternProfile;


/**
 * An in-memory implementation of the ErrorPatternStore interface.
 * This class is used for testing purposes and does not persist data across application restarts.
 */
public class InMemoryErrorPatternStore implements ErrorPatternStore {
  private List<ErrorPatternProfile> errorPatterns = new ArrayList<>();
  private Map<String, Category> categories = new HashMap<>();
  private Category defaultCategory = null;

  private static String DEFAULT_CATEGORY_NAME = "UNKNOWN";
  private static final int DEFAULT_PRIORITY = Integer.MAX_VALUE;

  public InMemoryErrorPatternStore() {
    Category user = new Category("USER", 1);
    this.categories.put(user.getCategoryName(), user);

    this.errorPatterns.add(new org.apache.gobblin.configuration.ErrorPatternProfile(".*file not found.*", "USER"));
    this.defaultCategory = new Category(DEFAULT_CATEGORY_NAME, DEFAULT_PRIORITY);
  }

  @Override
  public void addErrorPattern(org.apache.gobblin.configuration.ErrorPatternProfile issue)
      throws IOException {
    errorPatterns.add(issue);
  }

  @Override
  public boolean deleteErrorPattern(String descriptionRegex)
      throws IOException {
    if (errorPatterns == null) {
      return false;
    }
    return errorPatterns.removeIf(issue -> issue.getDescriptionRegex().matches(descriptionRegex));
  }

  @Override
  public org.apache.gobblin.configuration.ErrorPatternProfile getErrorPattern(String descriptionRegex)
      throws IOException {
    if (errorPatterns == null) {
      return null;
    }
    for (org.apache.gobblin.configuration.ErrorPatternProfile issue : errorPatterns) {
      if (issue.getDescriptionRegex().matches(descriptionRegex)) {
        return issue;
      }
    }
    return null;
  }

  @Override
  public List<org.apache.gobblin.configuration.ErrorPatternProfile> getAllErrorPatterns()
      throws IOException {
    return new ArrayList<>(errorPatterns);
  }

  @Override
  public List<org.apache.gobblin.configuration.ErrorPatternProfile> getErrorPatternsByCategory(String categoryName)
      throws IOException {
    List<org.apache.gobblin.configuration.ErrorPatternProfile> result = new ArrayList<>();
    if (errorPatterns != null) {
      for (org.apache.gobblin.configuration.ErrorPatternProfile issue : errorPatterns) {
        if (issue.getCategoryName() != null && issue.getCategoryName().equals(categoryName)) {
          result.add(issue);
        }
      }
    }
    return result;
  }

  @Override
  public void addErrorCategory(Category category)
      throws IOException {
    if (category != null) {
      categories.put(category.getCategoryName(), category);
    }
  }

  @Override
  public Category getErrorCategory(String categoryName)
      throws IOException {
    return categories.get(categoryName);
  }

  @Override
  public int getErrorCategoryPriority(String categoryName)
      throws IOException {
    Category category = getErrorCategory(categoryName);
    if (category != null) {
      return category.getPriority();
    }
    return 0; //TBD: what should return value be if no priority
  }

  @Override
  public List<Category> getAllErrorCategories()
      throws IOException {
    return Collections.emptyList();
  }

  public void setDefaultCategory(Category category) {
    this.defaultCategory = category;
  }

  @Override
  public Category getDefaultCategory()
      throws IOException {
    if (defaultCategory == null) {
      defaultCategory = new Category(DEFAULT_CATEGORY_NAME, DEFAULT_PRIORITY);
    }
    return defaultCategory;
  }

  @Override
  public List<org.apache.gobblin.configuration.ErrorPatternProfile> getAllErrorIssuesOrderedByCategoryPriority()
      throws IOException {
      if( errorPatterns == null || errorPatterns.isEmpty()) {
        getAllErrorPatterns();
      }
        errorPatterns.sort((issue1, issue2) -> {
          Category cat1 = categories.get(issue1.getCategoryName());
          Category cat2 = categories.get(issue2.getCategoryName());
          if (cat1 == null || cat2 == null) {
            return 0;
          }
          return Integer.compare(cat1.getPriority(), cat2.getPriority());
        });
        return errorPatterns;
    }
}
