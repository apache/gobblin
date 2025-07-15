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
* This class serves as a base class for initialisation and does not persist data across application restarts.
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
  public void addErrorPattern(ErrorPatternProfile issue)
      throws IOException {
    errorPatterns.add(issue);
  }

  @Override
  public boolean deleteErrorPattern(String descriptionRegex)
      throws IOException {
    if (errorPatterns == null) {
      return false;
    }
    return errorPatterns.removeIf(issue -> issue.getDescriptionRegex().equals(descriptionRegex));
  }

  @Override
  public ErrorPatternProfile getErrorPattern(String descriptionRegex)
      throws IOException {
    if (errorPatterns == null) {
      return null;
    }
    for (ErrorPatternProfile issue : errorPatterns) {
      if (issue.getDescriptionRegex().equals(descriptionRegex)) {
        return issue;
      }
    }
    return null;
  }

  @Override
  public List<ErrorPatternProfile> getAllErrorPatterns()
      throws IOException {
    return new ArrayList<>(errorPatterns);
  }

  @Override
  public List<ErrorPatternProfile> getErrorPatternsByCategory(String categoryName)
      throws IOException {
    List<ErrorPatternProfile> result = new ArrayList<>();
    if (errorPatterns != null) {
      for (ErrorPatternProfile issue : errorPatterns) {
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
    throw new IOException("Category not found: " + categoryName);
  }

  @Override
  public List<Category> getAllErrorCategories()
      throws IOException {
    return new ArrayList<>(categories.values());
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
  public List<ErrorPatternProfile> getAllErrorIssuesOrderedByCategoryPriority()
      throws IOException {
    if (errorPatterns == null) {
      throw new IOException("Error patterns list is null");
    }
    errorPatterns.sort((issue1, issue2) -> {
      Category cat1 = categories.get(issue1.getCategoryName());
      Category cat2 = categories.get(issue2.getCategoryName());
      if (cat1 == null && cat2 == null) {
        return 0;
      }
      if (cat1 == null) return -1;
      if (cat2 == null) return 1;
      return Integer.compare(cat1.getPriority(), cat2.getPriority());
    });
    return errorPatterns;
  }
}
