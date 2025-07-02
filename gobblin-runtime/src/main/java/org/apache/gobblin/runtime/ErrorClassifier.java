package org.apache.gobblin.runtime;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.Category;
import org.apache.gobblin.configuration.ErrorIssue;
import org.apache.gobblin.runtime.troubleshooter.Issue;
import org.apache.gobblin.runtime.troubleshooter.IssueSeverity;
import org.apache.gobblin.metastore.ErrorIssueStore;


/**
 * Classifies issues by matching their summary to in-memory error patterns and categories.
 */
@Slf4j
public class ErrorClassifier {
  private final List<PatternedErrorIssue> errorIssues;
  private final Map<String, Category> categoryMap;
  private ErrorIssueStore errorStore = null;

  private static final int MAX_ERRORS_IN_FINAL_ERROR = 5;//TBD: can be changed. Ask if this is preferred
  private static String DEFAULT_CODE = "T0000";
  private Category defaultCategory = null;// do we want to save store as a private variable?

  /**
   * Loads all error issues and categories from the store into memory.
   */
  @Inject
  public ErrorClassifier(ErrorIssueStore store)
      throws IOException {
    log.info("Inside ErrorClassifier constructor");
    this.errorStore = store;
    this.errorIssues = new ArrayList<>();
    for (ErrorIssue issue : store.getAllErrorIssues()) {
      errorIssues.add(new PatternedErrorIssue(issue));
    }
    log.info("Error classifier: Completed pattern loading");
    this.categoryMap = new HashMap<>();
    for (Category cat : store.getAllErrorCategories()) {
      categoryMap.put(cat.getCategoryName(), cat);
    }
    log.info("Error classifier: Completed error category loading");
    this.defaultCategory = store.getDefaultCategory();
    log.info("ErrorClassifier constructor construction complete with {} error issues and {} categories loaded. Default Category is {}",
        errorIssues.size(), categoryMap.size(), this.defaultCategory != null ? this.defaultCategory.getCategoryName() : "null");
  }

  /**
   * Classifies a list of issues and returns a new Issue summarizing the highest priority category and up to 5 matching errors.
   */
  public Issue classify(List<Issue> issues) {
    Map<String, List<Issue>> categoryToIssues = new HashMap<>();
    for (Issue issue : issues) {
      Category matchedCategory = null;
      for (PatternedErrorIssue pei : errorIssues) {
        if (pei.matches(issue.getSummary())) {
          matchedCategory = categoryMap.get(pei.getCategoryName());
          break;
        }
      }
      if (matchedCategory == null && defaultCategory != null) {
        matchedCategory = defaultCategory;
      }
      if (matchedCategory != null) {
        categoryToIssues.computeIfAbsent(matchedCategory.getCategoryName(), k -> new ArrayList<>()).add(issue);
      }
    }
    Category highest = null;
    for (String catName : categoryToIssues.keySet()) {
      Category cat = categoryMap.get(catName);
      if (cat == null && defaultCategory != null && defaultCategory.getCategoryName().equals(catName)) {
        cat = defaultCategory;
      }
      if (cat == null) {
        continue;
      }
      if (highest == null || cat.getPriority() < highest.getPriority()) {
        highest = cat;
      }
    }
    if (highest == null) {
      return null;
    }
    List<Issue> matched = categoryToIssues.get(highest.getCategoryName());
    List<String> summaries = new ArrayList<>();
    for (int i = 0; i < Math.min(MAX_ERRORS_IN_FINAL_ERROR, matched.size()); i++) {
      summaries.add(matched.get(i).getSummary());
    }
    String details = String.join(" || ", summaries);
    IssueSeverity severity = IssueSeverity.ERROR;
    return Issue.builder().summary("Category: " + highest.getCategoryName()).details(details).severity(severity)
        .time(ZonedDateTime.now()).code(DEFAULT_CODE).sourceClass(null).exceptionClass(null).properties(null).build();
  }

  /**
   * Returns the highest priority Category matching the given summary, or defaultCategory if initialised, or null if none match.
   */
  public Category classify(String summary) {
    if (summary == null) {
      return null;
    }
    Category highest = null;
    for (PatternedErrorIssue pei : errorIssues) {
      if (pei.matches(summary)) {
        Category cat = categoryMap.get(pei.getCategoryName());
        if (cat == null) {
          continue;
        }
        if (highest == null || cat.getPriority() < highest.getPriority()) {
          highest = cat;
        }
      }
    }
    if (highest == null) {
      return defaultCategory != null ? defaultCategory : null;
    }
    return highest;
  }

  /**
   * Helper class to store compiled regex for fast matching.
   */
  private static class PatternedErrorIssue {
    private final ErrorIssue issue;
    private final Pattern pattern;

    PatternedErrorIssue(ErrorIssue issue) {
      this.issue = issue;
      this.pattern = Pattern.compile(issue.getDescriptionRegex()); //TBD: catch exception if regex is invalid
    }

    boolean matches(String summary) {
      return pattern.matcher(summary).find();
    }

    String getCategoryName() {
      return issue.getCategoryName();
    }
  }
}
