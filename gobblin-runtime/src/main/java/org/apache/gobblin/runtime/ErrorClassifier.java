package org.apache.gobblin.runtime;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.typesafe.config.Config;

import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ErrorCategory;
import org.apache.gobblin.configuration.ErrorPatternProfile;
import org.apache.gobblin.metastore.ErrorPatternStore;
import org.apache.gobblin.runtime.troubleshooter.Issue;
import org.apache.gobblin.runtime.troubleshooter.IssueSeverity;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Classifies issues by matching their summary description to error patterns and categories.
 * Categorisation is based on regex patterns and their associated categories.
 * Each category has an associated priority value.
 */
@Slf4j
public class ErrorClassifier {
  private final List<CompiledErrorPattern> errorIssues;
  private final Map<String, ErrorCategory> categoryMap;
  private ErrorPatternStore errorStore = null;

  private final int maxErrorsInFinalIssue;
  private static final String FINAL_ISSUE_ERROR_CODE = "T0000";
  private ErrorCategory _defaultErrorCategory = null;

  /**
   * Loads all error issues and categories from the store into memory.
   */
  @Inject
  public ErrorClassifier(ErrorPatternStore store, Config config)
      throws IOException {
    this.errorStore = store;

    this.maxErrorsInFinalIssue =
        ConfigUtils.getInt(config, ServiceConfigKeys.ERROR_CLASSIFICATION_MAX_ERRORS_IN_FINAL_KEY,
            ServiceConfigKeys.DEFAULT_ERROR_CLASSIFICATION_MAX_ERRORS_IN_FINAL);

    //Obtaining Categories must be done before getting ErrorIssues, as it is used in ordering ErrorIssues by category priority.
    this.categoryMap = new HashMap<>();
    for (ErrorCategory cat : this.errorStore.getAllErrorCategories()) {
      categoryMap.put(cat.getCategoryName(), cat);
    }

    this.errorIssues = new ArrayList<>();
    for (ErrorPatternProfile errorPattern : this.errorStore.getAllErrorPatternsOrderedByCategoryPriority()) {
      errorIssues.add(new CompiledErrorPattern(errorPattern));
    }

    this._defaultErrorCategory = this.errorStore.getDefaultCategory();
  }

  /**
   * Returns the highest priority ErrorCategory matching the given summary, or _defaultErrorCategory if initialised, or null if none match.
   */
  public ErrorCategory classify(String summary) {
    if (summary == null) {
      return null;
    }
    ErrorCategory highest = null;
    for (CompiledErrorPattern pei : errorIssues) {
      if (pei.matches(summary)) {
        ErrorCategory cat = categoryMap.get(pei.getCategoryName());
        if (cat == null) {
          continue;
        }
        if (highest == null || cat.getPriority() < highest.getPriority()) {
          highest = cat;
        }
      }
    }
    if (highest == null) {
      return _defaultErrorCategory != null ? _defaultErrorCategory : null;
    }
    return highest;
  }

  /**
   * Classifies a list of issues and returns the highest priority category with its matched issues.
   * If no issues match, returns null.
   * If _defaultErrorCategory is set, it will be used for unmatched issues.
   */
  public Issue classifyEarlyStopWithDefault(List<Issue> issues) {
    if (issues == null || issues.isEmpty()) {
      return null;
    }

    ClassificationResult result = performClassification(issues);

    if (result.highestCategoryName == null) {
      return null;
    }

    return buildFinalIssue(result.highestCategoryName, result.categoryToIssues);
  }

  private ClassificationResult performClassification(List<Issue> issues) {
    ClassificationResult result = new ClassificationResult();

    for (Issue issue : issues) {
      classifySingleIssue(issue, result);
    }

    applyDefaultCategoryIfNeeded(result);
    return result;
  }

  private void classifySingleIssue(Issue issue, ClassificationResult result) {
    ErrorCategory matchedErrorCategory = findBestMatchingCategory(issue, result.highestPriority);

    if (matchedErrorCategory != null) {
      addMatchedIssue(issue, matchedErrorCategory, result);
    } else {
      addUnmatchedIssue(issue, result);
    }
  }

  private ErrorCategory findBestMatchingCategory(Issue issue, Integer currentHighestPriority) {
    for (CompiledErrorPattern pei : errorIssues) {
      ErrorCategory cat = categoryMap.get(pei.getCategoryName());
      if (cat == null) {
        continue;
      }

      // Early stop optimization - skip categories with lower priority
      if (currentHighestPriority != null && cat.getPriority() > currentHighestPriority) {
        break;
      }

      if (pei.matches(issue.getSummary())) {
        return cat;
      }
    }
    return null;
  }

  private void addMatchedIssue(Issue issue, ErrorCategory errorCategory, ClassificationResult result) {
    result.categoryToIssues.computeIfAbsent(errorCategory.getCategoryName(), k -> new ArrayList<>()).add(issue);

    updateHighestPriorityIfNeeded(errorCategory, result);
  }

  private void addUnmatchedIssue(Issue issue, ClassificationResult result) {
    result.unmatched.add(issue);

    // Initialize default priority only once when we encounter the first unmatched issue
    if (result.defaultPriority == null && _defaultErrorCategory != null) {
      result.defaultPriority = _defaultErrorCategory.getPriority();
      // Only update highest priority if no category has been matched yet OR if default category has higher priority (lower number)
      if (result.highestPriority == null || _defaultErrorCategory.getPriority() < result.highestPriority) {
        result.highestPriority = result.defaultPriority;
        result.highestCategoryName = _defaultErrorCategory.getCategoryName();
      }
    }
  }

  private void updateHighestPriorityIfNeeded(ErrorCategory errorCategory, ClassificationResult result) {
    if (result.highestPriority == null || errorCategory.getPriority() < result.highestPriority) {
      result.highestPriority = errorCategory.getPriority();
      result.highestCategoryName = errorCategory.getCategoryName();
    }
  }

  private void applyDefaultCategoryIfNeeded(ClassificationResult result) {
    boolean shouldUseDefault = result.highestPriority != null && result.highestPriority.equals(result.defaultPriority)
        && !result.unmatched.isEmpty() && _defaultErrorCategory != null;

    if (shouldUseDefault) {
      result.highestCategoryName = _defaultErrorCategory.getCategoryName();

      for (Issue issue : result.unmatched) {
        result.categoryToIssues.computeIfAbsent(_defaultErrorCategory.getCategoryName(), k -> new ArrayList<>()).add(issue);
      }
    }
  }

  private Issue buildFinalIssue(String categoryName, Map<String, List<Issue>> categoryToIssues) {
    List<Issue> matchedIssues = categoryToIssues.get(categoryName);
    String details = buildDetailsString(matchedIssues);

    return Issue.builder().summary("ErrorCategory: " + categoryName).details(details).severity(IssueSeverity.ERROR)
        .time(ZonedDateTime.now()).code(FINAL_ISSUE_ERROR_CODE).sourceClass(null).exceptionClass(null).properties(null).build();
  }

  private String buildDetailsString(List<Issue> issues) {
    List<String> summaries = new ArrayList<>();
    int limit = Math.min(maxErrorsInFinalIssue, issues.size());

    for (int i = 0; i < limit; i++) {
      summaries.add(issues.get(i).getSummary());
    }

    return String.join(" \n ", summaries);
  }

  /**
   * Helper class that stores the result of issue classification, including matched categories, unmatched issues, and priority information.
   */
  private static class ClassificationResult {
    Map<String, List<Issue>> categoryToIssues = new HashMap<>();
    List<Issue> unmatched = new ArrayList<>();
    Integer highestPriority = null;
    String highestCategoryName = null;
    Integer defaultPriority = null;
  }

  /**
   * Helper class to store compiled regex for fast matching.
   */
  private static class CompiledErrorPattern {
    private final ErrorPatternProfile issue;
    private final Pattern pattern;

    CompiledErrorPattern(ErrorPatternProfile issue) {
      this.issue = issue;
      try {
        this.pattern = Pattern.compile(issue.getDescriptionRegex(), Pattern.CASE_INSENSITIVE);
      } catch (PatternSyntaxException e) {
        log.error("Invalid regex pattern for issue: {}. Error: {}", issue.getDescriptionRegex(), e.getMessage());
        throw new IllegalArgumentException("Invalid regex pattern: " + issue.getDescriptionRegex(), e);
      }
    }

    boolean matches(String summary) {
      if (summary == null || summary.isEmpty()) {
        return false;
      }
      return pattern.matcher(summary).find();
    }

    String getCategoryName() {
      return issue.getCategoryName();
    }
  }
}


