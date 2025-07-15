package org.apache.gobblin.runtime;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.Category;
import org.apache.gobblin.configuration.ErrorPatternProfile;
import org.apache.gobblin.metastore.ErrorPatternStore;
import org.apache.gobblin.runtime.troubleshooter.Issue;
import org.apache.gobblin.runtime.troubleshooter.IssueSeverity;


/**
 * Classifies issues by matching their summary to in-memory error patterns and categories.
 */
@Slf4j
public class ErrorClassifier {
  private final List<CompiledErrorPattern> errorIssues;
  private final Map<String, Category> categoryMap;
  private ErrorPatternStore errorStore = null;

  private static final int MAX_ERRORS_IN_FINAL_ERROR = 5;//TBD: can be changed. Ask if this is preferred. Or confugurable
  private static final String DEFAULT_CODE = "T0000";
  private Category defaultCategory = null;

  /**
   * Loads all error issues and categories from the store into memory.
   */
  @Inject
  public ErrorClassifier(ErrorPatternStore store)
      throws IOException {
    log.info("Inside ErrorClassifier constructor");
    this.errorStore = store;

    //This obtaining of must be done before getting ErrorIssues, as it is used in ordering ErrorIssues by category priority
    this.categoryMap = new HashMap<>();
    for (Category cat : this.errorStore.getAllErrorCategories()) {
      categoryMap.put(cat.getCategoryName(), cat);
    }
    log.info("Error classifier: Completed error category loading");

    this.errorIssues = new ArrayList<>();
    for (ErrorPatternProfile issue : this.errorStore.getAllErrorIssuesOrderedByCategoryPriority()) {
      errorIssues.add(new CompiledErrorPattern(issue));
    }
    log.info("Error classifier: Completed pattern loading");
    //TBD: delete this
    List<String> regexList = new ArrayList<>();
    for (CompiledErrorPattern pei : errorIssues) {
      regexList.add(pei.issue.getDescriptionRegex());
    }
    log.info("All regex if available: {}", regexList);

    this.defaultCategory = this.errorStore.getDefaultCategory();
    log.info(
        "ErrorClassifier constructor construction complete with {} error issues and {} categories loaded. Default Category is {}",
        errorIssues.size(), categoryMap.size(),
        this.defaultCategory != null ? this.defaultCategory.getCategoryName() : "null");
  }

  /**
   * Returns the highest priority Category matching the given summary, or defaultCategory if initialised, or null if none match.
   */
  public Category classify(String summary) {
    if (summary == null) {
      return null;
    }
    Category highest = null;
    for (CompiledErrorPattern pei : errorIssues) {
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

//  public Issue classifyEarlyStopWithDefault(List<Issue> issues) {
//    Map<String, List<Issue>> categoryToIssues = new HashMap<>();
//    List<Issue> unmatched = new ArrayList<>();
//    Integer highestPriority = null;
//    String highestCategoryName = null;
//    Integer defaultPriority = null;
//    for (Issue issue : issues) {
//      log.info("Classifying issue: {}", issue.getSummary());
//      Category matchedCategory = null;
//      for (CompiledErrorPattern pei : errorIssues) {
//        Category cat = categoryMap.get(pei.getCategoryName());
//        if (cat == null) continue;
//        if (highestPriority!=null && cat.getPriority() > highestPriority) {
//          log.info("Breaking early for issue: {} with category: {} and priority: {}", issue.getSummary(),
//              cat.getCategoryName(), cat.getPriority());
//          break; // Early stop for this issue
//        }
//        if (pei.matches(issue.getSummary())) {
//          matchedCategory = cat;
//          if (highestPriority == null || cat.getPriority() < highestPriority) {
//            highestPriority = cat.getPriority();
//            highestCategoryName = cat.getCategoryName();
//            log.info("Matched issue: {} with category: {} and priority: {}", issue.getSummary(),
//                matchedCategory.getCategoryName(), matchedCategory.getPriority());
//          }
//          log.info("Breaking for issue: {} with category: {}", issue.getSummary(), matchedCategory.getCategoryName());
//          break;
//        }
//      }
//      if (matchedCategory != null) {
//        categoryToIssues.computeIfAbsent(matchedCategory.getCategoryName(), k -> new ArrayList<>()).add(issue);
//        log.info("Added issue: {} to category: {}", issue.getSummary(), matchedCategory.getCategoryName());
//      } else {
//        unmatched.add(issue);
//        if(defaultPriority == null){
//          defaultPriority = defaultCategory != null ? defaultCategory.getPriority() : Integer.MAX_VALUE;
//          if (highestPriority == null || defaultCategory.getPriority() < highestPriority) {
//            highestPriority = defaultPriority;
//            highestCategoryName = defaultCategory.getCategoryName();
//          }
//        }
//        log.info("Added to unmatched issue: {}", issue.getSummary());
//      }
//    }
//    // Only use default if there are unmatched issues
//    if (highestPriority == defaultPriority && !unmatched.isEmpty() && defaultCategory != null) {
//      log.info("Using default category: {} for unmatched issues", defaultCategory.getCategoryName());
//      highestCategoryName = defaultCategory.getCategoryName();
//      for (Issue issue : unmatched) {
//        categoryToIssues.computeIfAbsent(defaultCategory.getCategoryName(), k -> new ArrayList<>()).add(issue);
//      }
//    }
//    if (highestCategoryName == null) {
//      return null;
//    }
//    List<Issue> matched = categoryToIssues.get(highestCategoryName);
//    List<String> summaries = new ArrayList<>();
//    for (int i = 0; i < Math.min(MAX_ERRORS_IN_FINAL_ERROR, matched.size()); i++) {
//      summaries.add(matched.get(i).getSummary());
//      log.info("Matched issue: {}", matched.get(i).getSummary());
//    }
//    String details = String.join(" || ", summaries);
//    log.info("Final details for category {}: {}", highestCategoryName, details);
//    IssueSeverity severity = IssueSeverity.ERROR;
//    return Issue.builder().summary("Category: " + highestCategoryName).details(details).severity(severity)
//        .time(ZonedDateTime.now()).code(DEFAULT_CODE).sourceClass(null).exceptionClass(null).properties(null).build();
//  }

  public Issue classifyEarlyStopWithDefault(List<Issue> issues) {
    if(issues == null || issues.isEmpty()) {
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
      log.info("Classifying issue: {}", issue.getSummary());
      classifySingleIssue(issue, result);
    }

    applyDefaultCategoryIfNeeded(result);
    return result;
  }

  private void classifySingleIssue(Issue issue, ClassificationResult result) {
    Category matchedCategory = findBestMatchingCategory(issue, result.highestPriority);

    if (matchedCategory != null) {
      addMatchedIssue(issue, matchedCategory, result);
    } else {
      addUnmatchedIssue(issue, result);
    }
  }

  private Category findBestMatchingCategory(Issue issue, Integer currentHighestPriority) {
    for (CompiledErrorPattern pei : errorIssues) {
      Category cat = categoryMap.get(pei.getCategoryName());
      if (cat == null) continue;

      // Early stop optimization - skip categories with lower priority
      if (currentHighestPriority != null && cat.getPriority() > currentHighestPriority) {
        log.info("Breaking early for issue: {} with category: {} and priority: {}",
            issue.getSummary(), cat.getCategoryName(), cat.getPriority());
        break;
      }

      if (pei.matches(issue.getSummary())) {
        log.info("Matched issue: {} with category: {} and priority: {}",
            issue.getSummary(), cat.getCategoryName(), cat.getPriority());
        return cat;
      }
    }
    return null;
  }

  private void addMatchedIssue(Issue issue, Category category, ClassificationResult result) {
    result.categoryToIssues
        .computeIfAbsent(category.getCategoryName(), k -> new ArrayList<>())
        .add(issue);

    log.info("Added issue: {} to category: {}", issue.getSummary(), category.getCategoryName());

    updateHighestPriorityIfNeeded(category, result);
  }

  private void addUnmatchedIssue(Issue issue, ClassificationResult result) {
    result.unmatched.add(issue);
    log.info("Added to unmatched issue: {}", issue.getSummary());

    // Initialize default priority only once when we encounter the first unmatched issue
    if (result.defaultPriority == null && defaultCategory != null) {
      result.defaultPriority = defaultCategory.getPriority();
      // Only update highest priority if no category has been matched yet
      // OR if default category has higher priority (lower number)
      if (result.highestPriority == null || defaultCategory.getPriority() < result.highestPriority) {
        result.highestPriority = result.defaultPriority;
        result.highestCategoryName = defaultCategory.getCategoryName();
      }
    }
  }

  private void updateHighestPriorityIfNeeded(Category category, ClassificationResult result) {
    if (result.highestPriority == null || category.getPriority() < result.highestPriority) {
      result.highestPriority = category.getPriority();
      result.highestCategoryName = category.getCategoryName();
      log.info("Updated highest priority category to: {} with priority: {}",
          category.getCategoryName(), category.getPriority());
    }
  }

  private void applyDefaultCategoryIfNeeded(ClassificationResult result) {
    boolean shouldUseDefault = result.highestPriority != null &&
        result.highestPriority.equals(result.defaultPriority) &&
        !result.unmatched.isEmpty() &&
        defaultCategory != null;

    if (shouldUseDefault) {
      log.info("Using default category: {} for unmatched issues", defaultCategory.getCategoryName());
      result.highestCategoryName = defaultCategory.getCategoryName();

      for (Issue issue : result.unmatched) {
        result.categoryToIssues
            .computeIfAbsent(defaultCategory.getCategoryName(), k -> new ArrayList<>())
            .add(issue);
      }
    }
  }

  private Issue buildFinalIssue(String categoryName, Map<String, List<Issue>> categoryToIssues) {
    List<Issue> matchedIssues = categoryToIssues.get(categoryName);
    String details = buildDetailsString(matchedIssues);

    log.info("Final details for category {}: {}", categoryName, details);

    return Issue.builder()
        .summary("Category: " + categoryName)
        .details(details)
        .severity(IssueSeverity.ERROR)
        .time(ZonedDateTime.now())
        .code(DEFAULT_CODE)
        .sourceClass(null)
        .exceptionClass(null)
        .properties(null)
        .build();
  }

  private String buildDetailsString(List<Issue> issues) {
    List<String> summaries = new ArrayList<>();
    int limit = Math.min(MAX_ERRORS_IN_FINAL_ERROR, issues.size());

    for (int i = 0; i < limit; i++) {
      summaries.add(issues.get(i).getSummary());
      log.info("Matched issue: {}", issues.get(i).getSummary());
    }

    return String.join(" || ", summaries);
  }

  // Simple container class to hold classification state
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
        this.pattern = Pattern.compile(issue.getDescriptionRegex(),
            Pattern.CASE_INSENSITIVE); // //TBD: catch exception if regex is invalid
      }
      catch (PatternSyntaxException e) {
        log.error("Invalid regex pattern for issue: {}. Error: {}", issue.getDescriptionRegex(), e.getMessage());
        throw new IllegalArgumentException("Invalid regex pattern: " + issue.getDescriptionRegex(), e);
      }
    }

    boolean matches(String summary) {
      if(summary==null || summary.isEmpty()) {
        return false;
      }
      return pattern.matcher(summary).find();
    }

    String getCategoryName() {
      return issue.getCategoryName();
    }
  }
}


