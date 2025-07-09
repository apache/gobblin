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
import org.apache.gobblin.metastore.ErrorPatternStore;
import org.apache.gobblin.runtime.troubleshooter.Issue;
import org.apache.gobblin.runtime.troubleshooter.IssueSeverity;


/**
 * Classifies issues by matching their summary to in-memory error patterns and categories.
 */
@Slf4j
public class ErrorClassifier {
  private final List<PatternedErrorIssue> errorIssues;
  private final Map<String, Category> categoryMap;
  private ErrorPatternStore errorStore = null;

  private static final int MAX_ERRORS_IN_FINAL_ERROR = 5;//TBD: can be changed. Ask if this is preferred
  private static String DEFAULT_CODE = "T0000";
  private Category defaultCategory = null;// Tbd: do we want to save store as a private variable?

  /**
   * Loads all error issues and categories from the store into memory.
   */
  @Inject
  public ErrorClassifier(ErrorPatternStore store)
      throws IOException {
    log.info("Inside ErrorClassifier constructor");
    this.errorStore = store;

    //This must be done before getting ErrorIssues, as it is used in ordering ErrorIssues by category priority
    this.categoryMap = new HashMap<>();
    for (Category cat : store.getAllErrorCategories()) {
      categoryMap.put(cat.getCategoryName(), cat);
    }
    log.info("Error classifier: Completed error category loading");

    this.errorIssues = new ArrayList<>();
    for (ErrorIssue issue : store.getAllErrorIssuesOrderedByCategoryPriority()) {
      errorIssues.add(new PatternedErrorIssue(issue));
    }
    log.info("Error classifier: Completed pattern loading");
    //TBD: delete this
    List<String> regexList = new ArrayList<>();
    for (PatternedErrorIssue pei : errorIssues) {
      regexList.add(pei.issue.getDescriptionRegex());
    }
    log.info("All regex if available: {}", regexList);

    this.defaultCategory = store.getDefaultCategory();
    log.info(
        "ErrorClassifier constructor construction complete with {} error issues and {} categories loaded. Default Category is {}",
        errorIssues.size(), categoryMap.size(),
        this.defaultCategory != null ? this.defaultCategory.getCategoryName() : "null");
  }

  /**
   * Classifies a list of issues and returns a new Issue summarizing the highest priority category and up to 5 matching errors.
   */
  public Issue classify(List<Issue> issues) {
    //TBD: if sorting issues in priority order after initalisation, should that function be called here?
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
   * Optimized version of classify: assumes errorIssues are sorted by category priority (ascending, lower number = higher priority).
   * For each issue, assigns the first matching (highest priority) category and skips lower-priority patterns for that issue.
   * Returns a new Issue summarizing the highest priority category and up to 5 matching errors, same as classify().
   */
  public Issue classifyOptimized(List<Issue> issues) {
    Map<String, List<Issue>> categoryToIssues = new HashMap<>();
    Integer highestPriority = null;
    String highestCategoryName = null;
    for (Issue issue : issues) {
      Category matchedCategory = null;
      for (PatternedErrorIssue pei : errorIssues) {
        Category cat = categoryMap.get(pei.getCategoryName());
        if (cat == null) continue;
        if (highestPriority != null && cat.getPriority() > highestPriority) {
          // Since regexes are sorted, all further regexes are lower priority; break for this issue
          break;
        }
        if (pei.matches(issue.getSummary())) {
          matchedCategory = cat;
          if (highestPriority == null || cat.getPriority() < highestPriority) {
            highestPriority = cat.getPriority();
            highestCategoryName = cat.getCategoryName();
          }
          break; // Only assign the first (highest priority) match for this issue
        }
      }
      if (matchedCategory != null) {
        categoryToIssues.computeIfAbsent(matchedCategory.getCategoryName(), k -> new ArrayList<>()).add(issue);
      }
    }
    if (highestCategoryName == null) {
      return null;
    }
    List<Issue> matched = categoryToIssues.get(highestCategoryName);
    List<String> summaries = new ArrayList<>();
    for (int i = 0; i < Math.min(MAX_ERRORS_IN_FINAL_ERROR, matched.size()); i++) {
      summaries.add(matched.get(i).getSummary());
    }
    String details = String.join(" || ", summaries);
    IssueSeverity severity = IssueSeverity.ERROR;
    return Issue.builder().summary("Category: " + highestCategoryName).details(details).severity(severity)
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
   * Early stop per-issue with default: For each issue, go through regexes (sorted by priority).
   * If a regex matches, assign the category and update the highest priority found.
   * If a regex's priority is greater than the current highest (including default), break for this issue.
   * After all issues, if default is the highest, assign unmatched issues to default.
   */
  public Issue classifyEarlyStopWithDefault(List<Issue> issues) {
    Map<String, List<Issue>> categoryToIssues = new HashMap<>();
    List<Issue> unmatched = new ArrayList<>();
    Integer highestPriority = null;
    String highestCategoryName = null;
    int defaultPriority = defaultCategory != null ? defaultCategory.getPriority() : Integer.MAX_VALUE;
    for (Issue issue : issues) {
      Category matchedCategory = null;
      for (PatternedErrorIssue pei : errorIssues) {
        Category cat = categoryMap.get(pei.getCategoryName());
        if (cat == null) {
          continue;
        }
        int currentHighest = (highestPriority != null) ? Math.min(highestPriority, defaultPriority) : defaultPriority;
        if (cat.getPriority() > currentHighest) { // TBD: what if default hasn't come up?
          break; // Early stop for this issue
        }
        if (pei.matches(issue.getSummary())) {
          matchedCategory = cat;
          if (highestPriority == null || cat.getPriority() < highestPriority) {
            highestPriority = cat.getPriority();
            highestCategoryName = cat.getCategoryName();
          }
          break;
        }
      }
      if (matchedCategory != null) {
        categoryToIssues.computeIfAbsent(matchedCategory.getCategoryName(), k -> new ArrayList<>()).add(issue);
      } else {
        unmatched.add(issue);
      }
    }
    // If default is the highest priority found, assign unmatched to default
    if ((highestPriority == null || (defaultCategory != null && defaultPriority < highestPriority))
        && defaultCategory != null) {
      highestPriority = defaultPriority;
      highestCategoryName = defaultCategory.getCategoryName();
      for (Issue issue : unmatched) {
        categoryToIssues.computeIfAbsent(defaultCategory.getCategoryName(), k -> new ArrayList<>()).add(issue);
      }
    }
    if (highestCategoryName == null) {
      return null;
    }
    List<Issue> matched = categoryToIssues.get(highestCategoryName);
    List<String> summaries = new ArrayList<>();
    for (int i = 0; i < Math.min(MAX_ERRORS_IN_FINAL_ERROR, matched.size()); i++) {
      summaries.add(matched.get(i).getSummary());
    }
    String details = String.join(" || ", summaries);
    IssueSeverity severity = IssueSeverity.ERROR;
    return Issue.builder().summary("Category: " + highestCategoryName).details(details).severity(severity)
        .time(ZonedDateTime.now()).code(DEFAULT_CODE).sourceClass(null).exceptionClass(null).properties(null).build();
  }

  /**
   * V2: Only use default category if there are unmatched issues. If all issues match a pattern, use the highest priority matched category.
   * For each issue, go through regexes (sorted by priority). Early stop for this issue if regex priority > current highest (including default).
   */
  public Issue classifyEarlyStopWithDefaultV2(List<Issue> issues) {
    Map<String, List<Issue>> categoryToIssues = new HashMap<>();
    List<Issue> unmatched = new ArrayList<>();
    Integer highestPriority = null;
    String highestCategoryName = null;
    int defaultPriority = defaultCategory != null ? defaultCategory.getPriority() : Integer.MAX_VALUE;
    for (Issue issue : issues) {
      log.info("Classifying issue: {}", issue.getSummary());
      Category matchedCategory = null;
      for (PatternedErrorIssue pei : errorIssues) {
        Category cat = categoryMap.get(pei.getCategoryName());
        if (cat == null) continue;
        int currentHighest = (highestPriority != null) ? Math.min(highestPriority, defaultPriority) : defaultPriority;
        if (cat.getPriority() > currentHighest) {
          log.info("Breaking early for issue: {} with category: {} and priority: {}", issue.getSummary(),
              cat.getCategoryName(), cat.getPriority());
          break; // Early stop for this issue
        }
        if (pei.matches(issue.getSummary())) {
          matchedCategory = cat;
          if (highestPriority == null || cat.getPriority() < highestPriority) {
            highestPriority = cat.getPriority();
            highestCategoryName = cat.getCategoryName();
            log.info("Matched issue: {} with category: {} and priority: {}", issue.getSummary(),
                matchedCategory.getCategoryName(), matchedCategory.getPriority());
          }
          log.info("Breaking for issue: {} with category: {}", issue.getSummary(), matchedCategory.getCategoryName());
          break;
        }
      }
      if (matchedCategory != null) {
        categoryToIssues.computeIfAbsent(matchedCategory.getCategoryName(), k -> new ArrayList<>()).add(issue);
        log.info("Added issue: {} to category: {}", issue.getSummary(), matchedCategory.getCategoryName());
      } else {
        unmatched.add(issue);
        log.info("Added to unmatched issue: {}", issue.getSummary());
      }
    }
    // Only use default if there are unmatched issues
    if (!unmatched.isEmpty() && defaultCategory != null && (highestPriority == null || defaultPriority < highestPriority)) {
      log.info("Using default category: {} for unmatched issues", defaultCategory.getCategoryName());
      highestPriority = defaultPriority;
      highestCategoryName = defaultCategory.getCategoryName();
      for (Issue issue : unmatched) {
        categoryToIssues.computeIfAbsent(defaultCategory.getCategoryName(), k -> new ArrayList<>()).add(issue);
      }
    }
    if (highestCategoryName == null) {
      return null;
    }
    List<Issue> matched = categoryToIssues.get(highestCategoryName);
    List<String> summaries = new ArrayList<>();
    for (int i = 0; i < Math.min(MAX_ERRORS_IN_FINAL_ERROR, matched.size()); i++) {
      summaries.add(matched.get(i).getSummary());
      log.info("Matched issue: {}", matched.get(i).getSummary());
    }
    String details = String.join(" || ", summaries);
    log.info("Final details for category {}: {}", highestCategoryName, details);
    IssueSeverity severity = IssueSeverity.ERROR;
    return Issue.builder().summary("Category: " + highestCategoryName).details(details).severity(severity)
        .time(ZonedDateTime.now()).code(DEFAULT_CODE).sourceClass(null).exceptionClass(null).properties(null).build();
  }

  public Issue classifyEarlyStopWithDefaultV4(List<Issue> issues) {
    Map<String, List<Issue>> categoryToIssues = new HashMap<>();
    List<Issue> unmatched = new ArrayList<>();
    Integer highestPriority = null;
    String highestCategoryName = null;
    Integer defaultPriority = null;
//    int defaultPriority = defaultCategory != null ? defaultCategory.getPriority() : Integer.MAX_VALUE;
    for (Issue issue : issues) {
      log.info("Classifying issue: {}", issue.getSummary());
      Category matchedCategory = null;
      for (PatternedErrorIssue pei : errorIssues) {
        Category cat = categoryMap.get(pei.getCategoryName());
        if (cat == null) continue;
//        int currentHighest = (highestPriority != null) ? Math.min(highestPriority, defaultPriority) : defaultPriority;
        if (highestPriority!=null && cat.getPriority() > highestPriority) {
          log.info("Breaking early for issue: {} with category: {} and priority: {}", issue.getSummary(),
              cat.getCategoryName(), cat.getPriority());
          break; // Early stop for this issue
        }
        if (pei.matches(issue.getSummary())) {
          matchedCategory = cat;
          if (highestPriority == null || cat.getPriority() < highestPriority) {
            highestPriority = cat.getPriority();
            highestCategoryName = cat.getCategoryName();
            log.info("Matched issue: {} with category: {} and priority: {}", issue.getSummary(),
                matchedCategory.getCategoryName(), matchedCategory.getPriority());
          }
          log.info("Breaking for issue: {} with category: {}", issue.getSummary(), matchedCategory.getCategoryName());
          break;
        }
      }
      if (matchedCategory != null) {
        categoryToIssues.computeIfAbsent(matchedCategory.getCategoryName(), k -> new ArrayList<>()).add(issue);
        log.info("Added issue: {} to category: {}", issue.getSummary(), matchedCategory.getCategoryName());
      } else {
        unmatched.add(issue);
        if(defaultPriority == null){
          defaultPriority = defaultCategory != null ? defaultCategory.getPriority() : Integer.MAX_VALUE;
          if (highestPriority == null || defaultCategory.getPriority() < highestPriority) {
            highestPriority = defaultPriority;
            highestCategoryName = defaultCategory.getCategoryName();
          }
        }
        log.info("Added to unmatched issue: {}", issue.getSummary());
      }
    }
    // Only use default if there are unmatched issues
    if (highestPriority == defaultPriority && !unmatched.isEmpty() && defaultCategory != null) {
      log.info("Using default category: {} for unmatched issues", defaultCategory.getCategoryName());
      highestCategoryName = defaultCategory.getCategoryName();
      for (Issue issue : unmatched) {
        categoryToIssues.computeIfAbsent(defaultCategory.getCategoryName(), k -> new ArrayList<>()).add(issue);
      }
    }
    if (highestCategoryName == null) {
      return null;
    }
    List<Issue> matched = categoryToIssues.get(highestCategoryName);
    List<String> summaries = new ArrayList<>();
    for (int i = 0; i < Math.min(MAX_ERRORS_IN_FINAL_ERROR, matched.size()); i++) {
      summaries.add(matched.get(i).getSummary());
      log.info("Matched issue: {}", matched.get(i).getSummary());
    }
    String details = String.join(" || ", summaries);
    log.info("Final details for category {}: {}", highestCategoryName, details);
    IssueSeverity severity = IssueSeverity.ERROR;
    return Issue.builder().summary("Category: " + highestCategoryName).details(details).severity(severity)
        .time(ZonedDateTime.now()).code(DEFAULT_CODE).sourceClass(null).exceptionClass(null).properties(null).build();
  }

  /**
   * V3: Only use default category if there are unmatched issues, and only consider categories (including default) that actually have issues assigned.
   * For each issue, go through regexes (sorted by priority). Early stop for this issue if regex priority > current highest (including default).
   * This version ensures the default category is not selected unless it actually has issues assigned.
   */
  public Issue classifyEarlyStopWithDefaultV3(List<Issue> issues) {
    Map<String, List<Issue>> categoryToIssues = new HashMap<>();
    List<Issue> unmatched = new ArrayList<>();
    int defaultPriority = defaultCategory != null ? defaultCategory.getPriority() : Integer.MAX_VALUE;
    for (Issue issue : issues) {
      log.info("Classifying issue: {}", issue.getSummary());
      Category matchedCategory = null;
      for (PatternedErrorIssue pei : errorIssues) {
        Category cat = categoryMap.get(pei.getCategoryName());
        if (cat == null) continue;
        int currentHighest = defaultPriority; // V3: Only use defaultPriority for early stop
        if (cat.getPriority() > currentHighest) {
          log.info("Breaking early for issue: {} with category: {} and priority: {}", issue.getSummary(),
              cat.getCategoryName(), cat.getPriority());
          break; // Early stop for this issue
        }
        if (pei.matches(issue.getSummary())) {
          matchedCategory = cat;
          log.info("Matched issue: {} with category: {} and priority: {}", issue.getSummary(),
              matchedCategory.getCategoryName(), matchedCategory.getPriority());
          log.info("Breaking for issue: {} with category: {}", issue.getSummary(), matchedCategory.getCategoryName());
          break;
        }
      }
      if (matchedCategory != null) {
        categoryToIssues.computeIfAbsent(matchedCategory.getCategoryName(), k -> new ArrayList<>()).add(issue);
        log.info("Added issue: {} to category: {}", issue.getSummary(), matchedCategory.getCategoryName());
      } else {
        unmatched.add(issue);
        log.info("Added to unmatched issue: {}", issue.getSummary());
      }
    }
    // V3: Only assign unmatched to default if there are unmatched issues and defaultCategory exists
    if (!unmatched.isEmpty() && defaultCategory != null) {
      log.info("Using default category: {} for unmatched issues", defaultCategory.getCategoryName());
      for (Issue issue : unmatched) {
        categoryToIssues.computeIfAbsent(defaultCategory.getCategoryName(), k -> new ArrayList<>()).add(issue);
      }
    }
    // V3: Find the category with the lowest priority value (highest priority) among those with at least one issue
    String highestCategoryName = null;
    Integer highestPriority = null;
    for (String catName : categoryToIssues.keySet()) {
      Category cat = categoryMap.get(catName);
      if (cat == null) continue;
      if (highestPriority == null || cat.getPriority() < highestPriority) {
        highestPriority = cat.getPriority();
        highestCategoryName = catName;
      }
    }
    if (highestCategoryName == null) {
      return null;
    }
    List<Issue> matched = categoryToIssues.get(highestCategoryName);
    List<String> summaries = new ArrayList<>();
    for (int i = 0; i < Math.min(MAX_ERRORS_IN_FINAL_ERROR, matched.size()); i++) {
      summaries.add(matched.get(i).getSummary());
      log.info("Matched issue: {}", matched.get(i).getSummary());
    }
    String details = String.join(" || ", summaries);
    log.info("Final details for category {}: {}", highestCategoryName, details);
    IssueSeverity severity = IssueSeverity.ERROR;
    return Issue.builder().summary("Category: " + highestCategoryName).details(details).severity(severity)
        .time(ZonedDateTime.now()).code(DEFAULT_CODE).sourceClass(null).exceptionClass(null).properties(null).build();
  }

  /**
   * Helper class to store compiled regex for fast matching.
   */
  private static class PatternedErrorIssue {
    private final ErrorIssue issue;
    private final Pattern pattern;

    PatternedErrorIssue(ErrorIssue issue) {
      this.issue = issue;
      this.pattern = Pattern.compile(issue.getDescriptionRegex(), Pattern.CASE_INSENSITIVE); // //TBD: catch exception if regex is invalid
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


