/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.metastore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;

import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ErrorCategory;
import org.apache.gobblin.configuration.ErrorPatternProfile;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.util.ConfigUtils;


/**
 * An in-memory implementation of the ErrorPatternStore interface.
 * This class serves as a (default) base class for initialisation and does not persist data across application restarts.
 **/
@Slf4j
public class InMemoryErrorPatternStore implements ErrorPatternStore {
  private final List<ErrorPatternProfile> errorPatterns = new ArrayList<>();
  private final Map<String, ErrorCategory> categories = new HashMap<>();
  private ErrorCategory _defaultErrorCategory = null;

  private static final String DEFAULT_CATEGORY_NAME = "UNKNOWN";

  private final int default_priority;

  @Inject
  public InMemoryErrorPatternStore(Config config) {
    ErrorCategory user = new ErrorCategory("USER", 1);
    this.categories.put(user.getCategoryName(), user);

    this.errorPatterns.add(new ErrorPatternProfile(".*file not found.*", "USER"));
    default_priority = ConfigUtils.getInt(config, ServiceConfigKeys.ERROR_CLASSIFICATION_DEFAULT_PRIORITY_KEY,
        ServiceConfigKeys.DEFAULT_PRIORITY_VALUE);

    this._defaultErrorCategory = new ErrorCategory(DEFAULT_CATEGORY_NAME, default_priority);
  }

  public void upsertCategory(List<ErrorCategory> categories) {
    for (ErrorCategory errorCategory : categories) {
      this.categories.put(errorCategory.getCategoryName(), errorCategory);
    }
  }

  public void upsertPatterns(List<ErrorPatternProfile> patterns) {
    // Clear existing patterns and add all new ones
    this.errorPatterns.clear();
    this.errorPatterns.addAll(patterns);
  }

  public void setDefaultCategory(ErrorCategory errorCategory) {
    this._defaultErrorCategory = errorCategory;
  }

  @Override
  public void addErrorPattern(ErrorPatternProfile issue)
      throws IOException {
    errorPatterns.add(issue);
  }

  @Override
  public boolean deleteErrorPattern(String descriptionRegex)
      throws IOException {
    return errorPatterns.removeIf(issue -> issue.getDescriptionRegex().equals(descriptionRegex));
  }

  @Override
  public ErrorPatternProfile getErrorPattern(String descriptionRegex)
      throws IOException {
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
    for (ErrorPatternProfile issue : errorPatterns) {
      if (issue.getCategoryName() != null && issue.getCategoryName().equals(categoryName)) {
        result.add(issue);
      }
    }
    return result;
  }

  @Override
  public void addErrorCategory(ErrorCategory errorCategory)
      throws IOException {
    if (errorCategory != null) {
      categories.put(errorCategory.getCategoryName(), errorCategory);
    }
  }

  @Override
  public ErrorCategory getErrorCategory(String categoryName)
      throws IOException {
    return categories.get(categoryName);
  }

  @Override
  public int getErrorCategoryPriority(String categoryName)
      throws IOException {
    ErrorCategory errorCategory = getErrorCategory(categoryName);
    if (errorCategory != null) {
      return errorCategory.getPriority();
    }
    throw new IOException("ErrorCategory not found: " + categoryName);
  }

  @Override
  public List<ErrorCategory> getAllErrorCategories()
      throws IOException {
    return new ArrayList<>(categories.values());
  }

  @Override
  public ErrorCategory getDefaultCategory()
      throws IOException {
    if (_defaultErrorCategory == null) {
      _defaultErrorCategory = new ErrorCategory(DEFAULT_CATEGORY_NAME, default_priority);
    }
    return _defaultErrorCategory;
  }

  @Override
  public List<ErrorPatternProfile> getAllErrorPatternsOrderedByCategoryPriority()
      throws IOException {
    errorPatterns.sort((issue1, issue2) -> {
      ErrorCategory cat1 = categories.get(issue1.getCategoryName());
      ErrorCategory cat2 = categories.get(issue2.getCategoryName());
      if (cat1 == null && cat2 == null) {
        return 0;
      }
      if (cat1 == null) {
        return 1;
      }
      if (cat2 == null) {
        return -1;
      }
      return Integer.compare(cat1.getPriority(), cat2.getPriority());
    });
    return errorPatterns;
  }
}
