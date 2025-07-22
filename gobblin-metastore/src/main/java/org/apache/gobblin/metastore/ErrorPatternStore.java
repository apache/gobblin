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

import org.apache.gobblin.configuration.ErrorCategory;
import org.apache.gobblin.configuration.ErrorPatternProfile;

import java.io.IOException;
import java.util.List;


/**
 * Interface for a store that persists Errors and Categories.
 **/
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
