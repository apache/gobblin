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

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.ErrorPatternProfile;
import org.apache.gobblin.configuration.ErrorCategory;
import org.apache.gobblin.service.ServiceConfigKeys;

import com.typesafe.config.Config;

import org.apache.commons.lang3.StringUtils;

import org.apache.gobblin.util.ConfigUtils;

import javax.sql.DataSource;

import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


/**
 * MySQL-backed implementation of ErrorPatternStore.
 *
 * This class provides methods to primarily retrieve error regex patterns and error categories.
 * There are also methods to add and delete, which should be used with caution, and retrieve error patterns and categories.
 *
 * Expected table schemas:
 *
 * 1. error_summary_regex_store
 *    - description_regex: VARCHAR(255) NOT NULL UNIQUE
 *    - error_category_name: VARCHAR(255) NOT NULL
 *
 * 2. error_categories
 *    - error_category_name: VARCHAR(255) PRIMARY KEY
 *    - priority: INT UNIQUE NOT NULL
 *
 * Default Category Configuration:
 * - The default category can be configured via the property: gobblin.service.errorPatternStore.defaultCategory
 * - The configured value must exactly match an existing error_category_name in the error_categories table
 * - If no default is configured, the method returns null (no automatic fallback)
 * - If a configured default category is not found in the database, an IOException will be thrown
 *
 **/
@Slf4j
public class MysqlErrorPatternStore implements ErrorPatternStore {
  private final DataSource dataSource;
  private final String errorRegexSummaryStoreTable;
  private final String errorCategoriesTable;
  public static final String CONFIG_PREFIX = "MysqlErrorPatternStore";

  private static final int DEFAULT_MAX_CHARACTERS_IN_SQL_DESCRIPTION_REGEX = 300;
  private static final int DEFAULT_MAX_CHARACTERS_IN_SQL_CATEGORY_NAME = 255;
  private final int maxCharactersInSqlDescriptionRegex;
  private final int maxCharactersInSqlCategoryName;

  private static final String CREATE_ERROR_REGEX_SUMMARY_STORE_TABLE_STATEMENT =
      "CREATE TABLE IF NOT EXISTS %s (description_regex VARCHAR(%d) NOT NULL, error_category_name VARCHAR(%d) NOT NULL)";

  private static final String CREATE_ERROR_CATEGORIES_TABLE_NAME =
      "CREATE TABLE IF NOT EXISTS %s (error_category_name VARCHAR(%d) PRIMARY KEY, priority INT UNIQUE NOT NULL)";

  private static final String INSERT_ERROR_CATEGORY_STATEMENT = "INSERT INTO %s (error_category_name, priority) "
      + "VALUES (?, ?) ON DUPLICATE KEY UPDATE priority=VALUES(priority)";

  private static final String GET_ERROR_CATEGORY_STATEMENT =
      "SELECT error_category_name, priority FROM %s WHERE error_category_name = ?";

  private static final String GET_ALL_ERROR_CATEGORIES_STATEMENT =
      "SELECT error_category_name, priority FROM %s ORDER BY priority ASC";

  private static final String INSERT_ERROR_REGEX_SUMMARY_STATEMENT =
      "INSERT INTO %s (description_regex, error_category_name) "
          + "VALUES (?, ?) ON DUPLICATE KEY UPDATE error_category_name=VALUES(error_category_name)";

  private static final String DELETE_ERROR_REGEX_SUMMARY_STATEMENT = "DELETE FROM %s WHERE description_regex = ?";

  private static final String GET_ERROR_REGEX_SUMMARY_STATEMENT =
      "SELECT description_regex, error_category_name FROM %s WHERE description_regex   = ?";

  private static final String GET_ERROR_PATTERN_BY_CATEGORY_STATEMENT = "SELECT description_regex, error_category_name FROM %s "
      + " WHERE error_category_name = ?";

  private static final String GET_ALL_ERROR_REGEX_SUMMARIES_STATEMENT =
      "SELECT description_regex, error_category_name FROM %s";

  // This SQL statement retrieves the category with the lowest priority (highest priority value).
  private static final String GET_LOWEST_PRIORITY_ERROR_CATEGORY_STATEMENT =
      "SELECT error_category_name, priority FROM %s ORDER BY priority DESC LIMIT 1";

  private static final String GET_ALL_ERROR_ISSUES_ORDERED_BY_CATEGORY_PRIORITY_STATEMENT =
      "SELECT e.description_regex, e.error_category_name FROM %s e "
          + "JOIN %s c ON e.error_category_name = c.error_category_name "
          + "ORDER BY c.priority ASC";

  private final String configuredDefaultCategoryName;

  @Inject
  public MysqlErrorPatternStore(Config config)
      throws IOException {
    if (config.hasPath(CONFIG_PREFIX)) {
      config = config.getConfig(CONFIG_PREFIX).withFallback(config);
    } else {
      throw new IOException("Please specify the config for MysqlErrorPatternStore");
    }
    this.errorRegexSummaryStoreTable =
        ConfigUtils.getString(config, ConfigurationKeys.ERROR_REGEX_DB_TABLE_KEY, "error_summary_regex_store");
    this.errorCategoriesTable =
        ConfigUtils.getString(config, ConfigurationKeys.ERROR_CATEGORIES_DB_TABLE_KEY, "error_categories");
    this.dataSource = MysqlDataSourceFactory.get(config, SharedResourcesBrokerFactory.getImplicitBroker());

    this.maxCharactersInSqlDescriptionRegex = ConfigUtils.getInt(config, ConfigurationKeys.ERROR_REGEX_VARCHAR_SIZE_KEY,
        DEFAULT_MAX_CHARACTERS_IN_SQL_DESCRIPTION_REGEX);

    this.maxCharactersInSqlCategoryName = ConfigUtils.getInt(config, ConfigurationKeys.ERROR_CATEGORY_VARCHAR_SIZE_KEY,
        DEFAULT_MAX_CHARACTERS_IN_SQL_CATEGORY_NAME);

    // Use ServiceConfigKeys for the default category config key
    this.configuredDefaultCategoryName =
        ConfigUtils.getString(config, ServiceConfigKeys.ERROR_PATTERN_STORE_DEFAULT_CATEGORY_KEY, null);

    createTablesIfNotExist();
  }

  private void createTablesIfNotExist()
      throws IOException {
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement createRegexTable = connection.prepareStatement(
          String.format(CREATE_ERROR_REGEX_SUMMARY_STORE_TABLE_STATEMENT, errorRegexSummaryStoreTable,
              maxCharactersInSqlDescriptionRegex, maxCharactersInSqlCategoryName))) {
        createRegexTable.executeUpdate();
      }

      // Create error_categories table
      try (PreparedStatement createCategoriesTable = connection.prepareStatement(
          String.format(CREATE_ERROR_CATEGORIES_TABLE_NAME, errorCategoriesTable, maxCharactersInSqlCategoryName))) {
        createCategoriesTable.executeUpdate();
      }

      // Commit both changes together
      connection.commit();
    } catch (SQLException e) {
      throw new IOException("Failed to create tables for storing ErrorPatterns", e);
    }
  }

  @Override
  public void addErrorCategory(ErrorCategory errorCategory)
      throws IOException {
    String sql = String.format(INSERT_ERROR_CATEGORY_STATEMENT, errorCategoriesTable);
    try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, errorCategory.getCategoryName());
      ps.setInt(2, errorCategory.getPriority());
      ps.executeUpdate();
      conn.commit();
    } catch (SQLException e) {
      throw new IOException("Failed to add errorCategory", e);
    }
  }

  @Override
  public ErrorCategory getErrorCategory(String categoryName)
      throws IOException {
    try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(
        String.format(GET_ERROR_CATEGORY_STATEMENT, errorCategoriesTable))) {
      ps.setString(1, categoryName);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return new ErrorCategory(rs.getString(1), rs.getInt(2));
        }
      }
    } catch (SQLException e) {
      throw new IOException("Failed to get category", e);
    }
    return null;
  }

  @Override
  public int getErrorCategoryPriority(String categoryName)
      throws IOException {
    ErrorCategory cat = getErrorCategory(categoryName);
    if (cat == null) {
      throw new IOException("ErrorCategory not found: " + categoryName);
    }
    return cat.getPriority();
  }

  @Override
  public List<ErrorCategory> getAllErrorCategories()
      throws IOException {
    List<ErrorCategory> categories = new ArrayList<>();
    try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(
        String.format(GET_ALL_ERROR_CATEGORIES_STATEMENT, errorCategoriesTable)); ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        categories.add(new ErrorCategory(rs.getString(1), rs.getInt(2)));
      }
    } catch (SQLException e) {
      throw new IOException("Failed to get all categories", e);
    }
    return categories;
  }

  @Override
  public void addErrorPattern(ErrorPatternProfile issue)
      throws IOException {
    try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(
        String.format(INSERT_ERROR_REGEX_SUMMARY_STATEMENT, errorRegexSummaryStoreTable))) {
      ps.setString(1, issue.getDescriptionRegex());
      ps.setString(2, issue.getCategoryName());
      ps.executeUpdate();
      conn.commit();
    } catch (SQLException e) {
      throw new IOException("Failed to add issue", e);
    }
  }

  @Override
  public boolean deleteErrorPattern(String descriptionRegex)
      throws IOException {
    try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(
        String.format(DELETE_ERROR_REGEX_SUMMARY_STATEMENT, errorRegexSummaryStoreTable))) {
      ps.setString(1, descriptionRegex);
      int rows = ps.executeUpdate();
      conn.commit();
      return rows > 0;
    } catch (SQLException e) {
      throw new IOException("Failed to delete issue", e);
    }
  }

  @Override
  public ErrorPatternProfile getErrorPattern(String descriptionRegex)
      throws IOException {
    try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(
        String.format(GET_ERROR_REGEX_SUMMARY_STATEMENT, errorRegexSummaryStoreTable))) {
      ps.setString(1, descriptionRegex);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return new ErrorPatternProfile(rs.getString(1), rs.getString(2));
        }
      }
    } catch (SQLException e) {
      throw new IOException("Failed to get issue", e);
    }
    return null;
  }

  @Override
  public List<ErrorPatternProfile> getAllErrorPatterns()
      throws IOException {
    List<ErrorPatternProfile> issues = new ArrayList<>();
    try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(
        String.format(GET_ALL_ERROR_REGEX_SUMMARIES_STATEMENT, errorRegexSummaryStoreTable));
        ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        issues.add(new ErrorPatternProfile(rs.getString(1), rs.getString(2)));
      }
    } catch (SQLException e) {
      throw new IOException("Failed to get all issues", e);
    }
    return issues;
  }

  @Override
  public List<ErrorPatternProfile> getErrorPatternsByCategory(String categoryName)
      throws IOException {
    List<ErrorPatternProfile> issues = new ArrayList<>();
    try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(
        String.format(GET_ERROR_PATTERN_BY_CATEGORY_STATEMENT, errorRegexSummaryStoreTable)
    )) {
      ps.setString(1, categoryName);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          issues.add(new ErrorPatternProfile(rs.getString(1), rs.getString(2)));
        }
      }
    } catch (SQLException e) {
      throw new IOException("Failed to get issues by category", e);
    }
    return issues;
  }

  @Override
  public ErrorCategory getDefaultCategory()
      throws IOException {
    // 1. Try to use the configured default category name if set
    if (StringUtils.isNotBlank(configuredDefaultCategoryName)) {
      ErrorCategory cat = getErrorCategory(configuredDefaultCategoryName);
      if (cat != null) {
        return cat;
      } else {
        // Throw exception if configured category doesn't exist
        throw new IOException(String.format(
            "Configured default category '%s' not found in database",
            configuredDefaultCategoryName));
      }
    }

    // 2. No configuration provided - use lowest priority category as fallback
    ErrorCategory lowestPriorityCategory = getLowestPriorityCategory();
    if (lowestPriorityCategory == null) {
      throw new IOException("No error categories found in database");
    }

    log.info("No default category configured, using lowest priority category: {}",
        lowestPriorityCategory.getCategoryName());
    return lowestPriorityCategory;
  }

  /**
   * Returns the category with the lowest priority, i.e. the highest priority value (descending order).
   */
  private ErrorCategory getLowestPriorityCategory()
      throws IOException {
    try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(
        String.format(GET_LOWEST_PRIORITY_ERROR_CATEGORY_STATEMENT, errorCategoriesTable))) {
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return new ErrorCategory(rs.getString(1), rs.getInt(2));
        }
      }
    } catch (SQLException e) {
      throw new IOException("Failed to get category", e);
    }
    return null;
  }

  /**
   * Returns all ErrorIssues ordered by the priority of their category (ascending), then by description_regex.
   */
  @Override
  public List<ErrorPatternProfile> getAllErrorPatternsOrderedByCategoryPriority()
      throws IOException {
    List<ErrorPatternProfile> issues = new ArrayList<>();
    String sql = String.format(GET_ALL_ERROR_ISSUES_ORDERED_BY_CATEGORY_PRIORITY_STATEMENT, errorRegexSummaryStoreTable,
        errorCategoriesTable);
    try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        issues.add(new ErrorPatternProfile(rs.getString(1), rs.getString(2)));
      }
    } catch (SQLException e) {
      throw new IOException("Failed to get all issues ordered by category priority", e);
    }
    return issues;
  }
}