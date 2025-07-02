package org.apache.gobblin.metastore;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.ErrorIssue;
import org.apache.gobblin.configuration.Category;

import com.typesafe.config.Config;

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
 * MySQL-backed implementation of IssueStore, following Gobblin's MysqlDagActionStore pattern.
 * This class provides methods to primarily retrieve error regex patterns and error categories.
 */
@Slf4j
public class MysqlErrorIssueStore implements ErrorIssueStore {
  private final DataSource dataSource;
  private final String errorRegexSummaryStoreTable;
  private final String errorCategoriesTable;
  public static final String CONFIG_PREFIX = "MysqlErrorIssueStore";

  private static final int MAX_CHARACTERS_IN_DESCRIPTION_REGEX = 255; //do we want to configure this?

  private static final String CREATE_ERROR_REGEX_SUMMARY_STORE_TABLE_STATEMENT =
      "CREATE TABLE IF NOT EXISTS %s (" +
      "  description_regex VARCHAR(" + MAX_CHARACTERS_IN_DESCRIPTION_REGEX + ") NOT NULL UNIQUE, " +
      "  error_category_name VARCHAR(" + MAX_CHARACTERS_IN_DESCRIPTION_REGEX + ") NOT NULL" +
      ")";

  private static final String CREATE_ERROR_CATEGORIES_TABLE_NAME =
      "CREATE TABLE IF NOT EXISTS %s (" +
      " error_category_name VARCHAR(" + MAX_CHARACTERS_IN_DESCRIPTION_REGEX + ") PRIMARY KEY, priority INT NOT NULL" +
      " )";

  private static final String INSERT_ERROR_CATEGORY_STATEMENT = "INSERT INTO %s (error_category_name, priority) "
    + "VALUES (?, ?) ON DUPLICATE KEY UPDATE priority=VALUES(priority)";

  private static final String GET_ERROR_CATEGORY_STATEMENT = "SELECT error_category_name, priority FROM %s WHERE error_category_name = ?";

  private static final String GET_ALL_ERROR_CATEGORIES_STATEMENT = "SELECT error_category_name, priority FROM %s";

  private static final String INSERT_ERROR_REGEX_SUMMARY_STATEMENT = "INSERT INTO %s (description_regex, error_category_name) "
      + "VALUES (?, ?) ON DUPLICATE KEY UPDATE error_category_name=VALUES(error_category_name)";

  private static final String DELETE_ERROR_REGEX_SUMMARY_STATEMENT = "DELETE FROM %s WHERE description_regex = ?";

  private static final String GET_ERROR_REGEX_SUMMARY_STATEMENT = "SELECT description_regex, error_category_name FROM %s WHERE description_regex = ?";

  private static final String GET_ALL_ERROR_REGEX_SUMMARIES_STATEMENT = "SELECT description_regex, error_category_name FROM %s";

  private static final String GET_DEFAULT_CATEGORY_STATEMENT = "SELECT error_category_name, priority FROM %s WHERE is_default = TRUE ORDER BY priority DESC";

  @Inject
  public MysqlErrorIssueStore(Config config)
      throws IOException {
    log.info("Inside MysqlErrorIssueStore constructor");
    if (config.hasPath(CONFIG_PREFIX)) {
      config = config.getConfig(CONFIG_PREFIX).withFallback(config);
    } else {
      throw new IOException("Please specify the config for MysqlDagActionStore");
    }
    this.errorRegexSummaryStoreTable = ConfigUtils.getString(config, ConfigurationKeys.ERROR_REGEX_DB_TABLE_KEY, "error_summary_regex_store");
    this.errorCategoriesTable = ConfigUtils.getString(config, ConfigurationKeys.ERROR_CATEGORIES_DB_TABLE_KEY, "error_categories");
    this.dataSource = MysqlDataSourceFactory.get(config, SharedResourcesBrokerFactory.getImplicitBroker());
    log.info("MysqlErrorIssueStore almost initialized");
    createTablesIfNotExist();
    log.info("MysqlErrorIssueStore initialized");
  }

  /*
    //TBD: should we follow the pattern of MysqlStateStore and do a sample query to ensure connection is valid?
    private DataSource newDataSource(Config config) {
      HikariDataSource ds = new HikariDataSource();
      PasswordManager pm = PasswordManager.getInstance(ConfigUtils.configToProperties(config));
      ds.setJdbcUrl(config.getString(ConfigurationKeys.STATE_STORE_DB_URL_KEY)); //TBD: should we use a different key? like: SERVICE_DB_URL_KEY
      ds.setUsername(pm.readPassword(config.getString(ConfigurationKeys.STATE_STORE_DB_USER_KEY)));
      ds.setPassword(pm.readPassword(config.getString(ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY)));
      ds.setDriverClassName(ConfigUtils.getString(config,ConfigurationKeys.STATE_STORE_DB_JDBC_DRIVER_KEY,
          ConfigurationKeys.DEFAULT_STATE_STORE_DB_JDBC_DRIVER));
      ds.setMinimumIdle(0);
      ds.setAutoCommit(false);
      return ds;
    }
  */

  private void createTablesIfNotExist()
      throws IOException {

    try (Connection connection = dataSource.getConnection();
        PreparedStatement createStatement = connection.prepareStatement(String.format(CREATE_ERROR_REGEX_SUMMARY_STORE_TABLE_STATEMENT, errorRegexSummaryStoreTable))) {
      createStatement.executeUpdate();
      connection.commit();
    } catch (SQLException e) {
      throw new IOException("Failure creation: error_regex_summary.", e);
    }

    try (Connection connection = dataSource.getConnection();
        PreparedStatement createStatement = connection.prepareStatement(String.format(
            CREATE_ERROR_CATEGORIES_TABLE_NAME, errorCategoriesTable))) {
      createStatement.executeUpdate();
      connection.commit();
    } catch (SQLException e) {
      throw new IOException("Failure creation: error_categories.", e);
    }

  }

  @Override
  public void addErrorCategory(Category category)
      throws IOException {
    String sql = String.format(INSERT_ERROR_CATEGORY_STATEMENT, errorCategoriesTable);
    try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, category.getCategoryName());
      ps.setInt(2, category.getPriority());
      ps.executeUpdate();
      conn.commit();
    } catch (SQLException e) {
      throw new IOException("Failed to add category", e);
    }
  }

  @Override
  public Category getErrorCategory(String categoryName)
      throws IOException {
    try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(String.format(GET_ERROR_CATEGORY_STATEMENT, errorCategoriesTable))) {
      ps.setString(1, categoryName);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return new Category(rs.getString(1), rs.getInt(2));
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
    Category cat = getErrorCategory(categoryName);
    return cat != null ? cat.getPriority() : -1;
  }

  @Override
  public List<Category> getAllErrorCategories()
      throws IOException {
    List<Category> categories = new ArrayList<>();
    try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(String.format(GET_ALL_ERROR_CATEGORIES_STATEMENT, errorCategoriesTable));
        ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        categories.add(new Category(rs.getString(1), rs.getInt(2)));
      }
    } catch (SQLException e) {
      throw new IOException("Failed to get all categories", e);
    }
    return categories;
  }

  @Override
  public void addErrorIssue(ErrorIssue issue)
      throws IOException {
    try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(String.format(INSERT_ERROR_REGEX_SUMMARY_STATEMENT, errorRegexSummaryStoreTable))) {
      ps.setString(1, issue.getDescriptionRegex());
      ps.setString(2, issue.getCategoryName());
      ps.executeUpdate();
      conn.commit();
    } catch (SQLException e) {
      throw new IOException("Failed to add issue", e);
    }
  }

  @Override
  public boolean deleteErrorIssue(String descriptionRegex)
      throws IOException {
    try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(String.format(DELETE_ERROR_REGEX_SUMMARY_STATEMENT, errorRegexSummaryStoreTable))) {
      ps.setString(1, descriptionRegex);
      int rows = ps.executeUpdate();
      conn.commit();
      return rows > 0;
    } catch (SQLException e) {
      throw new IOException("Failed to delete issue", e);
    }
  }

  @Override
  public ErrorIssue getErrorIssue(String descriptionRegex)
      throws IOException {
    try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(String.format(GET_ERROR_REGEX_SUMMARY_STATEMENT, errorRegexSummaryStoreTable))) {
      ps.setString(1, descriptionRegex);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return new ErrorIssue(rs.getString(1), rs.getString(2));
        }
      }
    } catch (SQLException e) {
      throw new IOException("Failed to get issue", e);
    }
    return null;
  }

  @Override
  public List<ErrorIssue> getAllErrorIssues()
      throws IOException {
    List<ErrorIssue> issues = new ArrayList<>();
    try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(String.format(GET_ALL_ERROR_REGEX_SUMMARIES_STATEMENT, errorRegexSummaryStoreTable));
        ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        issues.add(new ErrorIssue(rs.getString(1), rs.getString(2)));
      }
    } catch (SQLException e) {
      throw new IOException("Failed to get all issues", e);
    }
    return issues;
  }

  @Override
  public List<ErrorIssue> getErrorIssuesByCategory(String categoryName)
      throws IOException {
    String sql = "SELECT description_regex, error_category_name FROM " + errorRegexSummaryStoreTable + " WHERE error_category_name = ?";
    List<ErrorIssue> issues = new ArrayList<>();
    try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, categoryName);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          issues.add(new ErrorIssue(rs.getString(1), rs.getString(2)));
        }
      }
    } catch (SQLException e) {
      throw new IOException("Failed to get issues by category", e);
    }
    return issues;
  }

  @Override
  public Category getDefaultCategory()
      throws IOException {
    if (hasIsDefaultColumn()) {
      Category cat = getDefaultCategoryFromIsDefault();
      if (cat != null) {
        return cat;
      }
    }
    // Fallback to previous logic: category with the highest priority (lowest priority number)
    List<Category> categories = getAllErrorCategories();
    if (categories.isEmpty()) {
      return null;
    }
    Category defaultCat = categories.get(0);
    for (Category cat : categories) {
      if (cat.getPriority() > defaultCat.getPriority()) {
        defaultCat = cat;
      }
    }
    return defaultCat;
  }

  /**
   * Checks if the is_default column exists in the categories table.
   */
  private boolean hasIsDefaultColumn()
      throws IOException {
    try (Connection conn = dataSource.getConnection()) {
      try (ResultSet rs = conn.getMetaData().getColumns(null, null, errorCategoriesTable, "is_default")) {
        return rs.next();
      }
    } catch (SQLException e) {
      throw new IOException("Failed to check for is_default column", e);
    }
  }

  /**
   * Returns the default category using is_default column, or null if not found.
   */
  private Category getDefaultCategoryFromIsDefault()
      throws IOException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(String.format(GET_DEFAULT_CATEGORY_STATEMENT, errorCategoriesTable))) {
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return new Category(rs.getString(1), rs.getInt(2));
        }
      }
    } catch (SQLException e) {
      throw new IOException("Failed to get default category with is_default", e);
    }
    return null;
  }
}
