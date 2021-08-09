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

package org.apache.gobblin.service.modules.troubleshooter;

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.reflect.TypeToken;
import com.typesafe.config.Config;

import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.runtime.troubleshooter.Issue;
import org.apache.gobblin.runtime.troubleshooter.IssueSeverity;
import org.apache.gobblin.runtime.troubleshooter.MultiContextIssueRepository;
import org.apache.gobblin.runtime.troubleshooter.TroubleshooterException;
import org.apache.gobblin.runtime.util.GsonUtils;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.db.ServiceDatabaseProvider;


@Singleton
@Slf4j
public class MySqlMultiContextIssueRepository extends AbstractIdleService implements MultiContextIssueRepository {

  private final ServiceDatabaseProvider databaseProvider;
  private final Configuration configuration;
  private ScheduledExecutorService scheduledExecutor;

  public MySqlMultiContextIssueRepository(ServiceDatabaseProvider databaseProvider) {
    this(databaseProvider, MySqlMultiContextIssueRepository.Configuration.builder().build());
  }

  @Inject
  public MySqlMultiContextIssueRepository(ServiceDatabaseProvider databaseProvider, Configuration configuration) {
    this.databaseProvider = Objects.requireNonNull(databaseProvider);
    this.configuration = Objects.requireNonNull(configuration);
  }

  @Override
  public List<Issue> getAll(String contextId)
      throws TroubleshooterException {
    Objects.requireNonNull(contextId, "contextId should not be null");

    String querySql = "select code, time, severity, summary, details, source_class, exception_class, properties "
        + "from issues where context_id = ? order by position";

    try (Connection connection = databaseProvider.getDatasource().getConnection();
        PreparedStatement statement = connection.prepareStatement(querySql)) {

      statement.setString(1, contextId);

      ArrayList<Issue> issues = new ArrayList<>();

      try (ResultSet results = statement.executeQuery()) {
        while (results.next()) {
          Issue.IssueBuilder issue = Issue.builder();
          issue.code(results.getString(1));
          issue.time(ZonedDateTime.ofInstant(Instant.ofEpochMilli(results.getTimestamp(2).getTime()), ZoneOffset.UTC));
          issue.severity(IssueSeverity.valueOf(results.getString(3)));
          issue.summary(results.getString(4));
          issue.details(results.getString(5));
          issue.sourceClass(results.getString(6));
          issue.exceptionClass(results.getString(7));

          String serializedProperties = results.getString(8);
          if (serializedProperties != null) {
            Type mapType = new TypeToken<HashMap<String, String>>() {
            }.getType();

            HashMap<String, String> properties =
                GsonUtils.GSON_WITH_DATE_HANDLING.fromJson(serializedProperties, mapType);
            issue.properties(properties);
          }

          issues.add(issue.build());
        }
      }

      return issues;
    } catch (SQLException e) {
      throw new TroubleshooterException("Cannot read issues from the database", e);
    }
  }

  @Override
  public void put(String contextId, Issue issue)
      throws TroubleshooterException {
    Objects.requireNonNull(contextId, "contextId should not be null");
    Objects.requireNonNull(issue, "issue should not be null");

    put(contextId, Collections.singletonList(issue));
  }

  @Override
  public void put(String contextId, List<Issue> issues)
      throws TroubleshooterException {
    Objects.requireNonNull(contextId, "contextId should not be null");
    Objects.requireNonNull(issues, "issues should not be null");

    String statementSql =
        "replace into issues (context_id, code, time, severity,summary,details,source_class,exception_class,properties) "
            + "values (?,?,?,?,?,?,?,?,?)";

    try (Connection connection = databaseProvider.getDatasource().getConnection();
        PreparedStatement statement = connection.prepareStatement(statementSql)) {
      connection.setAutoCommit(false);

      for (Issue issue : issues) {
        statement.setString(1, contextId);
        statement.setString(2, issue.getCode());
        statement.setTimestamp(3, new Timestamp(issue.getTime().toInstant().toEpochMilli()));
        statement.setString(4, issue.getSeverity().toString());
        statement.setString(5, issue.getSummary());
        statement.setString(6, issue.getDetails());
        statement.setString(7, issue.getSourceClass());
        statement.setString(8, issue.getExceptionClass());

        String serializedProperties = null;
        if (issue.getProperties() != null) {
          serializedProperties = GsonUtils.GSON_WITH_DATE_HANDLING.toJson(issue.getProperties());
        }
        statement.setString(9, serializedProperties);

        statement.executeUpdate();
      }
      connection.commit();
    } catch (SQLException e) {
      throw new TroubleshooterException("Cannot save issue to the database", e);
    }
  }

  @Override
  public void remove(String contextId, String issueCode)
      throws TroubleshooterException {
    Objects.requireNonNull(contextId, "contextId should not be null");
    Objects.requireNonNull(issueCode, "issueCode should not be null");

    String statementSql = "delete from issues where context_id=? and code=?";
    try (Connection connection = databaseProvider.getDatasource().getConnection();
        PreparedStatement statement = connection.prepareStatement(statementSql)) {
      statement.setString(1, contextId);
      statement.setString(2, issueCode);

      statement.executeUpdate();
    } catch (SQLException e) {
      throw new TroubleshooterException("Cannot remove issue from the database", e);
    }
  }

  @Override
  protected void startUp()
      throws Exception {
    scheduledExecutor = Executors.newScheduledThreadPool(1);
    scheduledExecutor.scheduleAtFixedRate(this::cleanupOldIssues, configuration.cleanupInterval.toMillis(),
                                          configuration.cleanupInterval.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  protected void shutDown()
      throws Exception {
    scheduledExecutor.shutdown();
  }

  private void cleanupOldIssues() {
    try {
      deleteIssuesOlderThan(ZonedDateTime.now().minus(configuration.deleteIssuesOlderThan));
      deleteOldIssuesOverTheCount(configuration.maxIssuesToKeep);
    } catch (Exception ex) {
      log.warn("Failed to cleanup old issues", ex);
    }
  }

  @VisibleForTesting
  public void deleteIssuesOlderThan(ZonedDateTime olderThanDate)
      throws SQLException {

    try (Connection connection = databaseProvider.getDatasource().getConnection();
        PreparedStatement statement = connection.prepareStatement("delete from issues where time < ?")) {

      Instant deleteBefore = olderThanDate.withZoneSameInstant(ZoneOffset.UTC).toInstant();
      statement.setTimestamp(1, new Timestamp(deleteBefore.toEpochMilli()));

      Stopwatch stopwatch = Stopwatch.createStarted();
      int deletedIssues = statement.executeUpdate();
      log.info("Deleted {} issues that are older than {} in {} ms", deletedIssues, deleteBefore,
               stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }
  }

  @VisibleForTesting
  public void deleteOldIssuesOverTheCount(long maxIssuesToKeep)
      throws SQLException {

    try (Connection connection = databaseProvider.getDatasource().getConnection();
        PreparedStatement countQuery = connection.prepareStatement("select count(*) from issues");
        ResultSet resultSet = countQuery.executeQuery()) {

      resultSet.next();
      long totalIssueCount = resultSet.getLong(1);

      long issuesToRemove = totalIssueCount - maxIssuesToKeep;
      if (issuesToRemove <= 0) {
        return;
      }

      // position is a table-wide auto-increment field. older issues will have smaller position.
      try (PreparedStatement deleteStatement = connection
          .prepareStatement("delete from issues order by position limit ?")) {

        deleteStatement.setLong(1, issuesToRemove);

        Stopwatch stopwatch = Stopwatch.createStarted();
        int deletedIssues = deleteStatement.executeUpdate();
        log.info("Deleted {} issues to keep the total issue count under {} in {} ms", deletedIssues, maxIssuesToKeep,
                 stopwatch.elapsed(TimeUnit.MILLISECONDS));
      }
    }
  }

  @Builder
  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  public static class Configuration {
    @Builder.Default
    private Duration cleanupInterval = ServiceConfigKeys.DEFAULT_MYSQL_ISSUE_REPO_CLEANUP_INTERVAL;

    @Builder.Default
    private long maxIssuesToKeep = ServiceConfigKeys.DEFAULT_MYSQL_ISSUE_REPO_MAX_ISSUES_TO_KEEP;

    @Builder.Default
    private Duration deleteIssuesOlderThan = ServiceConfigKeys.DEFAULT_MYSQL_ISSUE_REPO_DELETE_ISSUES_OLDER_THAN;

    @Inject
    public Configuration(Config innerConfig) {
      this(); // see https://github.com/projectlombok/lombok/issues/1347

      if (innerConfig.hasPath(ServiceConfigKeys.MYSQL_ISSUE_REPO_CLEANUP_INTERVAL)) {
        cleanupInterval = innerConfig.getDuration(ServiceConfigKeys.MYSQL_ISSUE_REPO_CLEANUP_INTERVAL);
      }

      if (innerConfig.hasPath(ServiceConfigKeys.MYSQL_ISSUE_REPO_MAX_ISSUES_TO_KEEP)) {
        maxIssuesToKeep = innerConfig.getLong(ServiceConfigKeys.MYSQL_ISSUE_REPO_MAX_ISSUES_TO_KEEP);
      }

      if (innerConfig.hasPath(ServiceConfigKeys.MYSQL_ISSUE_REPO_DELETE_ISSUES_OLDER_THAN)) {
        deleteIssuesOlderThan = innerConfig.getDuration(ServiceConfigKeys.MYSQL_ISSUE_REPO_DELETE_ISSUES_OLDER_THAN);
      }
    }
  }
}
