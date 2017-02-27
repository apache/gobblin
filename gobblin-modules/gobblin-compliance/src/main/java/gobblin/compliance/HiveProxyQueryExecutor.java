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
package gobblin.compliance;

import java.io.Closeable;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.thrift.TException;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;
import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.HostUtils;


/**
 * This class is responsible for executing Hive queries using jdbc connection.
 * This class can execute queries as hadoop proxy users by first authenticating hadoop super user.
 *
 * @author adsharma
 */
@Slf4j
@SuppressWarnings
public class HiveProxyQueryExecutor implements QueryExecutor, Closeable {
  private static final String HIVE_SERVER2_PROXY_USER = "hive.server2.proxy.user=";
  private static final String HIVE_JDBC_URL = ComplianceConfigurationKeys.COMPLIANCE_PREFIX + ".hive.jdbc.url";
  private static final String DEFAULT = "default";
  private static final String HIVE_SETTINGS = ComplianceConfigurationKeys.COMPLIANCE_PREFIX + ".hive.settings";
  private static final Splitter SC_SPLITTER = Splitter.on(";").omitEmptyStrings().trimResults();
  private Map<String, HiveConnection> connectionMap = new HashMap<>();
  private Map<String, Statement> statementMap = new HashMap<>();
  private State state;
  private List<String> settings = new ArrayList<>();

  public HiveProxyQueryExecutor(State state, List<String> proxies)
      throws IOException {
    try {
      this.state = new State();
      this.state.addAll(state.getProperties());
      setHiveSettings(state);
      setProxiedConnection(proxies);
    } catch (InterruptedException | TException e) {
      throw new IOException(e);
    }
  }

  public HiveProxyQueryExecutor(State state)
      throws IOException {
    this.state = new State();
    this.state.addAll(state.getProperties());
    setHiveSettings(state);
    try {
      if (!this.state.getPropAsBoolean(ComplianceConfigurationKeys.GOBBLIN_COMPLIANCE_SHOULD_PROXY,
          ComplianceConfigurationKeys.GOBBLIN_COMPLIANCE_DEFAULT_SHOULD_PROXY)) {
        setConnection();
        return;
      }
      Preconditions.checkArgument(this.state.contains(ComplianceConfigurationKeys.GOBBLIN_COMPLIANCE_PROXY_USER),
          "Missing required property " + ComplianceConfigurationKeys.GOBBLIN_COMPLIANCE_PROXY_USER);
      Preconditions.checkArgument(this.state.contains(ComplianceConfigurationKeys.GOBBLIN_COMPLIANCE_SUPER_USER),
          "Missing required property " + ComplianceConfigurationKeys.GOBBLIN_COMPLIANCE_SUPER_USER);
      List<String> proxies = new ArrayList<>();
      proxies.add(this.state.getProp(ComplianceConfigurationKeys.GOBBLIN_COMPLIANCE_PROXY_USER));
      proxies.add(this.state.getProp(ComplianceConfigurationKeys.GOBBLIN_COMPLIANCE_SUPER_USER));
      setProxiedConnection(proxies);
    } catch (ClassNotFoundException | SQLException | InterruptedException | TException e) {
      throw new IOException(e);
    }
  }

  private synchronized void setProxiedConnection(final List<String> proxies)
      throws IOException, InterruptedException, TException {
    Preconditions.checkArgument(this.state.contains(ConfigurationKeys.SUPER_USER_KEY_TAB_LOCATION),
        "Missing required property " + ConfigurationKeys.SUPER_USER_KEY_TAB_LOCATION);
    String superUser = this.state.getProp(ComplianceConfigurationKeys.GOBBLIN_COMPLIANCE_SUPER_USER);
    String keytabLocation = this.state.getProp(ConfigurationKeys.SUPER_USER_KEY_TAB_LOCATION);
    String realm = this.state.getProp(ConfigurationKeys.KERBEROS_REALM);
    UserGroupInformation loginUser = UserGroupInformation
        .loginUserFromKeytabAndReturnUGI(HostUtils.getPrincipalUsingHostname(superUser, realm), keytabLocation);
    loginUser.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run()
          throws MetaException, SQLException, ClassNotFoundException {
        for (String proxy : proxies) {
          HiveConnection hiveConnection = getHiveConnection(proxy);
          Statement statement = hiveConnection.createStatement();
          statementMap.put(proxy, statement);
          connectionMap.put(proxy, hiveConnection);
          for (String setting : settings) {
            statement.execute(setting);
          }
        }
        return null;
      }
    });
  }

  private synchronized void setConnection()
      throws ClassNotFoundException, SQLException {
    HiveConnection hiveConnection = getHiveConnection();
    Statement statement = hiveConnection.createStatement();
    this.statementMap.put(DEFAULT, statement);
    this.connectionMap.put(DEFAULT, hiveConnection);
    for (String setting : settings) {
      statement.execute(setting);
    }
  }

  private HiveConnection getHiveConnection(String proxyUser)
      throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.hive.jdbc.HiveDriver");
    Preconditions.checkArgument(this.state.contains(HIVE_JDBC_URL), "Missing required property " + HIVE_JDBC_URL);
    String url = this.state.getProp(HIVE_JDBC_URL);
    url = url + HIVE_SERVER2_PROXY_USER + proxyUser;
    return (HiveConnection) DriverManager.getConnection(url);
  }

  private HiveConnection getHiveConnection()
      throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.hive.jdbc.HiveDriver");
    Preconditions.checkArgument(this.state.contains(HIVE_JDBC_URL), "Missing required property " + HIVE_JDBC_URL);
    String url = this.state.getProp(HIVE_JDBC_URL);
    return (HiveConnection) DriverManager.getConnection(url);
  }

  @Override
  public void executeQueries(List<String> queries)
      throws SQLException {
    Optional<String> proxy = Optional.absent();
    executeQueries(queries, proxy);
  }

  @Override
  public void executeQuery(String query)
      throws SQLException {
    Optional<String> proxy = Optional.absent();
    executeQuery(query, proxy);
  }

  public void executeQueries(List<String> queries, Optional<String> proxy)
      throws SQLException {
    Preconditions.checkArgument(!this.statementMap.isEmpty(), "No hive connection. Unable to execute queries");
    if (!proxy.isPresent()) {
      Preconditions.checkArgument(this.statementMap.size() == 1, "Multiple Hive connections. Please specify a user");
      proxy = Optional.fromNullable(this.statementMap.keySet().iterator().next());
    }
    Statement statement = this.statementMap.get(proxy.get());
    for (String query : queries) {
      statement.execute(query);
    }
  }

  public void executeQuery(String query, Optional<String> proxy)
      throws SQLException {
    Preconditions.checkArgument(!this.statementMap.isEmpty(), "No hive connection. Unable to execute queries");
    if (!proxy.isPresent()) {
      Preconditions.checkArgument(this.statementMap.size() == 1, "Multiple Hive connections. Please specify a user");
      proxy = Optional.fromNullable(this.statementMap.keySet().iterator().next());
    }
    Statement statement = this.statementMap.get(proxy.get());
    for (String setting : this.settings) {
      statement.execute(setting);
    }
    statement.execute(query);
  }

  @Override
  public void close()
      throws IOException {
    try {
      for (Map.Entry<String, Statement> entry : this.statementMap.entrySet()) {
        if (entry.getValue() != null) {
          entry.getValue().close();
        }
      }
      for (Map.Entry<String, HiveConnection> entry : this.connectionMap.entrySet()) {
        if (entry.getValue() != null) {
          entry.getValue().close();
        }
      }
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  private void setHiveSettings(State state) {
    if (state.contains(HIVE_SETTINGS)) {
      String queryString = state.getProp(HIVE_SETTINGS);
      this.settings = SC_SPLITTER.splitToList(queryString);
    }
  }
}
