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

package org.apache.gobblin.runtime.spec_store;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.typesafe.config.Config;

import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.MysqlDataSourceFactory;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.InstrumentedSpecStore;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.api.SpecSerDe;
import org.apache.gobblin.runtime.api.SpecSerDeException;
import org.apache.gobblin.runtime.api.SpecStore;
import org.apache.gobblin.util.ConfigUtils;

import static org.apache.gobblin.service.ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY;
import static org.apache.gobblin.service.ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY;


/**
 * Implementation of {@link SpecStore} that stores specs as serialized java objects in MySQL. Note that versions are not
 * supported, so the version parameter will be ignored in methods that have it.
 *
 * A tag column is added into implementation to serve certain filtering purposes in MySQL-based SpecStore.
 * For example, in DR mode of GaaS, we would only want certain {@link Spec}s to be eligible for orchestrated
 * by alternative GaaS instances. Another example is whitelisting/blacklisting {@link Spec}s temporarily
 * but not removing it from {@link SpecStore}.
 */
@Slf4j
public class MysqlSpecStore extends InstrumentedSpecStore {
  public static final String CONFIG_PREFIX = "mysqlSpecStore";
  public static final String DEFAULT_TAG_VALUE = "";
  private static final String NEW_COLUMN = "spec_json";

  private static final String CREATE_TABLE_STATEMENT =
      "CREATE TABLE IF NOT EXISTS %s (spec_uri VARCHAR(128) NOT NULL, flow_group VARCHAR(128), flow_name VARCHAR(128), "
          + "template_uri VARCHAR(128), user_to_proxy VARCHAR(128), source_identifier VARCHAR(128), "
          + "destination_identifier VARCHAR(128), schedule VARCHAR(128), tag VARCHAR(128) NOT NULL, "
          + "modified_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, "
          + "isRunImmediately BOOLEAN, timezone VARCHAR(128), owning_group VARCHAR(128), "
          + "spec LONGBLOB, " + NEW_COLUMN + " JSON, PRIMARY KEY (spec_uri))";
  private static final String EXISTS_STATEMENT = "SELECT EXISTS(SELECT * FROM %s WHERE spec_uri = ?)";
  protected static final String INSERT_STATEMENT = "INSERT INTO %s (spec_uri, flow_group, flow_name, template_uri, "
      + "user_to_proxy, source_identifier, destination_identifier, schedule, tag, isRunImmediately, spec, " + NEW_COLUMN + ") "
      + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE spec = VALUES(spec), " + NEW_COLUMN + " = VALUES(" + NEW_COLUMN + ")";
  private static final String DELETE_STATEMENT = "DELETE FROM %s WHERE spec_uri = ?";
  private static final String GET_STATEMENT = "SELECT spec, " + NEW_COLUMN + " FROM %s WHERE spec_uri = ?";
  private static final String GET_ALL_STATEMENT = "SELECT spec_uri, spec, " + NEW_COLUMN + " FROM %s";
  private static final String GET_ALL_URIS_STATEMENT = "SELECT spec_uri FROM %s";
  private static final String GET_ALL_STATEMENT_WITH_TAG = "SELECT spec_uri FROM %s WHERE tag = ?";

  protected final DataSource dataSource;
  protected final String tableName;
  private final URI specStoreURI;
  protected final SpecSerDe specSerDe;

  public MysqlSpecStore(Config config, SpecSerDe specSerDe) throws IOException {
    super(config, specSerDe);
    if (config.hasPath(CONFIG_PREFIX)) {
      config = config.getConfig(CONFIG_PREFIX).withFallback(config);
    }

    this.dataSource = MysqlDataSourceFactory.get(config, SharedResourcesBrokerFactory.getImplicitBroker());
    this.tableName = config.getString(ConfigurationKeys.STATE_STORE_DB_TABLE_KEY);
    this.specStoreURI = URI.create(config.getString(ConfigurationKeys.STATE_STORE_DB_URL_KEY));
    this.specSerDe = specSerDe;

    try (Connection connection = this.dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(String.format(CREATE_TABLE_STATEMENT, this.tableName))) {
      statement.executeUpdate();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean existsImpl(URI specUri) throws IOException {
    try (Connection connection = this.dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(String.format(EXISTS_STATEMENT, this.tableName))) {
      statement.setString(1, specUri.toString());
      try (ResultSet rs = statement.executeQuery()) {
        rs.next();
        return rs.getBoolean(1);
      }
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void addSpecImpl(Spec spec) throws IOException {
    this.addSpec(spec, DEFAULT_TAG_VALUE);
  }

  /**
   * Temporarily only used for testing since tag it not exposed in endpoint of {@link org.apache.gobblin.runtime.api.FlowSpec}
   */
  public void addSpec(Spec spec, String tagValue) throws IOException {
    try (Connection connection = this.dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(String.format(INSERT_STATEMENT, this.tableName))) {
      setPreparedStatement(statement, spec, tagValue);
      statement.executeUpdate();
      connection.commit();
    } catch (SQLException | SpecSerDeException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean deleteSpec(Spec spec) throws IOException {
    return deleteSpec(spec.getUri());
  }

  @Override
  public boolean deleteSpecImpl(URI specUri) throws IOException {
    try (Connection connection = this.dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(String.format(DELETE_STATEMENT, this.tableName))) {
      statement.setString(1, specUri.toString());
      int result = statement.executeUpdate();
      connection.commit();
      return result != 0;
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean deleteSpec(URI specUri, String version) throws IOException {
    return deleteSpec(specUri);
  }

  @Override
  // TODO : this method is not doing what the contract is in the SpecStore interface
  public Spec updateSpecImpl(Spec spec) throws IOException {
    addSpec(spec);
    return spec;
  }

  @Override
  public Spec getSpecImpl(URI specUri) throws IOException, SpecNotFoundException {
    try (Connection connection = this.dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(String.format(GET_STATEMENT, this.tableName))) {
      statement.setString(1, specUri.toString());

      try (ResultSet rs = statement.executeQuery()) {
        if (!rs.next()) {
          throw new SpecNotFoundException(specUri);
        }
        return rs.getString(2) == null
            ? this.specSerDe.deserialize(ByteStreams.toByteArray(rs.getBlob(1).getBinaryStream()))
            : this.specSerDe.deserialize(rs.getString(2).getBytes(Charsets.UTF_8));
      }
    } catch (SQLException | SpecSerDeException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Spec getSpec(URI specUri, String version) throws IOException, SpecNotFoundException {
    return getSpec(specUri);
  }

  @Override
  public Collection<Spec> getAllVersionsOfSpec(URI specUri) throws IOException, SpecNotFoundException {
    return Lists.newArrayList(getSpec(specUri));
  }

  @Override
  public Collection<Spec> getSpecsImpl() throws IOException {
    try (Connection connection = this.dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(String.format(GET_ALL_STATEMENT, this.tableName))) {
      List<Spec> specs = new ArrayList<>();

      try (ResultSet rs = statement.executeQuery()) {
        while (rs.next()) {
          try {
            specs.add(
                rs.getString(3) == null
                    ? this.specSerDe.deserialize(rs.getString(2).getBytes(Charsets.UTF_8))
                    : this.specSerDe.deserialize(ByteStreams.toByteArray(rs.getBlob(3).getBinaryStream()))
            );
          } catch (SQLException | SpecSerDeException e) {
            log.error("Failed to deserialize spec", e);
          }
        }
      }
      return specs;
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Iterator<URI> getSpecURIsImpl() throws IOException {
    try (Connection connection = this.dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(String.format(GET_ALL_URIS_STATEMENT, this.tableName))) {
      return getURIIteratorByQuery(statement);
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Iterator<URI> getSpecURIsWithTag(String tag) throws IOException {
    try (Connection connection = this.dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(String.format(GET_ALL_STATEMENT_WITH_TAG, this.tableName))) {
      statement.setString(1, tag);
      return getURIIteratorByQuery(statement);
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  private Iterator<URI> getURIIteratorByQuery(PreparedStatement statement) throws SQLException {
    List<URI> specs = new ArrayList<>();

    try (ResultSet rs = statement.executeQuery()) {
      while (rs.next()) {
        URI specURI = URI.create(rs.getString(1));
        specs.add(specURI);
      }
    }

    return specs.iterator();
  }

  @Override
  public Optional<URI> getSpecStoreURI() {
    return Optional.of(this.specStoreURI);
  }

  protected void setPreparedStatement(PreparedStatement statement, Spec spec, String tagValue) throws SQLException {
    FlowSpec flowSpec = (FlowSpec) spec;
    URI specUri = flowSpec.getUri();
    Config flowConfig = flowSpec.getConfig();
    String flowGroup = flowConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY);
    String flowName = flowConfig.getString(ConfigurationKeys.FLOW_NAME_KEY);
    String templateURI = new Gson().toJson(flowSpec.getTemplateURIs());
    String userToProxy = ConfigUtils.getString(flowSpec.getConfig(), "user.to.proxy", null);
    String sourceIdentifier = flowConfig.getString(FLOW_SOURCE_IDENTIFIER_KEY);
    String destinationIdentifier = flowConfig.getString(FLOW_DESTINATION_IDENTIFIER_KEY);
    String schedule = ConfigUtils.getString(flowConfig, ConfigurationKeys.JOB_SCHEDULE_KEY, null);
    boolean isRunImmediately = ConfigUtils.getBoolean(flowConfig, ConfigurationKeys.FLOW_RUN_IMMEDIATELY, false);

    statement.setString(1, specUri.toString());
    statement.setString(2, flowGroup);
    statement.setString(3, flowName);
    statement.setString(4, templateURI);
    statement.setString(5, userToProxy);
    statement.setString(6, sourceIdentifier);
    statement.setString(7, destinationIdentifier);
    statement.setString(8, schedule);
    statement.setString(9, tagValue);
    statement.setBoolean(10, isRunImmediately);
    statement.setBlob(11, new ByteArrayInputStream(this.specSerDe.serialize(flowSpec)));
    statement.setString(12, new String(this.specSerDe.serialize(flowSpec), Charsets.UTF_8));
  }
}
