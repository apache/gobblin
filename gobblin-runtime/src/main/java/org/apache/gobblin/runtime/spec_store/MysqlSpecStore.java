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
import com.google.common.base.Splitter;
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
import org.apache.gobblin.runtime.api.FlowSpecSearchObject;
import org.apache.gobblin.runtime.api.InstrumentedSpecStore;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.api.SpecSearchObject;
import org.apache.gobblin.runtime.api.SpecSerDe;
import org.apache.gobblin.runtime.api.SpecSerDeException;
import org.apache.gobblin.runtime.api.SpecStore;
import org.apache.gobblin.service.ServiceConfigKeys;
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
// todo : This should be renamed to MysqlFlowSpecStore, because this implementation only stores FlowSpec, not a TopologySpec
public class MysqlSpecStore extends InstrumentedSpecStore {
  public static final String CONFIG_PREFIX = "mysqlSpecStore";
  public static final String DEFAULT_TAG_VALUE = "";
  private static final String NEW_COLUMN = "spec_json";

  private static final String EXISTS_STATEMENT = "SELECT EXISTS(SELECT * FROM %s WHERE spec_uri = ?)";
  protected static final String INSERT_STATEMENT = "INSERT INTO %s (spec_uri, flow_group, flow_name, template_uri, "
      + "user_to_proxy, source_identifier, destination_identifier, schedule, tag, isRunImmediately, owning_group, spec, " + NEW_COLUMN + ") "
      + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE spec = VALUES(spec), " + NEW_COLUMN + " = VALUES(" + NEW_COLUMN + ")";
  private static final String DELETE_STATEMENT = "DELETE FROM %s WHERE spec_uri = ?";
  private static final String GET_STATEMENT = "SELECT spec_uri, spec, " + NEW_COLUMN + " FROM %s WHERE ";
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
        PreparedStatement statement = connection.prepareStatement(getCreateJobStateTableTemplate())) {
      statement.executeUpdate();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  protected String getCreateJobStateTableTemplate() {
    return "CREATE TABLE IF NOT EXISTS " + this.tableName + " (spec_uri VARCHAR(" + FlowSpec.Utils.maxFlowSpecUri()
        + ") NOT NULL, flow_group VARCHAR(" + ServiceConfigKeys.MAX_FLOW_GROUP + "), flow_name VARCHAR("
        + ServiceConfigKeys.MAX_FLOW_GROUP + "), " + "template_uri VARCHAR(128), user_to_proxy VARCHAR(128), "
        + "source_identifier VARCHAR(128), " + "destination_identifier VARCHAR(128), schedule VARCHAR(128), "
        + "tag VARCHAR(128) NOT NULL, modified_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE "
        + "CURRENT_TIMESTAMP, isRunImmediately BOOLEAN, timezone VARCHAR(128), owning_group VARCHAR(128), spec LONGBLOB, "
        + NEW_COLUMN + " JSON, PRIMARY KEY (spec_uri))";
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
      setAddPreparedStatement(statement, spec, tagValue);
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
    Iterator<Spec> specsIterator = getSpecsImpl(FlowSpecSearchObject.builder().flowSpecUri(specUri).build()).iterator();
    if (specsIterator.hasNext()) {
      return specsIterator.next();
    } else {
      throw new SpecNotFoundException(specUri);
    }
  }

  public Collection<Spec> getSpecsImpl(SpecSearchObject specSearchObject) throws IOException {
    FlowSpecSearchObject flowSpecSearchObject = (FlowSpecSearchObject) specSearchObject;

    try (Connection connection = this.dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(createGetPreparedStatement(flowSpecSearchObject, this.tableName))) {
      setGetPreparedStatement(statement, flowSpecSearchObject);
      return getSpecsInternal(statement);
    } catch (SQLException e) {
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
      return getSpecsInternal(statement);
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  private Collection<Spec> getSpecsInternal(PreparedStatement statement) throws IOException {
    List<Spec> specs = new ArrayList<>();
    try (ResultSet rs = statement.executeQuery()) {
      while (rs.next()) {
        specs.add(
            rs.getString(3) == null
                ? this.specSerDe.deserialize(ByteStreams.toByteArray(rs.getBlob(2).getBinaryStream()))
                : this.specSerDe.deserialize(rs.getString(2).getBytes(Charsets.UTF_8))
        );
      }
    } catch (SQLException | SpecSerDeException e) {
      log.error("Failed to deserialize spec", e);
      throw new IOException(e);
    }
    return specs;
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

  /** This expects at least one parameter in {@link FlowSpecSearchObject} to be not null */
  static String createGetPreparedStatement(FlowSpecSearchObject flowSpecSearchObject, String tableName)
      throws IOException {
    String baseStatement = String.format(GET_STATEMENT, tableName);
    List<String> conditions = new ArrayList<>();

    if (flowSpecSearchObject.getFlowSpecUri() != null) {
      conditions.add("spec_uri = ?");
    }

    if (flowSpecSearchObject.getFlowGroup() != null) {
      conditions.add("flow_group = ?");
    }

    if (flowSpecSearchObject.getFlowName() != null) {
      conditions.add("flow_name = ?");
    }

    if (flowSpecSearchObject.getTemplateURI() != null) {
      conditions.add("template_uri = ?");
    }

    if (flowSpecSearchObject.getUserToProxy() != null) {
      conditions.add("user_to_proxy = ?");
    }

    if (flowSpecSearchObject.getSourceIdentifier() != null) {
      conditions.add("source_identifier = ?");
    }

    if (flowSpecSearchObject.getDestinationIdentifier() != null) {
      conditions.add("destination_identifier = ?");
    }

    if (flowSpecSearchObject.getSchedule() != null) {
      conditions.add("schedule = ?");
    }

    if (flowSpecSearchObject.getModifiedTimestamp() != null) {
      conditions.add("modified_time = ?");
    }

    if (flowSpecSearchObject.getIsRunImmediately() != null) {
      conditions.add("isRunImmediately = ?");
    }

    if (flowSpecSearchObject.getOwningGroup() != null) {
      conditions.add("owning_group = ?");
    }

    // If the propertyFilter is myKey=myValue, it looks for a config where key is `myKey` and value contains string `myValue`.
    // If the propertyFilter string does not have `=`, it considers the string as a key and just looks for its existence.
    // Multiple occurrences of `=` in  propertyFilter are not supported and ignored completely.
    if (flowSpecSearchObject.getPropertyFilter() != null) {
      String propertyFilter = flowSpecSearchObject.getPropertyFilter();
      Splitter commaSplitter = Splitter.on(",").trimResults().omitEmptyStrings();
      for (String property : commaSplitter.splitToList(propertyFilter)) {
        if (property.contains("=")) {
          String[] keyValue = property.split("=");
          if (keyValue.length != 2) {
            log.error("Incorrect flow config search query");
            continue;
          }
          conditions.add("spec_json->'$.configAsProperties.\"" + keyValue[0] + "\"' like " + "'%" + keyValue[1] + "%'");
        } else {
          conditions.add("spec_json->'$.configAsProperties.\"" + property + "\"' is not null");
        }
      }
    }

    if (conditions.size() == 0) {
      throw new IOException("At least one condition is required to query flow configs.");
    }

    return baseStatement + String.join(" AND ", conditions);
  }

  private static void setGetPreparedStatement(PreparedStatement statement, FlowSpecSearchObject flowSpecSearchObject)
      throws SQLException {
    int i = 0;

    if (flowSpecSearchObject.getFlowSpecUri() != null) {
      statement.setString(++i, flowSpecSearchObject.getFlowSpecUri().toString());
    }

    if (flowSpecSearchObject.getFlowGroup() != null) {
      statement.setString(++i, flowSpecSearchObject.getFlowGroup());
    }

    if (flowSpecSearchObject.getFlowName() != null) {
      statement.setString(++i, flowSpecSearchObject.getFlowName());
    }

    if (flowSpecSearchObject.getTemplateURI() != null) {
      statement.setString(++i, flowSpecSearchObject.getTemplateURI());
    }

    if (flowSpecSearchObject.getUserToProxy() != null) {
      statement.setString(++i, flowSpecSearchObject.getUserToProxy());
    }

    if (flowSpecSearchObject.getSourceIdentifier() != null) {
      statement.setString(++i, flowSpecSearchObject.getSourceIdentifier());
    }

    if (flowSpecSearchObject.getDestinationIdentifier() != null) {
      statement.setString(++i, flowSpecSearchObject.getDestinationIdentifier());
    }

    if (flowSpecSearchObject.getSchedule() != null) {
      statement.setString(++i, flowSpecSearchObject.getModifiedTimestamp());
    }

    if (flowSpecSearchObject.getIsRunImmediately() != null) {
      statement.setBoolean(++i, flowSpecSearchObject.getIsRunImmediately());
    }

    if (flowSpecSearchObject.getOwningGroup() != null) {
      statement.setString(++i, flowSpecSearchObject.getOwningGroup());
    }
  }

  protected void setAddPreparedStatement(PreparedStatement statement, Spec spec, String tagValue) throws SQLException {
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
    String owningGroup = ConfigUtils.getString(flowConfig, ConfigurationKeys.FLOW_OWNING_GROUP_KEY, null);
    boolean isRunImmediately = ConfigUtils.getBoolean(flowConfig, ConfigurationKeys.FLOW_RUN_IMMEDIATELY, false);

    int i = 0;
    statement.setString(++i, specUri.toString());
    statement.setString(++i, flowGroup);
    statement.setString(++i, flowName);
    statement.setString(++i, templateURI);
    statement.setString(++i, userToProxy);
    statement.setString(++i, sourceIdentifier);
    statement.setString(++i, destinationIdentifier);
    statement.setString(++i, schedule);
    statement.setString(++i, tagValue);
    statement.setBoolean(++i, isRunImmediately);
    statement.setString(++i, owningGroup);
    statement.setBlob(++i, new ByteArrayInputStream(this.specSerDe.serialize(flowSpec)));
    statement.setString(++i, new String(this.specSerDe.serialize(flowSpec), Charsets.UTF_8));
  }
}
