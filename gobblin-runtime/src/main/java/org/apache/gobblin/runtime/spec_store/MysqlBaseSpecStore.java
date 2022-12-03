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
import javax.sql.DataSource;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariDataSource;

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


/**
 * Implementation of {@link SpecStore} that stores specs in MySQL as a serialized BLOB, per the provided {@link SpecSerDe}.
 * Note: versions are unsupported, so the version parameter is ignored in methods that have it.
 *
 * A tag column is added into implementation to serve certain filtering purposes in MySQL-based SpecStore.
 * For example, in DR mode of GaaS, we would only want certain {@link Spec}s to be eligible for orchestrated
 * by alternative GaaS instances. Another example is allow-listing/deny-listing {@link Spec}s temporarily
 * but not removing it from {@link SpecStore}.
 *
 * The {@link MysqlSpecStore} is a specialization enhanced for {@link FlowSpec} search and retrieval.
 */
@Slf4j
public class MysqlBaseSpecStore extends InstrumentedSpecStore {

  /** `j.u.Function` variant for an operation that may @throw IOException or SQLException: preserves method signature checked exceptions */
  @FunctionalInterface
  protected interface CheckedFunction<T, R> {
    R apply(T t) throws IOException, SQLException;
  }

  public static final String CONFIG_PREFIX = "mysqlBaseSpecStore";
  public static final String DEFAULT_TAG_VALUE = "";

  private static final String EXISTS_STATEMENT = "SELECT EXISTS(SELECT * FROM %s WHERE spec_uri = ?)";
  protected static final String INSERT_STATEMENT = "INSERT INTO %s (spec_uri, tag, spec) "
      + "VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE spec = VALUES(spec)";
  // Keep previous syntax that update only update spec and spec_json
  //todo: do we need change this behavior so that everything can be updated
  protected static final String UPDATE_STATEMENT = "UPDATE %s SET spec=?,spec_json=? WHERE spec_uri=? AND UNIX_TIMESTAMP(modified_time) < ?";
  private static final String DELETE_STATEMENT = "DELETE FROM %s WHERE spec_uri = ?";
  private static final String GET_STATEMENT_BASE = "SELECT spec_uri, spec FROM %s WHERE ";
  private static final String GET_ALL_STATEMENT = "SELECT spec_uri, spec FROM %s";
  private static final String GET_ALL_URIS_STATEMENT = "SELECT spec_uri FROM %s";
  private static final String GET_ALL_URIS_WITH_TAG_STATEMENT = "SELECT spec_uri FROM %s WHERE tag = ?";
  private static final String GET_SIZE_STATEMENT = "SELECT COUNT(*) FROM %s ";
  // NOTE: using max length of a `FlowSpec` URI, as it's believed to be the longest of existing `Spec` types
  private static final String CREATE_TABLE_STATEMENT = "CREATE TABLE IF NOT EXISTS %s (spec_uri VARCHAR(" + FlowSpec.Utils.maxFlowSpecUriLength()
        + ") NOT NULL, tag VARCHAR(128) NOT NULL, modified_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE "
        + "CURRENT_TIMESTAMP, spec LONGBLOB, PRIMARY KEY (spec_uri))";

  /**
   * The main point of difference with base classes is the DB schema and hence certain of the DML and queries.  Given the interrelation
   * between statements, collect them within this inner class that enables selective, per-statement override, and delivers them as a unit.
   */
  protected class SqlStatements {
    public final String updateStatement = String.format(getTablelessUpdateStatement(), MysqlBaseSpecStore.this.tableName);
    public final String existsStatement = String.format(getTablelessExistsStatement(), MysqlBaseSpecStore.this.tableName);
    public final String insertStatement = String.format(getTablelessInsertStatement(), MysqlBaseSpecStore.this.tableName);
    public final String deleteStatement = String.format(getTablelessDeleteStatement(), MysqlBaseSpecStore.this.tableName);
    public final String getStatementBase = String.format(getTablelessGetStatementBase(), MysqlBaseSpecStore.this.tableName);
    public final String getAllStatement = String.format(getTablelessGetAllStatement(), MysqlBaseSpecStore.this.tableName);
    public final String getAllURIsStatement = String.format(getTablelessGetAllURIsStatement(), MysqlBaseSpecStore.this.tableName);
    public final String getAllURIsWithTagStatement = String.format(getTablelessGetAllURIsWithTagStatement(), MysqlBaseSpecStore.this.tableName);
    public final String getSizeStatement = String.format(getTablelessGetSizeStatement(), MysqlBaseSpecStore.this.tableName);
    public final String createTableStatement = String.format(getTablelessCreateTableStatement(), MysqlBaseSpecStore.this.tableName);

    public void completeInsertPreparedStatement(PreparedStatement statement, Spec spec, String tagValue) throws SQLException {
      URI specUri = spec.getUri();

      int i = 0;
      statement.setString(++i, specUri.toString());
      statement.setString(++i, tagValue);
      statement.setBlob(++i, new ByteArrayInputStream(MysqlBaseSpecStore.this.specSerDe.serialize(spec)));
    }

    public Spec extractSpec(ResultSet rs) throws SQLException, IOException {
      return MysqlBaseSpecStore.this.specSerDe.deserialize(ByteStreams.toByteArray(rs.getBlob(2).getBinaryStream()));
    }

    protected String getTablelessExistsStatement() { return MysqlBaseSpecStore.EXISTS_STATEMENT; }
    protected String getTablelessUpdateStatement() { return MysqlBaseSpecStore.UPDATE_STATEMENT; }
    protected String getTablelessInsertStatement() { return MysqlBaseSpecStore.INSERT_STATEMENT; }
    protected String getTablelessDeleteStatement() { return MysqlBaseSpecStore.DELETE_STATEMENT; }
    protected String getTablelessGetStatementBase() { return MysqlBaseSpecStore.GET_STATEMENT_BASE; }
    protected String getTablelessGetAllStatement() { return MysqlBaseSpecStore.GET_ALL_STATEMENT; }
    protected String getTablelessGetAllURIsStatement() { return MysqlBaseSpecStore.GET_ALL_URIS_STATEMENT; }
    protected String getTablelessGetAllURIsWithTagStatement() { return MysqlBaseSpecStore.GET_ALL_URIS_WITH_TAG_STATEMENT; }
    protected String getTablelessGetSizeStatement() { return MysqlBaseSpecStore.GET_SIZE_STATEMENT; }
    protected String getTablelessCreateTableStatement() { return MysqlBaseSpecStore.CREATE_TABLE_STATEMENT; }
  }


  protected final DataSource dataSource;
  protected final String tableName;
  private final URI specStoreURI;
  protected final SpecSerDe specSerDe;
  protected final SqlStatements sqlStatements;

  public MysqlBaseSpecStore(Config config, SpecSerDe specSerDe) throws IOException {
    super(config, specSerDe);
    String configPrefix = getConfigPrefix();
    if (config.hasPath(configPrefix)) {
      config = config.getConfig(configPrefix).withFallback(config);
    }

    this.dataSource = MysqlDataSourceFactory.get(config, SharedResourcesBrokerFactory.getImplicitBroker());
    this.tableName = config.getString(ConfigurationKeys.STATE_STORE_DB_TABLE_KEY);
    this.specStoreURI = URI.create(config.getString(ConfigurationKeys.STATE_STORE_DB_URL_KEY));
    this.specSerDe = specSerDe;
    this.sqlStatements = createSqlStatements();

    withPreparedStatement(this.sqlStatements.createTableStatement, statement ->
      statement.executeUpdate()
    );
  }

  protected String getConfigPrefix() {
    return MysqlBaseSpecStore.CONFIG_PREFIX;
  }

  protected SqlStatements createSqlStatements() {
    return new SqlStatements();
  }

  @Override
  public boolean existsImpl(URI specUri) throws IOException {
    return withPreparedStatement(this.sqlStatements.existsStatement, statement -> {
      statement.setString(1, specUri.toString());
      try (ResultSet rs = statement.executeQuery()) {
        rs.next();
        return rs.getBoolean(1);
      }
    });
  }

  @Override
  public void addSpecImpl(Spec spec) throws IOException {
    this.addSpec(spec, DEFAULT_TAG_VALUE);
  }

  /**
   * Temporarily only used for testing since tag it not exposed in endpoint of {@link org.apache.gobblin.runtime.api.FlowSpec}
   */
  public void addSpec(Spec spec, String tagValue) throws IOException {
    withPreparedStatement(this.sqlStatements.insertStatement, statement -> {
      this.sqlStatements.completeInsertPreparedStatement(statement, spec, tagValue);
      statement.executeUpdate();
      return null; // (type: `Void`)
    }, true);
  }

  @Override
  public boolean deleteSpec(Spec spec) throws IOException {
    return deleteSpec(spec.getUri());
  }

  @Override
  public boolean deleteSpecImpl(URI specUri) throws IOException {
    return withPreparedStatement(this.sqlStatements.deleteStatement, statement -> {
      statement.setString(1, specUri.toString());
      int result = statement.executeUpdate();
      return result != 0;
    }, true);
  }

  @Override
  public boolean deleteSpec(URI specUri, String version) throws IOException {
    return deleteSpec(specUri);
  }

  @Override
  // TODO: fix to obey the `SpecStore` contract of returning the *updated* `Spec`
  public Spec updateSpecImpl(Spec spec) throws IOException {
    addSpec(spec);
    return spec;
  }

  @Override
  public Spec getSpecImpl(URI specUri) throws IOException, SpecNotFoundException {
    Iterator<Spec> resultSpecs = withPreparedStatement(this.sqlStatements.getAllStatement + " WHERE spec_uri = ?", statement -> {
      statement.setString(1, specUri.toString());
      return retrieveSpecs(statement).iterator();
    });
    if (resultSpecs.hasNext()) {
      return resultSpecs.next();
    } else {
      throw new SpecNotFoundException(specUri);
    }
  }

  @Override
  public Spec getSpec(URI specUri, String version) throws IOException, SpecNotFoundException {
    return getSpec(specUri); // `version` ignored, as mentioned in javadoc
  }

  @Override
  public Collection<Spec> getAllVersionsOfSpec(URI specUri) throws IOException, SpecNotFoundException {
    return Lists.newArrayList(getSpec(specUri));
  }

  @Override
  public Collection<Spec> getSpecsImpl() throws IOException {
    return withPreparedStatement(this.sqlStatements.getAllStatement, statement -> {
      return retrieveSpecs(statement);
    });
  }

  protected final Collection<Spec> retrieveSpecs(PreparedStatement statement) throws IOException {
    List<Spec> specs = new ArrayList<>();
    try (ResultSet rs = statement.executeQuery()) {
      while (rs.next()) {
        specs.add(this.sqlStatements.extractSpec(rs));
      }
    } catch (SQLException | SpecSerDeException e) {
      log.error("Failed to deserialize spec", e);
      throw new IOException(e);
    }
    return specs;
  }

  @Override
  public Iterator<URI> getSpecURIsImpl() throws IOException {
    return withPreparedStatement(this.sqlStatements.getAllURIsStatement, statement -> {
      return retreiveURIs(statement).iterator();
    });
  }

  @Override
  public int getSizeImpl() throws IOException {
    return withPreparedStatement(this.sqlStatements.getSizeStatement, statement -> {
      try (ResultSet resultSet = statement.executeQuery()) {
        resultSet.next();
        return resultSet.getInt(1);
      }
    });
  }

  @Override
  public Collection<Spec> getSpecsImpl(int start, int count) throws IOException {
    List<String> limitAndOffset = new ArrayList<>();
    if (count > 0) {
      // Order by two fields to make a full order by
      limitAndOffset.add(" ORDER BY modified_time DESC, spec_uri ASC LIMIT");
      limitAndOffset.add(String.valueOf(count));
      if (start > 0) {
        limitAndOffset.add("OFFSET");
        limitAndOffset.add(String.valueOf(start));
      }
    }
    String finalizedStatement = this.sqlStatements.getAllStatement + String.join(" ", limitAndOffset);
    return withPreparedStatement(finalizedStatement, statement -> {
      return retrieveSpecs(statement);
    });
  }

  @Override
  public Iterator<URI> getSpecURIsWithTagImpl(String tag) throws IOException {
    return withPreparedStatement(this.sqlStatements.getAllURIsWithTagStatement, statement -> {
      statement.setString(1, tag);
      return retreiveURIs(statement).iterator();
    });
  }

  private List<URI> retreiveURIs(PreparedStatement statement) throws SQLException {
    List<URI> uris = new ArrayList<>();

    try (ResultSet rs = statement.executeQuery()) {
      while (rs.next()) {
        URI specURI = URI.create(rs.getString(1));
        uris.add(specURI);
      }
    }

    return uris;
  }

  @Override
  public Optional<URI> getSpecStoreURI() {
    return Optional.of(this.specStoreURI);
  }

  /** Abstracts recurring pattern around resource management and exception re-mapping. */
  protected <T> T withPreparedStatement(String sql, CheckedFunction<PreparedStatement, T> f, boolean shouldCommit) throws IOException {
    try (Connection connection = this.dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      T result = f.apply(statement);
      if (shouldCommit) {
        connection.commit();
      }
      return result;
    } catch (SQLException e) {
      // TODO: revisit use of connection test query following verification of successful connection pool migration:
      //   If your driver supports JDBC4 we strongly recommend not setting this property. This is for "legacy" drivers
      //   that do not support the JDBC4 Connection.isValid() API; see:
      //   https://github.com/brettwooldridge/HikariCP#gear-configuration-knobs-baby
      log.warn("Received SQL exception that can result from invalid connection. Checking if validation query is set {} Exception is {}", ((HikariDataSource) this.dataSource).getConnectionTestQuery(), e);
      throw new IOException(e);
    }
  }

  /** Abstracts recurring pattern, while not presuming to DB `commit()`. */
  protected final <T> T withPreparedStatement(String sql, CheckedFunction<PreparedStatement, T> f) throws IOException {
    return withPreparedStatement(sql, f, false);
  }
}
