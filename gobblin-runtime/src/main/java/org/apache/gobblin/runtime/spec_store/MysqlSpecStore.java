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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import javax.sql.DataSource;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.MysqlDataSourceFactory;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.api.SpecStore;


/**
 * Implementation of {@link SpecStore} that stores specs as serialized java objects in MySQL. Note that versions are not
 * supported, so the version parameter will be ignored in methods that have it.
 */
public class MysqlSpecStore implements SpecStore {
  private static final String CREATE_TABLE_STATEMENT =
      "CREATE TABLE IF NOT EXISTS %s (spec_uri VARCHAR(128) NOT NULL, spec LONGBLOB, PRIMARY KEY (spec_uri))";
  private static final String EXISTS_STATEMENT = "SELECT EXISTS(SELECT * FROM %s WHERE spec_uri = ?)";
  private static final String INSERT_STATEMENT = "INSERT INTO %s (spec_uri, spec) VALUES (?, ?) ON DUPLICATE KEY UPDATE spec = VALUES(spec)";
  private static final String DELETE_STATEMENT = "DELETE FROM %s WHERE spec_uri = ?";
  private static final String GET_STATEMENT = "SELECT spec FROM %s WHERE spec_uri = ?";
  private static final String GET_ALL_STATEMENT = "SELECT spec_uri, spec FROM %s";

  private DataSource dataSource;
  private String tableName;
  private URI specStoreURI;

  public MysqlSpecStore(Config config) throws IOException {
    this.dataSource = MysqlDataSourceFactory.get(config, SharedResourcesBrokerFactory.getImplicitBroker());
    this.tableName = config.getString(ConfigurationKeys.STATE_STORE_DB_TABLE_KEY);
    this.specStoreURI = URI.create(config.getString(ConfigurationKeys.STATE_STORE_DB_URL_KEY));
    try (Connection connection = this.dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(String.format(CREATE_TABLE_STATEMENT, this.tableName))) {
      statement.executeUpdate();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean exists(URI specUri) throws IOException {
    try (Connection connection = this.dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(String.format(EXISTS_STATEMENT, this.tableName))) {
      statement.setString(1, specUri.toString());
      ResultSet rs = statement.executeQuery();
      rs.next();
      return rs.getBoolean(1);
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void addSpec(Spec spec) throws IOException {
    try (Connection connection = this.dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(String.format(INSERT_STATEMENT, this.tableName));
        ByteArrayOutputStream aos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(aos)) {

      oos.writeObject(spec);
      statement.setString(1, spec.getUri().toString());
      statement.setBlob(2, new ByteArrayInputStream(aos.toByteArray()));
      statement.executeUpdate();

      connection.commit();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean deleteSpec(Spec spec) throws IOException {
    return deleteSpec(spec.getUri());
  }

  @Override
  public boolean deleteSpec(URI specUri) throws IOException {
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
  public Spec updateSpec(Spec spec) throws IOException, SpecNotFoundException {
    addSpec(spec);
    return spec;
  }

  @Override
  public Spec getSpec(URI specUri) throws IOException, SpecNotFoundException {
    try (Connection connection = this.dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(String.format(GET_STATEMENT, this.tableName))) {

      statement.setString(1, specUri.toString());
      ResultSet rs = statement.executeQuery();
      if (!rs.next()) {
        throw new SpecNotFoundException(specUri);
      }

      Blob blob = rs.getBlob(1);

      try (ObjectInputStream ois = new ObjectInputStream(blob.getBinaryStream())) {
        return (Spec) ois.readObject();
      }

    } catch (SQLException | ClassNotFoundException e) {
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
  public Collection<Spec> getSpecs() throws IOException {
    try (Connection connection = this.dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(String.format(GET_ALL_STATEMENT, this.tableName))) {

      List<Spec> specs = new ArrayList<>();
      ResultSet rs = statement.executeQuery();

      while (rs.next()) {
        Blob blob = rs.getBlob(2);
        try (ObjectInputStream ois = new ObjectInputStream(blob.getBinaryStream())) {
          specs.add((Spec) ois.readObject());
        }
      }

      return specs;
    } catch (SQLException | ClassNotFoundException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Iterator<URI> getSpecURIs() throws IOException {
    try (Connection connection = this.dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(String.format(GET_ALL_STATEMENT, this.tableName))) {

      List<URI> specs = new ArrayList<>();
      ResultSet rs = statement.executeQuery();

      while (rs.next()) {
        URI specURI = URI.create(rs.getString(1));
        specs.add(specURI);
      }

      return specs.iterator();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Optional<URI> getSpecStoreURI() {
    return Optional.of(this.specStoreURI);
  }
}
