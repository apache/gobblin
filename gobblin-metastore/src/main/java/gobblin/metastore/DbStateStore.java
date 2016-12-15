/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metastore;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import gobblin.configuration.State;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import javax.sql.DataSource;
import org.apache.hadoop.io.Text;

/**
 * An implementation of {@link StateStore} backed by MySQL.
 *
 * <p>
 *
 *     This implementation stores serialized {@link State}s as a blob in the database in the Sequence file format.
 *     The database table row is keyed by the store name and table name.
 *     State keys are state IDs (see {@link State#getId()}), and values are objects of {@link State} or
 *     any of its extensions. Keys will be empty strings if state IDs are not set
 *     (i.e., {@link State#getId()} returns <em>null</em>). In this case, the
 *     {@link DbStateStore#get(String, String, String)} method may not work.
 * </p>
 *
 * @param <T> state object type
 **/
public class DbStateStore<T extends State> implements StateStore<T> {

  // Class of the state objects to be put into the store
  private final Class<T> stateClass;

  private final DataSource dataSource;

  private static final String UPSERT_JOB_STATE_TEMPLATE =
      "INSERT INTO $TABLE$ (store_name, table_name, state) VALUES(?,?,?)"
          + " ON DUPLICATE KEY UPDATE state = values(state)";

  private static final String SELECT_JOB_STATE_TEMPLATE =
      "SELECT state FROM $TABLE$ WHERE store_name = ? and table_name = ?";

  private static final String SELECT_JOB_STATE_WITH_LIKE_TEMPLATE =
      "SELECT state FROM $TABLE$ WHERE store_name = ? and table_name like ?";

  private static final String SELECT_JOB_STATE_EXISTS_TEMPLATE =
      "SELECT 1 FROM $TABLE$ WHERE store_name = ? and table_name = ?";

  private static final String DELETE_JOB_STORE_TEMPLATE =
      "DELETE FROM $TABLE$ WHERE store_name = ?";

  private static final String DELETE_JOB_STATE_TEMPLATE =
      "DELETE FROM $TABLE$ WHERE store_name = ? AND table_name = ?";

  private static final String CLONE_JOB_STATE_TEMPLATE =
      "INSERT INTO $TABLE$(store_name, table_name, state)"
          + " (SELECT store_name, ?, state FROM $TABLE$ s WHERE"
          + " store_name = ? AND table_name = ?)"
          + " ON DUPLICATE KEY UPDATE state = s.state";

  private static final String CREATE_JOB_STATE_TABLE_TEMPLATE =
      "CREATE TABLE IF NOT EXISTS $TABLE$ (store_name varchar(1000), table_name varchar(1000), state mediumblob, "
          + " primary key(store_name, table_name))";

  private final String UPSERT_JOB_STATE_SQL;
  private final String SELECT_JOB_STATE_SQL;
  private final String SELECT_JOB_STATE_WITH_LIKE_SQL;
  private final String SELECT_JOB_STATE_EXISTS_SQL;
  private final String DELETE_JOB_STORE_SQL;
  private final String DELETE_JOB_STATE_SQL;
  private final String CLONE_JOB_STATE_SQL;

  public DbStateStore(DataSource dataSource, String stateStoreTableName, Class<T> stateClass) throws IOException {
    this.dataSource = dataSource;
    this.stateClass = stateClass;

    UPSERT_JOB_STATE_SQL = UPSERT_JOB_STATE_TEMPLATE.replace("$TABLE$", stateStoreTableName);
    SELECT_JOB_STATE_SQL = SELECT_JOB_STATE_TEMPLATE.replace("$TABLE$", stateStoreTableName);
    SELECT_JOB_STATE_WITH_LIKE_SQL = SELECT_JOB_STATE_WITH_LIKE_TEMPLATE.replace("$TABLE$", stateStoreTableName);
    SELECT_JOB_STATE_EXISTS_SQL = SELECT_JOB_STATE_EXISTS_TEMPLATE.replace("$TABLE$", stateStoreTableName);
    DELETE_JOB_STORE_SQL = DELETE_JOB_STORE_TEMPLATE.replace("$TABLE$", stateStoreTableName);
    DELETE_JOB_STATE_SQL = DELETE_JOB_STATE_TEMPLATE.replace("$TABLE$", stateStoreTableName);
    CLONE_JOB_STATE_SQL = CLONE_JOB_STATE_TEMPLATE.replace("$TABLE$", stateStoreTableName);

    // create table if it does not exist
    String createJobTable = CREATE_JOB_STATE_TABLE_TEMPLATE.replace("$TABLE$", stateStoreTableName);
    try (Connection connection = dataSource.getConnection();
        PreparedStatement createStatement = connection.prepareStatement(createJobTable)) {
      createStatement.executeUpdate();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean create(String storeName) throws IOException {
    /* nothing to do since state will be stored as a new row in a DB table that has been validated */
    return true;
  }

  @Override
  public boolean create(String storeName, String tableName) throws IOException {
    if (exists(storeName, tableName)) {
      throw new IOException(String.format("State already exists for storeName %s tableName %s", storeName, tableName));
    }

    return true;
  }

  @Override
  public boolean exists(String storeName, String tableName) throws IOException {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement queryStatement = connection.prepareStatement(SELECT_JOB_STATE_EXISTS_SQL)) {
      int index = 0;
      queryStatement.setString(++index, storeName);
      queryStatement.setString(++index, tableName);

      try (ResultSet rs = queryStatement.executeQuery()) {
        if (rs.next()) {
          return true;
        } else {
          return false;
        }
      }
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  /**
   * Serializes the state to the {@link DataOutput}
   * @param dataOutput output target receiving the serialized data
   * @param state the state to serialize
   * @throws IOException
   */
  private void addStateToOs(DataOutput dataOutput, T state) throws IOException {
    new Text(Strings.nullToEmpty(state.getId())).write(dataOutput);
    state.write(dataOutput);
  }

  @Override
  public void put(String storeName, String tableName, T state) throws IOException {

    try (Connection connection = dataSource.getConnection();
        PreparedStatement insertStatement = connection.prepareStatement(UPSERT_JOB_STATE_SQL)) {
      connection.setAutoCommit(false);
      ByteArrayOutputStream byteArrayOs = new ByteArrayOutputStream();
      DataOutputStream dataOutput = new DataOutputStream(byteArrayOs);

      int index = 0;
      insertStatement.setString(++index, storeName);
      insertStatement.setString(++index, tableName);

      addStateToOs(dataOutput, state);

      insertStatement.setBlob(++index, new ByteArrayInputStream(byteArrayOs.toByteArray()));

      insertStatement.executeUpdate();
      connection.commit();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void putAll(String storeName, String tableName, Collection<T> states) throws IOException {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement insertStatement = connection.prepareStatement(UPSERT_JOB_STATE_SQL)) {
      connection.setAutoCommit(false);
      ByteArrayOutputStream byteArrayOs = new ByteArrayOutputStream();
      DataOutputStream dataOutput = new DataOutputStream(byteArrayOs);

      int index = 0;
      insertStatement.setString(++index, storeName);
      insertStatement.setString(++index, tableName);

      for (T state : states) {
        addStateToOs(dataOutput, state);
      }

      insertStatement.setBlob(++index, new ByteArrayInputStream(byteArrayOs.toByteArray()));

      insertStatement.executeUpdate();
      connection.commit();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  public T get(String storeName, String tableName, String stateId) throws IOException {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement queryStatement = connection.prepareStatement(SELECT_JOB_STATE_SQL)) {
      int index = 0;
      queryStatement.setString(++index, storeName);
      queryStatement.setString(++index, tableName);

      try (ResultSet rs = queryStatement.executeQuery()) {
        if (rs.next()) {
          Blob blob = rs.getBlob(1);
          DataInputStream dis = new DataInputStream(blob.getBinaryStream());
          Text key = new Text();

          // keep deserializing while we have data
          while (dis.available() > 0) {
            T state = this.stateClass.newInstance();

            key.readFields(dis);
            state.readFields(dis);

            if (key.toString().equals(stateId)) {
              return state;
            }
          }
        }
      }
    } catch (Exception e) {
      throw new IOException(e);
    }

    return null;
  }

  protected List<T> getAll(String storeName, String tableName, boolean useLike) throws IOException {
    List<T> states = Lists.newArrayList();

    try (Connection connection = dataSource.getConnection();
        PreparedStatement queryStatement = connection.prepareStatement(useLike ?
            SELECT_JOB_STATE_WITH_LIKE_SQL : SELECT_JOB_STATE_SQL)) {
      queryStatement.setString(1, storeName);
      queryStatement.setString(2, tableName);

      try (ResultSet rs = queryStatement.executeQuery()) {
        while (rs.next()) {
          Blob blob = rs.getBlob(1);
          DataInputStream dis = new DataInputStream(blob.getBinaryStream());
          Text key = new Text();

          // keep deserializing while we have data
          while (dis.available() > 0) {
            T state = this.stateClass.newInstance();
            key.readString(dis);
            state.readFields(dis);
            states.add(state);
          }
        }
      }
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new IOException(e);
    }

    return states;
  }

  @Override
  public List<T> getAll(String storeName, String tableName) throws IOException {
    return getAll(storeName, tableName, false);
  }

  @Override
  public List<T> getAll(String storeName) throws IOException {
    return getAll(storeName, "%", true);
  }

  @Override
  public void createAlias(String storeName, String original, String alias) throws IOException {

    if (!exists(storeName, original)) {
      throw new IOException(String.format("State does not exist for table %s", original));
    }

    try (Connection connection = dataSource.getConnection();
        PreparedStatement cloneStatement = connection.prepareStatement(CLONE_JOB_STATE_SQL)) {
      connection.setAutoCommit(false);
      int index = 0;
      cloneStatement.setString(++index, alias);
      cloneStatement.setString(++index, storeName);
      cloneStatement.setString(++index, original);
      cloneStatement.executeUpdate();
      connection.commit();
    } catch (SQLException e) {
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public void delete(String storeName, String tableName) throws IOException {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement deleteStatement = connection.prepareStatement(DELETE_JOB_STATE_SQL)) {
      connection.setAutoCommit(false);
      int index = 0;
      deleteStatement.setString(++index, storeName);
      deleteStatement.setString(++index, tableName);
      deleteStatement.executeUpdate();
      connection.commit();
    } catch (SQLException e) {
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public void delete(String storeName) throws IOException {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement deleteStatement = connection.prepareStatement(DELETE_JOB_STORE_SQL)) {
      connection.setAutoCommit(false);
      deleteStatement.setString(1, storeName);
      deleteStatement.executeUpdate();
      connection.commit();
    } catch (SQLException e) {
      throw new IOException(e.getMessage());
    }
  }
}
