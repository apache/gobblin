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

package gobblin.hive;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import gobblin.annotation.Alpha;
import gobblin.configuration.State;
import lombok.AllArgsConstructor;
import lombok.Getter;


/**
 * A class that represents a Hive table or partition.
 *
 * @author ziliu
 */
@Getter
@Alpha
public class HiveRegistrationUnit {

  protected final String dbName;
  protected final String tableName;
  protected final List<Column> columns = Lists.newArrayList();
  protected final State props = new State();
  protected final State storageProps = new State();
  protected final State serDeProps = new State();

  protected final Optional<HiveSerDeManager> serDeManager;

  /**
   * Table or Partition properties
   */
  protected Optional<Long> createTime;
  protected Optional<Long> lastAccessTime;

  /**
   * Storage properties
   */
  protected Optional<String> location;
  protected Optional<String> inputFormat;
  protected Optional<String> outputFormat;
  protected Optional<Boolean> isCompressed;
  protected Optional<Integer> numBuckets;
  protected Optional<List<String>> bucketColumns;
  protected Optional<Boolean> isStoredAsSubDirs;

  /**
   * SerDe properties
   */
  protected Optional<String> serDeType;

  HiveRegistrationUnit(Builder<?> builder) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(builder.dbName));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(builder.tableName));

    this.dbName = builder.dbName;
    this.tableName = builder.tableName;
    this.columns.addAll(builder.columns);
    this.props.addAll(builder.props);
    this.storageProps.addAll(builder.storageProps);
    this.serDeProps.addAll(builder.serDeProps);

    this.serDeManager = builder.serDeManager;

    populateTablePartitionFields();
    populateStorageFields();
    populateSerDeFields();
  }

  protected void populateTablePartitionFields() {
    this.createTime = this.props.contains(HiveConstants.CREATE_TIME)
        ? Optional.of(this.props.getPropAsLong(HiveConstants.CREATE_TIME)) : Optional.<Long> absent();
    this.lastAccessTime = this.props.contains(HiveConstants.LAST_ACCESS_TIME)
        ? Optional.of(this.props.getPropAsLong(HiveConstants.LAST_ACCESS_TIME)) : Optional.<Long> absent();
  }

  protected void populateStorageFields() {
    this.location = Optional.fromNullable(this.storageProps.getProp(HiveConstants.LOCATION));
    this.inputFormat = Optional.fromNullable(this.storageProps.getProp(HiveConstants.INPUT_FORMAT));
    this.outputFormat = Optional.fromNullable(this.storageProps.getProp(HiveConstants.OUTPUT_FORMAT));
    this.isCompressed = this.storageProps.contains(HiveConstants.COMPRESSED)
        ? Optional.of(this.storageProps.getPropAsBoolean(HiveConstants.COMPRESSED)) : Optional.<Boolean> absent();
    this.numBuckets = this.storageProps.contains(HiveConstants.NUM_BUCKETS)
        ? Optional.of(this.storageProps.getPropAsInt(HiveConstants.NUM_BUCKETS)) : Optional.<Integer> absent();
    this.bucketColumns = this.storageProps.contains(HiveConstants.BUCKET_COLUMNS)
        ? Optional.of(this.storageProps.getPropAsList(HiveConstants.BUCKET_COLUMNS)) : Optional.<List<String>> absent();
    this.isStoredAsSubDirs = this.storageProps.contains(HiveConstants.STORED_AS_SUB_DIRS)
        ? Optional.of(this.storageProps.getPropAsBoolean(HiveConstants.STORED_AS_SUB_DIRS))
        : Optional.<Boolean> absent();
  }

  protected void populateSerDeFields() {
    this.serDeType = Optional.fromNullable(this.serDeProps.getProp(HiveConstants.SERDE_TYPE));
  }

  /**
   * Set the columns for a table or partition.
   *
   * <p>
   *   Columns does not need to be set for a table if the table's serde already provides the schema,
   *   such as Avro tables. Columns does not need to be set for a partition if they are the same as
   *   the table's columns.
   * </p>
   * @param columns
   */
  public void setColumns(List<Column> columns) {
    this.columns.clear();
    this.columns.addAll(columns);
  }

  /**
   * Set a table/partition parameter.
   *
   * <p>
   *   When using {@link gobblin.hive.metastore.HiveMetaStoreBasedRegister}, since it internally use
   *   {@link org.apache.hadoop.hive.metastore.api.Table} and {@link org.apache.hadoop.hive.metastore.api.Partition}
   *   which distinguishes between table/partition parameters, storage descriptor parameters, and serde parameters,
   *   one may need to distinguish them when constructing a {@link HiveRegistrationUnit} by using
   *   {@link #setProp(String, Object)}, {@link #setStorageProp(String, Object)} and
   *   {@link #setSerDeProp(String, Object)}. When using query-based Hive registration, they do not need to be
   *   distinguished since all parameters will be passed via TBLPROPERTIES.
   * </p>
   */
  public void setProp(String key, Object value) {
    this.props.setProp(key, value);
    updateTablePartitionFields(key, value);
  }

  /**
   * Set a storage parameter for a table/partition.
   *
   * <p>
   *   When using {@link gobblin.hive.metastore.HiveMetaStoreBasedRegister}, since it internally use
   *   {@link org.apache.hadoop.hive.metastore.api.Table} and {@link org.apache.hadoop.hive.metastore.api.Partition}
   *   which distinguishes between table/partition parameters, storage descriptor parameters, and serde parameters,
   *   one may need to distinguish them when constructing a {@link HiveRegistrationUnit} by using
   *   {@link #setProp(String, Object)}, {@link #setStorageProp(String, Object)} and
   *   {@link #setSerDeProp(String, Object)}. When using query-based Hive registration, they do not need to be
   *   distinguished since all parameters will be passed via TBLPROPERTIES.
   * </p>
   */
  public void setStorageProp(String key, Object value) {
    this.storageProps.setProp(key, value);
    updateStorageFields(key, value);
  }

  /**
   * Set a serde parameter for a table/partition.
   *
   * <p>
   *   When using {@link gobblin.hive.metastore.HiveMetaStoreBasedRegister}, since it internally use
   *   {@link org.apache.hadoop.hive.metastore.api.Table} and {@link org.apache.hadoop.hive.metastore.api.Partition}
   *   which distinguishes between table/partition parameters, storage descriptor parameters, and serde parameters,
   *   one may need to distinguish them when constructing a {@link HiveRegistrationUnit} by using
   *   {@link #setProp(String, Object)}, {@link #setStorageProp(String, Object)} and
   *   {@link #setSerDeProp(String, Object)}. When using query-based Hive registration, they do not need to be
   *   distinguished since all parameters will be passed via TBLPROPERTIES.
   * </p>
   */
  public void setSerDeProp(String key, Object value) {
    this.serDeProps.setProp(key, value);
    updateSerDeFields(key, value);
  }

  /**
   * Set table/partition parameters.
   *
   * <p>
   *   When using {@link gobblin.hive.metastore.HiveMetaStoreBasedRegister}, since it internally use
   *   {@link org.apache.hadoop.hive.metastore.api.Table} and {@link org.apache.hadoop.hive.metastore.api.Partition}
   *   which distinguishes between table/partition parameters, storage descriptor parameters, and serde parameters,
   *   one may need to distinguish them when constructing a {@link HiveRegistrationUnit} by using
   *   {@link #setProps(State)}, {@link #setStorageProps(State)} and
   *   {@link #setSerDeProps(State)}. When using query-based Hive registration, they do not need to be
   *   distinguished since all parameters will be passed via TBLPROPERTIES.
   * </p>
   */
  public void setProps(State props) {
    this.props.addAll(props);
    populateTablePartitionFields();
  }

  /**
   * Set storage parameters for a table/partition.
   *
   * <p>
   *   When using {@link gobblin.hive.metastore.HiveMetaStoreBasedRegister}, since it internally use
   *   {@link org.apache.hadoop.hive.metastore.api.Table} and {@link org.apache.hadoop.hive.metastore.api.Partition}
   *   which distinguishes between table/partition parameters, storage descriptor parameters, and serde parameters,
   *   one may need to distinguish them when constructing a {@link HiveRegistrationUnit} by using
   *   {@link #setProps(State)}, {@link #setStorageProps(State)} and
   *   {@link #setSerDeProps(State)}. When using query-based Hive registration, they do not need to be
   *   distinguished since all parameters will be passed via TBLPROPERTIES.
   * </p>
   */
  public void setStorageProps(State storageProps) {
    this.storageProps.addAll(storageProps);
    populateStorageFields();
  }

  /**
   * Set serde parameters for a table/partition.
   *
   * <p>
   *   When using {@link gobblin.hive.metastore.HiveMetaStoreBasedRegister}, since it internally use
   *   {@link org.apache.hadoop.hive.metastore.api.Table} and {@link org.apache.hadoop.hive.metastore.api.Partition}
   *   which distinguishes between table/partition parameters, storage descriptor parameters, and serde parameters,
   *   one may need to distinguish them when constructing a {@link HiveRegistrationUnit} by using
   *   {@link #setProps(State)}, {@link #setStorageProps(State)} and
   *   {@link #setSerDeProps(State)}. When using query-based Hive registration, they do not need to be
   *   distinguished since all parameters will be passed via TBLPROPERTIES.
   * </p>
   */
  public void setSerDeProps(State serdeProps) {
    this.serDeProps.addAll(serdeProps);
    populateSerDeFields();
  }

  protected void updateTablePartitionFields(String key, Object value) {
    switch (key) {
      case HiveConstants.CREATE_TIME:
        this.createTime = Optional.of((long) value);
        break;
      case HiveConstants.LAST_ACCESS_TIME:
        this.createTime = Optional.of((long) value);
        break;
    }
  }

  protected void updateStorageFields(String key, Object value) {
    switch (key) {
      case HiveConstants.LOCATION:
        this.location = Optional.of((String) value);
        break;
      case HiveConstants.INPUT_FORMAT:
        this.inputFormat = Optional.of((String) value);
        break;
      case HiveConstants.OUTPUT_FORMAT:
        this.outputFormat = Optional.of((String) value);
        break;
      case HiveConstants.COMPRESSED:
        this.isCompressed = Optional.of((boolean) value);
        break;
      case HiveConstants.NUM_BUCKETS:
        this.numBuckets = Optional.of((int) value);
        break;
      case HiveConstants.BUCKET_COLUMNS:
        this.bucketColumns = Optional.of(Splitter.on(',').omitEmptyStrings().trimResults().splitToList((String) value));
        break;
      case HiveConstants.STORED_AS_SUB_DIRS:
        this.isStoredAsSubDirs = Optional.of((boolean) value);
        break;
    }
  }

  protected void updateSerDeFields(String key, Object value) {
    switch (key) {
      case HiveConstants.SERDE_TYPE:
        this.serDeType = Optional.of((String) value);
        break;
    }
  }

  /**
   * Setting serde parameters for a table/partition using the table/partition's {@link HiveSerDeManager}.
   *
   * <p>
   *   Requires that the {@link HiveSerDeManager} of the table/partition must be specified in
   *   {@link Builder#withSerdeManaager(HiveSerDeManager)}, and the table/partition's location must be specified
   *   either in {@link #setLocation(String)} or via {@link HiveConstants#LOCATION}.
   * </p>
   */
  public void setSerDeProps() throws IOException {
    this.serDeManager.get().addSerDeProperties(new Path(this.location.get()), this);
  }

  public void setCreateTime(long createTime) {
    this.createTime = Optional.of(createTime);
  }

  public void setLastAccessTime(long lastAccessTime) {
    this.lastAccessTime = Optional.of(lastAccessTime);
  }

  public void setLocation(String location) {
    this.location = Optional.of(location);
  }

  public void setInputFormat(String inputFormat) {
    this.inputFormat = Optional.of(inputFormat);
  }

  public void setOutputFormat(String outputFormat) {
    this.outputFormat = Optional.of(outputFormat);
  }

  public void setCompressed(boolean isCompressed) {
    this.isCompressed = Optional.of(isCompressed);
  }

  public void setNumBuckets(int numBuckets) {
    this.numBuckets = Optional.of(numBuckets);
  }

  public void setBucketColumns(List<String> bucketColumns) {
    this.bucketColumns = Optional.<List<String>> of(ImmutableList.<String> copyOf(bucketColumns));
  }

  public void setStoredAsSubDirs(boolean isStoredAsSubDirs) {
    this.isStoredAsSubDirs = Optional.of(isStoredAsSubDirs);
  }

  public void setSerDeType(String serDeType) {
    this.serDeType = Optional.of(serDeType);
  }

  static abstract class Builder<T extends Builder<?>> {
    private String dbName;
    private String tableName;
    private List<Column> columns = Lists.newArrayList();
    private State props = new State();
    private State storageProps = new State();
    private State serDeProps = new State();
    private Optional<HiveSerDeManager> serDeManager = Optional.absent();

    @SuppressWarnings("unchecked")
    public T withDbName(String dbName) {
      this.dbName = dbName;
      return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withTableName(String tableName) {
      this.tableName = tableName;
      return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withColumns(List<Column> columns) {
      this.columns = columns;
      return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withProps(State props) {
      this.props = props;
      return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withStorageProps(State storageProps) {
      this.storageProps = storageProps;
      return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withSerdeProps(State serDeProps) {
      this.serDeProps = serDeProps;
      return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withSerdeManaager(HiveSerDeManager serDeManager) {
      this.serDeManager = Optional.of(serDeManager);
      return (T) this;
    }

    public abstract HiveRegistrationUnit build();
  }

  @AllArgsConstructor
  @Getter
  public static class Column {
    private final String name;
    private final String type;
    private final String comment;
  }
}
