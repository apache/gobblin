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

package org.apache.gobblin.data.management.copy.hive;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import com.google.common.collect.ImmutableSet;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.CopyableDataset;
import org.apache.gobblin.data.management.copy.hive.HiveDatasetFinder.DbAndTable;
import org.apache.gobblin.data.management.copy.prioritization.PrioritizedCopyableDataset;
import org.apache.gobblin.data.management.partition.FileSet;
import org.apache.gobblin.hive.HiveMetastoreClientPool;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.util.AutoReturnableObject;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.request_allocation.PushDownRequestor;


/**
 * Hive dataset implementing {@link CopyableDataset}.
 */
@Slf4j
@Alpha
@Getter
@ToString
public class HiveDataset implements PrioritizedCopyableDataset {

  private static Splitter SPLIT_ON_DOT = Splitter.on(".").omitEmptyStrings().trimResults();
  public static final ImmutableSet<TableType> COPYABLE_TABLES = ImmutableSet.of(TableType.EXTERNAL_TABLE, TableType.MANAGED_TABLE);

  public static final String REGISTERER = "registerer";
  public static final String REGISTRATION_GENERATION_TIME_MILLIS = "registrationGenerationTimeMillis";
  public static final String DATASET_NAME_PATTERN_KEY = "hive.datasetNamePattern";
  public static final String DATABASE = "Database";
  public static final String TABLE = "Table";

  public static final String DATABASE_TOKEN = "$DB";
  public static final String TABLE_TOKEN = "$TABLE";

  public static final String LOGICAL_DB_TOKEN = "$LOGICAL_DB";
  public static final String LOGICAL_TABLE_TOKEN = "$LOGICAL_TABLE";

  // Will not be serialized/de-serialized
  protected transient final Properties properties;
  protected transient final FileSystem fs;
  protected transient final HiveMetastoreClientPool clientPool;
  private transient final MetricContext metricContext;
  protected transient final Table table;
  protected transient final Config datasetConfig;

  // Only set if table has exactly one location
  protected final Optional<Path> tableRootPath;
  protected final String tableIdentifier;
  protected final Optional<String> datasetNamePattern;
  protected final DbAndTable dbAndTable;
  protected final DbAndTable logicalDbAndTable;

  public HiveDataset(FileSystem fs, HiveMetastoreClientPool clientPool, Table table, Properties properties) {
    this(fs, clientPool, table, properties, ConfigFactory.empty());
  }

  public HiveDataset(FileSystem fs, HiveMetastoreClientPool clientPool, Table table, Config datasetConfig) {
    this(fs, clientPool, table, new Properties(), datasetConfig);
  }

  public HiveDataset(FileSystem fs, HiveMetastoreClientPool clientPool, Table table, Properties properties, Config datasetConfig) {
    this.fs = fs;
    this.clientPool = clientPool;
    this.table = table;
    this.properties = properties;

    this.tableRootPath = PathUtils.isGlob(this.table.getDataLocation()) ? Optional.<Path> absent() :
        Optional.fromNullable(this.table.getDataLocation());

    this.tableIdentifier = this.table.getDbName() + "." + this.table.getTableName();

    this.datasetNamePattern = Optional.fromNullable(ConfigUtils.getString(datasetConfig, DATASET_NAME_PATTERN_KEY, null));
    this.dbAndTable = new DbAndTable(table.getDbName(), table.getTableName());
    if (this.datasetNamePattern.isPresent()) {
      this.logicalDbAndTable = parseLogicalDbAndTable(this.datasetNamePattern.get(), this.dbAndTable, LOGICAL_DB_TOKEN, LOGICAL_TABLE_TOKEN);
    } else {
      this.logicalDbAndTable = this.dbAndTable;
    }
    this.datasetConfig = resolveConfig(datasetConfig, dbAndTable, logicalDbAndTable);
    this.metricContext = Instrumented.getMetricContext(new State(properties), HiveDataset.class,
        Lists.<Tag<?>> newArrayList(new Tag<>(DATABASE, table.getDbName()), new Tag<>(TABLE, table.getTableName())));
  }

  @Override
  public Iterator<FileSet<CopyEntity>> getFileSetIterator(FileSystem targetFs, CopyConfiguration configuration)
      throws IOException {
    if (!canCopyTable()) {
      return Iterators.emptyIterator();
    }
    try {
      return new HiveCopyEntityHelper(this, configuration, targetFs).getCopyEntities(configuration);
    } catch (IOException ioe) {
      log.error("Failed to copy table " + this.table, ioe);
      return Iterators.emptyIterator();
    }
  }

  /**
   * Finds all files read by the table and generates CopyableFiles.
   * For the specific semantics see {@link HiveCopyEntityHelper#getCopyEntities}.
   */
  @Override
  public Iterator<FileSet<CopyEntity>> getFileSetIterator(FileSystem targetFs, CopyConfiguration configuration,
      Comparator<FileSet<CopyEntity>> prioritizer, PushDownRequestor<FileSet<CopyEntity>> requestor)
      throws IOException {
    if (!canCopyTable()) {
      return Iterators.emptyIterator();
    }
    try {
      List<FileSet<CopyEntity>> fileSetList = Lists.newArrayList(new HiveCopyEntityHelper(this, configuration, targetFs)
          .getCopyEntities(configuration, prioritizer, requestor));
      Collections.sort(fileSetList, prioritizer);
      return fileSetList.iterator();
    } catch (IOException ioe) {
      log.error("Failed to copy table " + this.table, ioe);
      return Iterators.emptyIterator();
    }
  }

  @Override
  public String datasetURN() {
    return this.table.getCompleteName();
  }

  /**
   * Resolve {@value #DATABASE_TOKEN} and {@value #TABLE_TOKEN} in <code>rawString</code> to {@link Table#getDbName()}
   * and {@link Table#getTableName()}
   */
  public static String resolveTemplate(String rawString, Table table) {
    if (StringUtils.isBlank(rawString)) {
      return rawString;
    }
    return StringUtils.replaceEach(rawString, new String[] { DATABASE_TOKEN, TABLE_TOKEN }, new String[] { table.getDbName(), table.getTableName() });
  }

  /***
   * Parse logical Database and Table name from a given DbAndTable object.
   *
   * Eg.
   * Dataset Name Pattern         : prod_$LOGICAL_DB_linkedin.prod_$LOGICAL_TABLE_linkedin
   * Source DB and Table          : prod_dbName_linkedin.prod_tableName_linkedin
   * Logical DB Token             : $LOGICAL_DB
   * Logical Table Token          : $LOGICAL_TABLE
   * Parsed Logical DB and Table  : dbName.tableName
   *
   * @param datasetNamePattern    Dataset name pattern.
   * @param dbAndTable            Source DB and Table.
   * @param logicalDbToken        Logical DB token.
   * @param logicalTableToken     Logical Table token.
   * @return  Parsed logical DB and Table.
   */
  @VisibleForTesting
  protected static DbAndTable parseLogicalDbAndTable(String datasetNamePattern, DbAndTable dbAndTable,
      String logicalDbToken, String logicalTableToken) {
    Preconditions.checkArgument(StringUtils.isNotBlank(datasetNamePattern), "Dataset name pattern must not be empty.");

    List<String> datasetNameSplit = Lists.newArrayList(SPLIT_ON_DOT.split(datasetNamePattern));
    Preconditions.checkArgument(datasetNameSplit.size() == 2, "Dataset name pattern must of the format: "
        + "dbPrefix_$LOGICAL_DB_dbPostfix.tablePrefix_$LOGICAL_TABLE_tablePostfix (prefix / postfix are optional)");

    String dbNamePattern = datasetNameSplit.get(0);
    String tableNamePattern = datasetNameSplit.get(1);

    String logicalDb = extractTokenValueFromEntity(dbAndTable.getDb(), dbNamePattern, logicalDbToken);
    String logicalTable = extractTokenValueFromEntity(dbAndTable.getTable(), tableNamePattern, logicalTableToken);

    return new DbAndTable(logicalDb, logicalTable);
  }

  /***
   * Extract token value from source entity, where token value is represented by a token in the source entity.
   *
   * Eg.
   * Source Entity  : prod_tableName_avro
   * Source Template: prod_$LOGICAL_TABLE_avro
   * Token          : $LOGICAL_TABLE
   * Extracted Value: tableName
   *
   * @param sourceEntity      Source entity (typically a table or database name).
   * @param sourceTemplate    Source template representing the source entity.
   * @param token             Token representing the value to extract from the source entity using the template.
   * @return Extracted token value from the source entity.
   */
  @VisibleForTesting
  protected static String extractTokenValueFromEntity(String sourceEntity, String sourceTemplate, String token) {
    Preconditions.checkArgument(StringUtils.isNotBlank(sourceEntity), "Source entity should not be blank");
    Preconditions.checkArgument(StringUtils.isNotBlank(sourceTemplate), "Source template should not be blank");
    Preconditions.checkArgument(sourceTemplate.contains(token), String.format("Source template: %s should contain token: %s", sourceTemplate, token));

    String extractedValue = sourceEntity;
    List<String> preAndPostFix = Lists.newArrayList(Splitter.on(token).trimResults().split(sourceTemplate));

    extractedValue = StringUtils.removeStart(extractedValue, preAndPostFix.get(0));
    extractedValue = StringUtils.removeEnd(extractedValue, preAndPostFix.get(1));

    return extractedValue;
  }

  /***
   * Replace various tokens (DB, TABLE, LOGICAL_DB, LOGICAL_TABLE) with their values.
   *
   * @param datasetConfig       The config object that needs to be resolved with final values.
   * @param realDbAndTable      Real DB and Table .
   * @param logicalDbAndTable   Logical DB and Table.
   * @return Resolved config object.
   */
  @VisibleForTesting
  protected static Config resolveConfig(Config datasetConfig, DbAndTable realDbAndTable, DbAndTable logicalDbAndTable) {
    Preconditions.checkNotNull(datasetConfig, "Dataset config should not be null");
    Preconditions.checkNotNull(realDbAndTable, "Real DB and table should not be null");
    Preconditions.checkNotNull(logicalDbAndTable, "Logical DB and table should not be null");

    Properties resolvedProperties = new Properties();
    Config resolvedConfig = datasetConfig.resolve();
    for (Map.Entry<String, ConfigValue> entry : resolvedConfig.entrySet()) {
      if (ConfigValueType.LIST.equals(entry.getValue().valueType())) {
        List<String> rawValueList = resolvedConfig.getStringList(entry.getKey());
        List<String> resolvedValueList = Lists.newArrayList();
        for (String rawValue : rawValueList) {
          String resolvedValue = StringUtils.replaceEach(rawValue,
              new String[] { DATABASE_TOKEN, TABLE_TOKEN, LOGICAL_DB_TOKEN, LOGICAL_TABLE_TOKEN },
              new String[] { realDbAndTable.getDb(), realDbAndTable.getTable(), logicalDbAndTable.getDb(), logicalDbAndTable.getTable() });
          resolvedValueList.add(resolvedValue);
        }
        StringBuilder listToStringWithQuotes = new StringBuilder();
        for (String resolvedValueStr : resolvedValueList) {
          if (listToStringWithQuotes.length() > 0) {
            listToStringWithQuotes.append(",");
          }
          listToStringWithQuotes.append("\"").append(resolvedValueStr).append("\"");
        }
        resolvedProperties.setProperty(entry.getKey(), listToStringWithQuotes.toString());
      } else {
        String resolvedValue = StringUtils.replaceEach(resolvedConfig.getString(entry.getKey()),
          new String[] { DATABASE_TOKEN, TABLE_TOKEN, LOGICAL_DB_TOKEN, LOGICAL_TABLE_TOKEN },
          new String[] { realDbAndTable.getDb(), realDbAndTable.getTable(), logicalDbAndTable.getDb(), logicalDbAndTable.getTable() });
        resolvedProperties.setProperty(entry.getKey(), resolvedValue);
      }
    }

    return ConfigUtils.propertiesToConfig(resolvedProperties);
  }

  /**
   * Sort all partitions inplace on the basis of complete name ie dbName.tableName.partitionName
   */
  public static List<Partition> sortPartitions(List<Partition> partitions) {
    Collections.sort(partitions, new Comparator<Partition>() {
      @Override
      public int compare(Partition o1, Partition o2) {
        return o1.getCompleteName().compareTo(o2.getCompleteName());
      }
    });
    return partitions;
  }

  /**
   * This method returns a sorted list of partitions.
   */
  public List<Partition> getPartitionsFromDataset() throws IOException{
    try (AutoReturnableObject<IMetaStoreClient> client = getClientPool().getClient()) {
      List<Partition> partitions =
          HiveUtils.getPartitions(client.get(), getTable(), Optional.<String>absent());
      return sortPartitions(partitions);
    }
  }

  private boolean canCopyTable() {
    if (!COPYABLE_TABLES.contains(this.table.getTableType())) {
      log.warn(String.format("Not copying %s: tables of type %s are not copyable.", this.table.getCompleteName(),
          this.table.getTableType()));
      return false;
    }
    return true;
  }
}
