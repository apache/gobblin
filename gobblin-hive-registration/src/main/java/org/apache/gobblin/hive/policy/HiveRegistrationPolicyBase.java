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

package org.apache.gobblin.hive.policy;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;

import com.codahale.metrics.Timer;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.config.client.ConfigClient;
import org.apache.gobblin.config.client.api.VersionStabilityPolicy;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.hive.HivePartition;
import org.apache.gobblin.hive.HiveRegProps;
import org.apache.gobblin.hive.HiveRegister;
import org.apache.gobblin.hive.HiveSerDeManager;
import org.apache.gobblin.hive.HiveTable;
import org.apache.gobblin.hive.metastore.HiveMetaStoreUtils;
import org.apache.gobblin.hive.spec.HiveSpec;
import org.apache.gobblin.hive.spec.SimpleHiveSpec;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.source.extractor.extract.kafka.ConfigStoreUtils;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaSource;
import org.apache.gobblin.util.ConfigUtils;


/**
 * A base implementation of {@link HiveRegistrationPolicy}. It obtains database name from
 * property {@link #HIVE_DATABASE_NAME} or {@link #HIVE_DATABASE_REGEX} (group 1), obtains
 * table name from property {@link #HIVE_TABLE_NAME} and {@link #HIVE_TABLE_REGEX} (group 1),
 * and builds a {@link SimpleHiveSpec}.
 *
 * @author Ziyang Liu
 */
@Alpha
public class HiveRegistrationPolicyBase implements HiveRegistrationPolicy {

  public static final String HIVE_DATABASE_NAME = "hive.database.name";
  public static final String ADDITIONAL_HIVE_DATABASE_NAMES = "additional.hive.database.names";
  public static final String HIVE_DATABASE_REGEX = "hive.database.regex";
  public static final String HIVE_DATABASE_NAME_PREFIX = "hive.database.name.prefix";
  public static final String HIVE_DATABASE_NAME_SUFFIX = "hive.database.name.suffix";
  public static final String HIVE_TABLE_NAME = "hive.table.name";
  public static final String ADDITIONAL_HIVE_TABLE_NAMES = "additional.hive.table.names";
  public static final String HIVE_TABLE_REGEX = "hive.table.regex";
  public static final String HIVE_TABLE_NAME_PREFIX = "hive.table.name.prefix";
  public static final String HIVE_TABLE_NAME_SUFFIX = "hive.table.name.suffix";
  public static final String HIVE_SANITIZE_INVALID_NAMES = "hive.sanitize.invalid.names";
  public static final String HIVE_FS_URI = "hive.registration.fs.uri";
  // {@value PRIMARY_TABLE_TOKEN} if present in {@value ADDITIONAL_HIVE_TABLE_NAMES} or dbPrefix.{@value HIVE_TABLE_NAME}
  // .. will be replaced by the table name determined via {@link #getTableName(Path)}
  public static final String PRIMARY_TABLE_TOKEN = "$PRIMARY_TABLE";

  protected static final ConfigClient configClient =
      org.apache.gobblin.config.client.ConfigClient.createConfigClient(VersionStabilityPolicy.WEAK_LOCAL_STABILITY);

  protected Optional<Config> configForTopic = Optional.<Config>absent();

  /**
   * A valid db or table name should start with an alphanumeric character, and contains only
   * alphanumeric characters and '_'.
   */
  private static final Pattern VALID_DB_TABLE_NAME_PATTERN_1 = Pattern.compile("[a-z0-9][a-z0-9_]*");

  /**
   * A valid db or table name should contain at least one letter or '_' (i.e., should not be numbers only).
   */
  private static final Pattern VALID_DB_TABLE_NAME_PATTERN_2 = Pattern.compile(".*[a-z_].*");
  public static final String CONFIG_FOR_TOPIC_TIMER = "configForTopicTimer";

  protected final HiveRegProps props;
  protected final FileSystem fs;
  protected final boolean sanitizeNameAllowed;
  protected final Optional<Pattern> dbNamePattern;
  protected final Optional<Pattern> tableNamePattern;
  protected final String dbNamePrefix;
  protected final String dbNameSuffix;
  protected final String tableNamePrefix;
  protected final String tableNameSuffix;
  protected final boolean emptyInputPathFlag;

  protected final MetricContext metricContext;

  public HiveRegistrationPolicyBase(State props) throws IOException {
    Preconditions.checkNotNull(props);
    this.props = new HiveRegProps(props);
    if (props.contains(HiveRegistrationPolicyBase.HIVE_FS_URI)) {
      this.fs = FileSystem.get(URI.create(props.getProp(HiveRegistrationPolicyBase.HIVE_FS_URI)), new Configuration());
    } else {
      this.fs = FileSystem.get(new Configuration());
    }
    this.sanitizeNameAllowed = props.getPropAsBoolean(HIVE_SANITIZE_INVALID_NAMES, true);
    this.dbNamePattern = props.contains(HIVE_DATABASE_REGEX)
        ? Optional.of(Pattern.compile(props.getProp(HIVE_DATABASE_REGEX))) : Optional.<Pattern> absent();
    this.tableNamePattern = props.contains(HIVE_TABLE_REGEX)
        ? Optional.of(Pattern.compile(props.getProp(HIVE_TABLE_REGEX))) : Optional.<Pattern> absent();
    this.dbNamePrefix = props.getProp(HIVE_DATABASE_NAME_PREFIX, StringUtils.EMPTY);
    this.dbNameSuffix = props.getProp(HIVE_DATABASE_NAME_SUFFIX, StringUtils.EMPTY);
    this.tableNamePrefix = props.getProp(HIVE_TABLE_NAME_PREFIX, StringUtils.EMPTY);
    this.tableNameSuffix = props.getProp(HIVE_TABLE_NAME_SUFFIX, StringUtils.EMPTY);
    this.emptyInputPathFlag = props.getPropAsBoolean(MAPREDUCE_JOB_INPUT_PATH_EMPTY_KEY, false);
    this.metricContext = Instrumented.getMetricContext(props, HiveRegister.class);

    // Get Topic-specific config object doesn't require any runtime-set properties in prop object, safe to initialize
    // in constructor.
    if (this.props.getProperties().containsKey(KafkaSource.TOPIC_NAME)) {
      Timer.Context context = this.metricContext.timer(CONFIG_FOR_TOPIC_TIMER).time();
      configForTopic =
          ConfigStoreUtils.getConfigForTopic(this.props.getProperties(), KafkaSource.TOPIC_NAME, this.configClient);
      context.close();
    }
  }

  /**
   * This method first tries to obtain the database name from {@link #HIVE_DATABASE_NAME}.
   * If this property is not specified, it then tries to obtain the database name using
   * the first group of {@link #HIVE_DATABASE_REGEX}.
   *
   */
  protected Optional<String> getDatabaseName(Path path) {
    if (!this.props.contains(HIVE_DATABASE_NAME) && !this.props.contains(HIVE_DATABASE_REGEX)) {
      return Optional.<String> absent();
    }

    return Optional.<String> of(
        this.dbNamePrefix + getDatabaseOrTableName(path, HIVE_DATABASE_NAME, HIVE_DATABASE_REGEX, this.dbNamePattern)
            + this.dbNameSuffix);
  }

  /**
   * Obtain Hive database names. The returned {@link Iterable} contains the database name returned by
   * {@link #getDatabaseName(Path)} (if present) plus additional database names specified in
   * {@link #ADDITIONAL_HIVE_DATABASE_NAMES}.
   * Note that the dataset-specific configuration will overwrite job configuration for the value of
   * {@link #ADDITIONAL_HIVE_DATABASE_NAMES}
   *
   */
  protected Iterable<String> getDatabaseNames(Path path) {
    List<String> databaseNames = Lists.newArrayList();

    Optional<String> databaseName;
    if ((databaseName = getDatabaseName(path)).isPresent()) {
      databaseNames.add(databaseName.get());
    }

    if (configForTopic.isPresent() && configForTopic.get().hasPath(ADDITIONAL_HIVE_DATABASE_NAMES)) {
      databaseNames.addAll(ConfigUtils.getStringList(configForTopic.get(), ADDITIONAL_HIVE_DATABASE_NAMES).stream()
          .map(x -> this.dbNamePrefix + x + this.dbNameSuffix).collect(Collectors.toList()));
    } else if (!Strings.isNullOrEmpty(this.props.getProp(ADDITIONAL_HIVE_DATABASE_NAMES))) {
      for (String additionalDbName : this.props.getPropAsList(ADDITIONAL_HIVE_DATABASE_NAMES)) {
        databaseNames.add(this.dbNamePrefix + additionalDbName + this.dbNameSuffix);
      }
    }

    Preconditions.checkState(!databaseNames.isEmpty(), "Hive database name not specified");
    return databaseNames;
  }

  /**
   * This method first tries to obtain the table name from {@link #HIVE_TABLE_NAME}.
   * If this property is not specified, it then tries to obtain the table name using
   * the first group of {@link #HIVE_TABLE_REGEX}.
   */
  protected Optional<String> getTableName(Path path) {
    if (!this.props.contains(HIVE_TABLE_NAME) && !this.props.contains(HIVE_TABLE_REGEX)) {
      return Optional.<String> absent();
    }
    return Optional.<String> of(
        this.tableNamePrefix + getDatabaseOrTableName(path, HIVE_TABLE_NAME, HIVE_TABLE_REGEX, this.tableNamePattern)
            + this.tableNameSuffix);
  }

  /***
   * Obtain Hive table names.
   *
   * The returned {@link Iterable} contains:
   *  1. Table name returned by {@link #getTableName(Path)}
   *  2. Table names specified by <code>additional.hive.table.names</code>
   *
   * In table names above, the {@value PRIMARY_TABLE_TOKEN} if present is also replaced by the
   * table name obtained via {@link #getTableName(Path)}.
   *
   * @param path Path for the table on filesystem.
   * @return Table names to register.
   */
  protected Iterable<String> getTableNames(Path path) {
    List<String> tableNames = getTableNames(Optional.<String>absent(), path);

    Preconditions.checkState(!tableNames.isEmpty(), "Hive table name not specified");
    return tableNames;
  }

  /***
   * Obtain Hive table names filtered by <code>dbPrefix</code> (if present).
   *
   * The returned {@link List} contains:
   *  A. If <code>dbPrefix</code> is absent:
   *    1. Table name returned by {@link #getTableName(Path)}
   *    2. Table names specified by <code>additional.hive.table.names</code>
   *  B. If dbPrefix is present:
   *    1. Table names specified by <code>dbPrefix.hive.table.names</code>
   *
   * In table names above, the {@value PRIMARY_TABLE_TOKEN} if present is also replaced by the
   * table name obtained via {@link #getTableName(Path)}.
   *
   * @param dbPrefix Prefix to the property <code>additional.table.names</code>, to obtain table names only
   *                         for the specified db. Eg. If <code>dbPrefix</code> is db, then
   *                         <code>db.hive.table.names</code> is the resolved property name.
   * @param path Path for the table on filesystem.
   * @return Table names to register.
   */
  protected List<String> getTableNames(Optional<String> dbPrefix, Path path) {
    List<String> tableNames = Lists.newArrayList();

    Optional<String> primaryTableName;
    if ((primaryTableName = getTableName(path)).isPresent() && !dbPrefix.isPresent()) {
      tableNames.add(primaryTableName.get());
    }

    String additionalNamesProp;
    if (dbPrefix.isPresent()) {
      additionalNamesProp = String.format("%s.%s", dbPrefix.get(), HIVE_TABLE_NAME);
    } else {
      additionalNamesProp = ADDITIONAL_HIVE_TABLE_NAMES;
    }

    // Searching additional table name from ConfigStore-returned object.
    if (primaryTableName.isPresent() && configForTopic.isPresent() && configForTopic.get().hasPath(additionalNamesProp)) {
      for (String additionalTableName : ConfigUtils.getStringList(configForTopic.get(), additionalNamesProp)) {
        String resolvedTableName =
            StringUtils.replace(additionalTableName, PRIMARY_TABLE_TOKEN, primaryTableName.get());
        tableNames.add(this.tableNamePrefix + resolvedTableName + this.tableNameSuffix);
      }
    } else if (!Strings.isNullOrEmpty(this.props.getProp(additionalNamesProp))) {
      for (String additionalTableName : this.props.getPropAsList(additionalNamesProp)) {
        String resolvedTableName =
            primaryTableName.isPresent() ? StringUtils.replace(additionalTableName, PRIMARY_TABLE_TOKEN,
                primaryTableName.get()) : additionalTableName;
        tableNames.add(this.tableNamePrefix + resolvedTableName + this.tableNameSuffix);
      }
    }

    return tableNames;
  }

  protected String getDatabaseOrTableName(Path path, String nameKey, String regexKey, Optional<Pattern> pattern) {
    String name;
    if (this.props.contains(nameKey)) {
      name = this.props.getProp(nameKey);
    } else if (pattern.isPresent()) {
      Matcher matcher = pattern.get().matcher(path.toString());
      if (matcher.matches() && matcher.groupCount() >= 1) {
        name = matcher.group(1);
      } else {
        throw new IllegalStateException("No group match found for regexKey " + regexKey+" with regexp "+ pattern.get().toString() +" on path "+path);
      }
    } else {
      throw new IllegalStateException("Missing required property " + nameKey + " or " + regexKey);
    }

    return sanitizeAndValidateName(name);
  }

  protected String sanitizeAndValidateName(String name) {
    name = name.toLowerCase();

    if (this.sanitizeNameAllowed && !isNameValid(name)) {
      name = sanitizeName(name);
    }

    if (isNameValid(name)) {
      return name;
    }
    throw new IllegalStateException(name + " is not a valid Hive database or table name");
  }

  /**
   * A base implementation for creating {@link HiveTable}s given a {@link Path}.
   *
   * <p>
   *   This method returns a list of {@link Hivetable}s that contains one table per db name
   *   (returned by {@link #getDatabaseNames(Path)}) and table name (returned by {@link #getTableNames(Path)}.
   * </p>
   *
   * @param path a {@link Path} used to create the {@link HiveTable}.
   * @return a list of {@link HiveTable}s for the given {@link Path}.
   * @throws IOException
   */
  protected List<HiveTable> getTables(Path path) throws IOException {
    List<HiveTable> tables = Lists.newArrayList();

    for (String databaseName : getDatabaseNames(path)) {
      // Get tables to register ONLY for this Hive database (specified via prefix filter in properties)
      boolean foundTablesViaDbFilter = false;
      for (String tableName : getTableNames(Optional.of(databaseName), path)) {
        tables.add(getTable(path, databaseName, tableName));
        foundTablesViaDbFilter = true;
      }

      // If no tables found via db filter, get tables to register in all Hive databases and add them for this database
      if (!foundTablesViaDbFilter) {
        for (String tableName : getTableNames(path)) {
          tables.add(getTable(path, databaseName, tableName));
        }
      }
    }
    return tables;
  }

  /**
   * A base implementation for creating a non bucketed, external {@link HiveTable} for a {@link Path}.
   *
   * @param path a {@link Path} used to create the {@link HiveTable}.
   * @param dbName the database name for the created {@link HiveTable}.
   * @param tableName the table name for the created {@link HiveTable}.
   * @return a {@link HiveTable}s for the given {@link Path}.
   * @throws IOException
   */
  protected HiveTable getTable(Path path, String dbName, String tableName) throws IOException {

    HiveTable.Builder tableBuilder = new HiveTable.Builder().withDbName(dbName).withTableName(tableName);

    if (!this.emptyInputPathFlag) {
      tableBuilder = tableBuilder.withSerdeManaager(HiveSerDeManager.get(this.props));
    }
    HiveTable table = tableBuilder.build();
    table.setLocation(this.fs.makeQualified(getTableLocation(path)).toString());

    if (!this.emptyInputPathFlag) {
      table.setSerDeProps(path);
    }

    // Setting table-level props.
    table.setProps(getRuntimePropsEnrichedTblProps());

    table.setStorageProps(this.props.getStorageProps());
    table.setSerDeProps(this.props.getSerdeProps());
    table.setNumBuckets(-1);
    table.setBucketColumns(Lists.<String> newArrayList());
    table.setTableType(TableType.EXTERNAL_TABLE.toString());
    return table;
  }

  /**
   * Enrich the table-level properties with properties carried over from ingestion runtime.
   * Extend this class to add more runtime properties if required.
   */
  protected State getRuntimePropsEnrichedTblProps() {
    State tableProps = new State(this.props.getTablePartitionProps());
    if (this.props.getRuntimeTableProps().isPresent()){
      tableProps.setProp(HiveMetaStoreUtils.RUNTIME_PROPS, this.props.getRuntimeTableProps().get());
    }
    return tableProps;
  }

  protected Optional<HivePartition> getPartition(Path path, HiveTable table) throws IOException {
    return Optional.<HivePartition> absent();
  }

  protected Path getTableLocation(Path path) {
    return path;
  }

  /**
   * Determine whether a database or table name is valid.
   *
   * A name is valid if and only if: it starts with an alphanumeric character, contains only alphanumeric characters
   * and '_', and is NOT composed of numbers only.
   */
  protected static boolean isNameValid(String name) {
    Preconditions.checkNotNull(name);
    name = name.toLowerCase();
    return VALID_DB_TABLE_NAME_PATTERN_1.matcher(name).matches()
        && VALID_DB_TABLE_NAME_PATTERN_2.matcher(name).matches();
  }

  /**
   * Attempt to sanitize an invalid database or table name by replacing characters that are not alphanumeric
   * or '_' with '_'.
   */
  protected static String sanitizeName(String name) {
    return name.replaceAll("[^a-zA-Z0-9_]", "_");
  }

  @Override
  public Collection<HiveSpec> getHiveSpecs(Path path) throws IOException {
    List<HiveSpec> specs = Lists.newArrayList();
    for (HiveTable table : getTables(path)) {
      specs.add(new SimpleHiveSpec.Builder<>(path).withTable(table).withPartition(getPartition(path, table)).build());
    }
    return specs;
  }

  /**
   * Get a {@link HiveRegistrationPolicy} from a {@link State} object.
   *
   * @param props A {@link State} object that contains property, {@link #HIVE_REGISTRATION_POLICY},
   * which is the class name of the desired policy. This policy class must have a constructor that
   * takes a {@link State} object.
   */
  public static HiveRegistrationPolicy getPolicy(State props) {
    Preconditions.checkArgument(props.contains(ConfigurationKeys.HIVE_REGISTRATION_POLICY));

    String policyType = props.getProp(ConfigurationKeys.HIVE_REGISTRATION_POLICY);
    try {
      return (HiveRegistrationPolicy) ConstructorUtils.invokeConstructor(Class.forName(policyType), props);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(
          "Unable to instantiate " + HiveRegistrationPolicy.class.getSimpleName() + " with type " + policyType, e);
    }
  }
}