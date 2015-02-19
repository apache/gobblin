/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.workunit;

import java.util.List;
import java.util.Locale;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;


/**
 * A class representing all the base attributes required by all tables types. Subclasses
 * will be expected to validate each table type for their respective required attributes.
 *
 * <p>
 *   The extract ID only needs to be unique for {@link Extract}s belonging to the same
 *   namespace/table. One or more {@link WorkUnit}s can share the same extract ID.
 *   {@link WorkUnit}s that do share an extract ID will be considered parts of a single
 *   {@link Extract} for the purpose of applying publishing policies.
 * </p>
 *
 * @author kgoodhop
 *
 */
public class Extract extends State {

  public enum TableType {
    SNAPSHOT_ONLY,
    SNAPSHOT_APPEND,
    APPEND_ONLY
  }

  private static final DateTimeFormatter DTF =
      DateTimeFormat.forPattern("yyyyMMddHHmmss").withLocale(Locale.US).withZone(DateTimeZone.UTC);
  private final State previousTableState = new State();

  /**
   * Constructor.
   *
   * @param state a {@link SourceState} carrying properties needed to construct an {@link Extract}
   * @param namespace dot separated namespace path
   * @param type {@link TableType}
   * @param table table name
   */
  public Extract(SourceState state, TableType type, String namespace, String table) {
    // Values should only be null for deserialization
    if (state != null && type != null && !Strings.isNullOrEmpty(namespace) && !Strings.isNullOrEmpty(table)) {
      String extractId = DTF.print(new DateTime());
      super.addAll(state);
      super.setProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY, type.toString());
      super.setProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY, namespace);
      super.setProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, table);
      super.setProp(ConfigurationKeys.EXTRACT_EXTRACT_ID_KEY, extractId);

      for (WorkUnitState pre : state.getPreviousWorkUnitStates()) {
        Extract previousExtract = pre.getWorkunit().getExtract();
        if (previousExtract.getNamespace().equals(namespace) && previousExtract.getTable().equals(table)) {
          this.previousTableState.addAll(pre);
        }
      }

      // Setting full drop date if not already specified, the value can still be overridden if required.
      if (state.getPropAsBoolean(ConfigurationKeys.EXTRACT_IS_FULL_KEY) && !state
          .contains(ConfigurationKeys.EXTRACT_FULL_RUN_TIME_KEY)) {
        super.setProp(ConfigurationKeys.EXTRACT_FULL_RUN_TIME_KEY, System.currentTimeMillis());
      }
    }
  }

  /**
   * Deep copy constructor.
   *
   * @param extract the other {@link Extract} instance
   */
  public Extract(Extract extract) {
    addAll(extract);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Extract)) {
      return false;
    }
    Extract otherExtract = (Extract) other;
    return this.getNamespace().equals(otherExtract.getNamespace()) && this.getTable().equals(otherExtract.getTable())
        && this.getExtractId().equals(otherExtract.getExtractId());
  }

  @Override
  public int hashCode() {
    return (this.getNamespace() + this.getTable() + this.getExtractId()).hashCode();
  }

  /**
   * Get the writer output file path corresponding to this {@link Extract}.
   *
   * @return writer output file path corresponding to this {@link Extract}
   */
  public String getOutputFilePath() {
    return this.getNamespace().replaceAll("\\.", "/") + "/" +
        this.getTable() + "/" + this.getExtractId() + "_" +
        (this.getIsFull() ? "full" : "append");
  }

  /**
   * If this {@link Extract} has extract table type defined.
   *
   * @return <code>true</code> if it has, <code>false</code> otherwise.
   */
  public boolean hasType() {
    return contains(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY);
  }

  /**
   * Get the {@link TableType} of the table.
   *
   * @return {@link TableType} of the table
   */
  public TableType getType() {
    return TableType.valueOf(getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY));
  }

  /**
   * Get the dot-separated namespace of the table.
   *
   * @return dot-separated namespace of the table
   */
  public String getNamespace() {
    return getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY, "");
  }

  /**
   * Get the name of the table.
   *
   * @return name of the table
   */
  public String getTable() {
    return getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, "");
  }

  /**
   * Get a (non-globally) unique ID for this {@link Extract}.
   *
   * @return unique ID for this {@link Extract}
   */
  public String getExtractId() {
    return getProp(ConfigurationKeys.EXTRACT_EXTRACT_ID_KEY, "");
  }

  /**
   * Set a (non-globally) unique ID for this {@link Extract}.
   *
   * @param extractId unique ID for this {@link Extract}
   */
  public void setExtractId(String extractId) {
    setProp(ConfigurationKeys.EXTRACT_EXTRACT_ID_KEY, extractId);
  }

  /**
   * Check if this {@link Extract} represents the full contents of the source table.
   *
   * @return <code>true</code> if this {@link Extract} represents the full contents
   *         of the source table and <code>false</code> otherwise
   */
  public boolean getIsFull() {
    return getPropAsBoolean(ConfigurationKeys.EXTRACT_IS_FULL_KEY, false);
  }

  /**
   * Set full drop date from the given time.
   *
   * @param extractFullRunTime full extract time
   */
  public void setFullTrue(long extractFullRunTime) {
    setProp(ConfigurationKeys.EXTRACT_IS_FULL_KEY, true);
    setProp(ConfigurationKeys.EXTRACT_FULL_RUN_TIME_KEY, extractFullRunTime);
  }

  /**
   * Set primary keys.
   *
   * <p>
   *   The order of primary keys does not matter.
   * </p>
   *
   * @param primaryKeyFieldName primary key names
   */
  public void setPrimaryKeys(String... primaryKeyFieldName) {
    setProp(ConfigurationKeys.EXTRACT_PRIMARY_KEY_FIELDS_KEY, Joiner.on(",").join(primaryKeyFieldName));
  }

  /**
   * Add more primary keys to the existing set of primary keys.
   *
   * @param primaryKeyFieldName primary key names
   */
  public void addPrimaryKey(String... primaryKeyFieldName) {
    StringBuilder sb = new StringBuilder(getProp(ConfigurationKeys.EXTRACT_PRIMARY_KEY_FIELDS_KEY, ""));
    Joiner.on(",").appendTo(sb, primaryKeyFieldName);
    setProp(ConfigurationKeys.EXTRACT_PRIMARY_KEY_FIELDS_KEY, sb.toString());
  }

  /**
   * Get the list of primary keys.
   *
   * @return list of primary keys
   */
  public List<String> getPrimaryKeys() {
    return getPropAsList(ConfigurationKeys.EXTRACT_PRIMARY_KEY_FIELDS_KEY);
  }

  /**
   * Set delta fields.
   *
   * <p>
   *   The order of delta fields does not matter.
   * </p>
   *
   * @param deltaFieldName delta field names
   */
  public void setDeltaFields(String... deltaFieldName) {
    setProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY, Joiner.on(",").join(deltaFieldName));
  }

  /**
   * Add more delta fields to the existing set of delta fields.
   *
   * @param deltaFieldName delta field names
   */
  public void addDeltaField(String... deltaFieldName) {
    StringBuilder sb = new StringBuilder(getProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY, ""));
    Joiner.on(",").appendTo(sb, deltaFieldName);
    setProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY, sb.toString());
  }

  /**
   * Get the list of delta fields.
   *
   * @return list of delta fields
   */
  public List<String> getDeltaFields() {
    return getPropAsList(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY);
  }

  /**
   * Get the previous table {@link State}.
   *
   * @return previous table {@link State}
   */
  public State getPreviousTableState() {
    return previousTableState;
  }
}
