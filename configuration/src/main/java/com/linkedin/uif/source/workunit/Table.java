package com.linkedin.uif.source.workunit;

import java.util.List;

import com.google.common.base.Joiner;
import com.linkedin.uif.configuration.State;


/**
 * Class representing all the base attributes required by all tables types.
 * Subclasses will be expected to validate each table type for their respective
 * required attributes.
 *
 * extractId only needs to be unique for extracts belonging to the same
 * namespace.table. One or more workunits can share the same extractId.
 * Workunits that do share an extractId will be considered parts of single
 * extract for the purpose of applying publishing policies.
 *
 * @author kgoodhop
 *
 */
public class Table extends State {

  /**
   * Constructor
   *
   * @param namespace
   *            dot seperated namespace path
   * @param table
   *            table name
   * @param extractId
   *            unique id for each extract
   */
  public Table(String namespace, String table, String extractId) {
    super.setProp("extract.namespace.name", namespace);
    super.setProp("extract.table.name", table);
    super.setProp("extract.extract.id", extractId);
  }

  /**
   * deep copy constructor
   *
   * @param table
   */
  public Table(Table table) {
    addAll(table);
  }

  /**
   * dot seperated namespace path
   *
   * @return
   */
  public String getNamespace() {
    return getProp("extract.namespace.name", "");
  }

  /**
   * name of the table
   *
   * @return
   */
  public String getTable() {
    return getProp("extract.table.name", "");
  }

  /**
   * unique id for every extract belonging to a specific table, not globally
   * unique
   *
   * @return
   */
  public String getExtractId() {
    return getProp("extract.extract.id", "");
  }

  /**
   * true if this extract represents the full contents of the source table
   *
   * @return
   */
  public boolean getIsFull() {
    return getPropAsBoolean("extract.is.full", false);
  }

  /**
   * Timestamp for when the full extract was pulled, usually the time the
   * extract began
   *
   * @return
   */
  public long getFullExtractRunTime() {
    return getPropAsLong("extract.full.run.time", -1);
  }

  protected void setFullTrue() {
    setProp("extract.is.full", true);
  }

  /**
   * only required if this extract is a full drop
   *
   * @param extractFullRunTime required for setting full to true
   */
  public void setFullTrue(long extractFullRunTime) {
    setFullTrue();
    setProp("extract.full.run.time", extractFullRunTime);
  }

  /**
   * optional and represents the LWM across all workunits for this extract
   *
   * @param lwm
   */
  public void setExtractLowWaterMark(long lwm) {
    setProp("extract.low.water.mark", lwm);
  }

  public long getExtractLowWaterMark() {
    return getPropAsLong("extract.low.water.mark", -1);
  }

  /**
   * optional and represents the HWM across all workunits for this extract
   *
   * @param lwm
   */
  public void setExtractHighWaterMark(long hwm) {
    setExtractHighWaterMark(hwm);
  }

  /**
   * optional and represents the HWM across all workunits for this extract
   *
   * @param lwm
   * @param estimated
   *            true if HWM is only estimated
   */
  public void setExtractHighWaterMark(long hwm, boolean estimated) {
    setProp("extract.high.water.mark", hwm);
    setProp("extract.high.water.mark.estimated", estimated);
  }

  public long getExtractHighWaterMark() {
    return getPropAsLong("extract.high.water.mark", -1);
  }

  public boolean getIsExtractHighWaterMarkEstimated() {
    return getPropAsBoolean("extract.high.water.mark.estimated", false);
  }

  /**
   * optional and indicates the total number of records in this extract
   *
   * @param count
   */
  public void setRecordCount(long count) {
    setRecordCount(count, false);
  }

  /**
   * optional and indicates the total number of records in this extract
   *
   * @param count
   * @param estimated
   *            indicates if a count is only estimated
   */
  public void setRecordCount(long count, boolean estimated) {
    setProp("extract.record.count", count);
    setProp("extract.record.count.estimated", estimated);
  }

  public long getRecordCount() {
    return getPropAsLong("extract.record.count", -1);
  }

  public boolean getIsRecordCountEstimated() {
    return getPropAsBoolean("extract.record.count.estimated", false);
  }

  /**
   * optional count taken of source table and valid as of a specific high
   * water mark
   *
   * @param count
   */
  public void setValidationRecordCount(long count, long hwm) {
    setProp("extract.validation.record.count", count);
    setProp("extract.validation.record.count.high.water.mark", hwm);
  }

  public long getValidationRecordCount() {
    return getPropAsLong("extract.validation.record.count", -1);
  }

  public long getValidationRecordCountHighWaterMark() {
    return getPropAsLong("extract.validation.record.count.high.water.mark", -1);
  }

  /**
   * optional if true then multiple tables from different namespaces will be
   * considered shards of the same table and therefore merged.
   *
   * @param isSharded
   */
  public void setIsSharded(boolean isSharded) {
    setProp("extract.is.sharded", isSharded);
  }

  public boolean getIsSharded() {
    return getPropAsBoolean("extract.is.sharded", false);
  }

  public boolean getIsSecured() {
    return getPropAsBoolean("extract.is.secured", false);
  }

  /**
   * optional if set then all output files will be restricted to users
   * belonging to the specified group
   *
   * @param permissionGroup group is required for setting secured
   */
  public void setIsSecured(String permissionGroup) {
    setProp("extract.is.secured", true);
    setProp("extract.security.permission.group", permissionGroup);
  }

  /**
   * sets all the primary keys without regard to order
   *
   * @param primaryKeyFieldName
   *            dot seperated name for deeply nested keys.
   */
  public void setPrimaryKeys(String... primaryKeyFieldName) {
    setProp("extract.primary.key.fields", Joiner.on(",").join(primaryKeyFieldName));
  }

  /**
   * add one primary key to the existing primary keys
   *
   * @param primaryKeyFieldName
   *            dot seperated name for deeply nested keys.
   */
  public void addPrimaryKey(String... primaryKeyFieldName) {
    StringBuilder sb = new StringBuilder(getProp("extract.primary.key.fields", ""));
    Joiner.on(",").appendTo(sb, primaryKeyFieldName);
    setProp("extract.primary.key.fields", sb.toString());
  }

  public List<String> getPrimaryKeys() {
    return getPropAsList("extract.primary.key.fields");
  }

  /**
   * sets all the delta fields and order matters
   *
   * @param deltaFieldName
   *            dot seperated name for deeply nested delta fields.
   */
  public void setDeltaFields(String... deltaFieldName) {
    setProp("extract.delta.fields", Joiner.on(",").join(deltaFieldName));
  }

  /**
   * adds one delta field to the end of the existing delta fields.
   *
   * @param deltaFieldName
   *            dot seperated name for deeply nested delta fields.
   */
  public void addDeltaField(String... deltaFieldName) {
    StringBuilder sb = new StringBuilder(getProp("extract.delta.fields", ""));
    Joiner.on(",").appendTo(sb, deltaFieldName);
    setProp("extract.delta.fields", sb.toString());
  }

  public List<String> getDeltaFields() {
    return getPropAsList("extract.delta.fields");
  }

  /**
   * Verifies that required properties have been set for specific table type.
   *
   * @return
   * @throws MissingExtractAttributeException
   */
  public void validateTableAttributes() throws MissingExtractAttributeException {
    if (getNamespace().isEmpty() || getTable().isEmpty() || getExtractId().isEmpty()) {
      throw new MissingExtractAttributeException("All extracts require a namespace, tableName, and extractId");
    }
  }
}
