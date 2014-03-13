package com.linkedin.uif.source.workunit;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import com.google.common.base.Joiner;
import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.SourceState;
import com.linkedin.uif.configuration.State;
import com.linkedin.uif.configuration.WorkUnitState;

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
public class Extract extends State {

  public enum TableType {
    SNAPSHOT_ONLY,
    SNAPSHOT_APPEND,
    APPEND_ONLY
  }
  
  private static SimpleDateFormat DTF = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US);
  private State previousTableState = new State();
  
  static {
    DTF.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

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
  public Extract(SourceState state, TableType type, String namespace, String table) {
    // values should only be null for deserialization.
    if (state != null && type != null && namespace != null && table != null){
      String extractId = DTF.format(new Date());
      super.addAll(state);
      super.setProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY, type.toString());
      super.setProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY, namespace);
      super.setProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, table);
      super.setProp(ConfigurationKeys.EXTRACT_EXTRACT_ID_KEY, extractId);

      if (state.getPreviousStates() != null) {
        for (WorkUnitState pre : state.getPreviousStates()){
          Extract previousExtract = pre.getWorkunit().getExtract();
          if (previousExtract.getNamespace().equals(namespace) && previousExtract.getTable().equals(table)){
            previousTableState.addAll(pre);
          }
        }
      }
    }
  }


  /**
   * deep copy constructor
   *
   * @param extract
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
      return this.getExtractId().equals(otherExtract.getExtractId());
  }
  
  /**
   * indicates snapshot or append or snapshot data with an append only changelog
   * @return
   */
  public TableType getType() {
    return TableType.valueOf(getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY));
  }

  /**
   * dot seperated namespace path
   *
   * @return
   */
  public String getNamespace() {
    return getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY, "");
  }

  /**
   * name of the table
   *
   * @return
   */
  public String getTable() {
    return getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, "");
  }

  /**
   * unique id for every extract belonging to a specific table, not globally
   * unique
   *
   * @return
   */
  public String getExtractId() {
    return getProp(ConfigurationKeys.EXTRACT_EXTRACT_ID_KEY, "");
  }

  /**
   * true if this extract represents the full contents of the source table
   *
   * @return
   */
  public boolean getIsFull() {
    return getPropAsBoolean(ConfigurationKeys.EXTRACT_IS_FULL_KEY, false);
  }

  /**
   * Timestamp for when the full extract was pulled, usually the time the
   * extract began
   *
   * @return
   */
  public long getFullExtractRunTime() {
    return getPropAsLong(ConfigurationKeys.EXTRACT_FULL_RUN_TIME_KEY, -1);
  }

  protected void setFullTrue() {
    setProp(ConfigurationKeys.EXTRACT_IS_FULL_KEY, true);
  }

  /**
   * only required if this extract is a full drop
   *
   * @param extractFullRunTime required for setting full to true
   */
  public void setFullTrue(long extractFullRunTime) {
    setFullTrue();
    setProp(ConfigurationKeys.EXTRACT_FULL_RUN_TIME_KEY, extractFullRunTime);
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
    setProp(ConfigurationKeys.EXTRACT_RECORD_COUNT_KEY, count);
    setProp(ConfigurationKeys.EXTRACT_RECORD_COUNT_ESTIMATED_KEY, estimated);
  }

  public long getRecordCount() {
    return getPropAsLong(ConfigurationKeys.EXTRACT_RECORD_COUNT_KEY, -1);
  }

  public boolean getIsRecordCountEstimated() {
    return getPropAsBoolean(ConfigurationKeys.EXTRACT_RECORD_COUNT_ESTIMATED_KEY, false);
  }

  /**
   * optional count taken of source table and valid as of a specific high
   * water mark
   *
   * @param count
   */
  public void setValidationRecordCount(long count, long hwm) {
    setProp(ConfigurationKeys.EXTRACT_VALIDATION_RECORD_COUNT_KEY, count);
    setProp(ConfigurationKeys.EXTRACT_VALIDATION_RECORD_COUNT_HWM_KEY, hwm);
  }

  public long getValidationRecordCount() {
    return getPropAsLong(ConfigurationKeys.EXTRACT_VALIDATION_RECORD_COUNT_KEY, -1);
  }

  public long getValidationRecordCountHighWaterMark() {
    return getPropAsLong(ConfigurationKeys.EXTRACT_VALIDATION_RECORD_COUNT_HWM_KEY, -1);
  }

  /**
   * optional if true then multiple tables from different namespaces will be
   * considered shards of the same table and therefore merged under a single namespace
   *
   * @param realNameSpace namespace table should be merged under
   */
  public void setShardedTrue(String realNameSpace) {
    setProp(ConfigurationKeys.EXTRACT_IS_SHARDED_KEY, true);
    setProp(ConfigurationKeys.EXTRACT_SHARDED_REAL_NAMESPACE_KEY, realNameSpace);
  }

  public boolean getIsSharded() {
    return getPropAsBoolean(ConfigurationKeys.EXTRACT_IS_SHARDED_KEY, false);
  }

  public String getShardedRealNameSpace(){
    return getProp(ConfigurationKeys.EXTRACT_SHARDED_REAL_NAMESPACE_KEY);
  }

  public boolean getIsSecured() {
    return getPropAsBoolean(ConfigurationKeys.EXTRACT_IS_SECURED_KEY, false);
  }

  /**
   * optional if set then all output files will be restricted to users
   * belonging to the specified group
   *
   * @param permissionGroup group is required for setting secured
   */
  public void setIsSecured(String permissionGroup) {
    setProp(ConfigurationKeys.EXTRACT_IS_SECURED_KEY, true);
    setProp(ConfigurationKeys.EXTRACT_SECURITY_PERMISSION_GROUP_KEY, permissionGroup);
  }

  public String getSecurityPermissionGroup(){
    return getProp(ConfigurationKeys.EXTRACT_SECURITY_PERMISSION_GROUP_KEY);
  }

  /**
   * sets all the primary keys without regard to order
   *
   * @param primaryKeyFieldName
   *            dot seperated name for deeply nested keys.
   */
  public void setPrimaryKeys(String... primaryKeyFieldName) {
    setProp(ConfigurationKeys.EXTRACT_PRIMARY_KEY_FIELDS_KEY, Joiner.on(",").join(primaryKeyFieldName));
  }

  /**
   * add one primary key to the existing primary keys
   *
   * @param primaryKeyFieldName
   *            dot seperated name for deeply nested keys.
   */
  public void addPrimaryKey(String... primaryKeyFieldName) {
    StringBuilder sb = new StringBuilder(getProp(ConfigurationKeys.EXTRACT_PRIMARY_KEY_FIELDS_KEY, ""));
    Joiner.on(",").appendTo(sb, primaryKeyFieldName);
    setProp(ConfigurationKeys.EXTRACT_PRIMARY_KEY_FIELDS_KEY, sb.toString());
  }

  public List<String> getPrimaryKeys() {
    return getPropAsList(ConfigurationKeys.EXTRACT_PRIMARY_KEY_FIELDS_KEY);
  }

  /**
   * sets all the delta fields and order matters
   *
   * @param deltaFieldName
   *            dot seperated name for deeply nested delta fields.
   */
  public void setDeltaFields(String... deltaFieldName) {
    setProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY, Joiner.on(",").join(deltaFieldName));
  }

  /**
   * adds one delta field to the end of the existing delta fields.
   *
   * @param deltaFieldName
   *            dot seperated name for deeply nested delta fields.
   */
  public void addDeltaField(String... deltaFieldName) {
    StringBuilder sb = new StringBuilder(getProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY, ""));
    Joiner.on(",").appendTo(sb, deltaFieldName);
    setProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY, sb.toString());
  }

  public List<String> getDeltaFields() {
    return getPropAsList(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY);
  }
  
  public State getPreviousTableState() {
    return previousTableState;
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

    switch (getType())
    {
      case SNAPSHOT_ONLY:
        if (getFullExtractRunTime() == -1)
          throw new MissingExtractAttributeException(
              "Full snapshot tables require a timestamp for full drops.  In most cases, this is when the full extract was started. "
                  + "You can set the timestamp using " + Extract.class.getName() + ".setFullTrue(long extractFullRunTime)");
        break;
      case APPEND_ONLY:
        try {
          getDeltaFields();
        } catch (NullPointerException e) {
          throw new MissingExtractAttributeException(
              "Append tables require at least one delta field in order to partition the data according to data/time.  This should be a timestamp or something equivelent. "
                  + "You can set the delta field(s) using one of the setters on " + Extract.class.getName());

        }
        break;
      case SNAPSHOT_APPEND:
        // primary keys always required
        try {
          getPrimaryKeys();
        } catch (NullPointerException e) {
          throw new MissingExtractAttributeException(
              "SnapshotAppend tables require a primary key in order to apply the changelog.  "
                  + "If this table won't have a change log, consider using a different table type. "
                  + "You can set the primary key(s) using one of the setters on " + Extract.class.getName());
        }

        if (getIsFull() && getFullExtractRunTime() == -1)
          throw new MissingExtractAttributeException(
              "SnapshotAppend tables require a timestamp for full drops.  In most cases, this is when the full extract was started. "
                  + "You can set the timestamp using " + Extract.class.getName() + ".setFullTrue(long extractFullRunTime)");

        // due to legacy requirements, full snapshots don't require delta
        // fields and subsequent incremental extracts will indicate the delta field
        if (!getIsFull()) {
          try {
            getDeltaFields();
          } catch (NullPointerException e) {
            throw new MissingExtractAttributeException(
                "SnapshotAppend tables require at least one delta field in order to apply the changelog.  This could be a timestamp, transactionId, scn, or something equivelent. "
                    + "You can set the delta field(s) using one of the setters on " + Extract.class.getName());

          }
        }
    }
  }
}
