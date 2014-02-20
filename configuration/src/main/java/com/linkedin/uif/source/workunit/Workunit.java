package com.linkedin.uif.source.workunit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.linkedin.uif.configuration.SourceState;
import com.linkedin.uif.configuration.State;


/**
 * Represents the definition for a finite pull task
 * @author kgoodhop
 *
 */
public class WorkUnit extends State {
  public enum TableType {
    SNAPSHOT_ONLY,
    SNAPSHOT_APPEND,
    APPEND_ONLY
  }
  TableType type;
  private Table table;

  public WorkUnit() {
  }

  /**
   * Constructor
   *
   * @param state all properties will be copied into this workunit
   * @param type append snapshot or both
   * @param namespace dot seperated namespace path
   * @param table dataset name
   * @param extractId unique id for each extract for a namespace table
   */
  public WorkUnit(SourceState state, TableType type, String namespace, String table, String extractId) {
    this.addAll(state);
    this.type = type;

    switch (type) {
      case SNAPSHOT_ONLY:
        this.table = new SnapshotOnlyTable(namespace, table, extractId);
        break;
      case SNAPSHOT_APPEND:
        this.table = new SnapshotAppendTable(namespace, table, extractId);
        break;
      case APPEND_ONLY:
        this.table = new AppendOnlyTable(namespace, table, extractId);
        break;
    }
  }

  /**
   * indicates snapshot or append or snapshot data with an append only changelog
   * @return
   */
  public TableType getType() {
    return type;
  }

  /**
   * Attributes object for differing pull types.
   * @return
   */
  public Table getTable() {
    return table;
  }

  public long getHighWaterMark() {
    return getPropAsLong("workunit.high.water.mark");
  }

  public void setHighWaterMark(long highWaterMark) {
    setProp("workunit.high.water.mark", highWaterMark);
  }

  public long getLowWaterMark() {
    return getPropAsLong("workunit.low.water.mark");
  }

  public void setLowWaterMark(long lowWaterMark) {
    setProp("workunit.low.water.mark", lowWaterMark);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.table.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    this.table.write(out);
  }
}
