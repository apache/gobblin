package com.linkedin.uif.source.workunit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.SourceState;
import com.linkedin.uif.configuration.State;


/**
 * Represents the definition for a finite pull task
 * @author kgoodhop
 *
 */
public class WorkUnit extends State {

  private Extract extract;

  /**
   * Constructor
   *
   * @param state {@link SourceState} all properties will be copied into this workunit
   * @param extract {@link Extract}
   */
  public WorkUnit(SourceState state, Extract extract) {
    // values should only be null for deserialization.
    if (state != null)
      this.addAll(state);

    if (extract != null)
      this.extract = extract;
    else
      this.extract = new Extract(null, null, null, null, null);
  }

  /**
   * Copy constructor
   * @param other
   */
  public WorkUnit(WorkUnit other) {
    addAll(other);
    extract.addAll(other.getExtract());
  }

  /**
   * Attributes object for differing pull types.
   * @return
   */
  public Extract getExtract() {
    return extract;
  }

  public long getHighWaterMark() {
    return getPropAsLong(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY);
  }

  public void setHighWaterMark(long highWaterMark) {
    setProp(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY, highWaterMark);
  }

  public long getLowWaterMark() {
    return getPropAsLong(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY);
  }

  public void setLowWaterMark(long lowWaterMark) {
    setProp(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY, lowWaterMark);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.extract.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    this.extract.write(out);
  }
}
