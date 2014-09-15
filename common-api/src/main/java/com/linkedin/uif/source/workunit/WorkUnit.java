package com.linkedin.uif.source.workunit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.SourceState;
import com.linkedin.uif.configuration.State;
import com.linkedin.uif.source.extractor.Extractor;


/**
 * <p>Represents the definition for a finite pull task.  An instance of WorkUnit
 * should contain all the properties {@link Extractor} needs to perform the pull
 * task.
 * </p>
 * 
 * @author kgoodhop
 *
 */
public class WorkUnit extends State {

  private Extract extract;

  /**
   * Default constructor
   */
  public WorkUnit() {
    this(null, null);
  }

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
      this.extract = new Extract(null, null, null, null);
  }

  /**
   * Copy constructor
   * @param other
   */
  public WorkUnit(WorkUnit other) {
    addAll(other);
    this.extract = other.getExtract();
  }

  /**
   * Attributes object for differing pull types.
   * @return {@link Extract}
   */
  public Extract getExtract() {
    return extract;
  }

  /**
   * getter for max water mark for this WorkUnit
   * @return
   */
  public long getHighWaterMark() {
    return getPropAsLong(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY);
  }

  /**
   * setter for max water mark for this WorkUnit
   * @param highWaterMark
   */
  public void setHighWaterMark(long highWaterMark) {
    setProp(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY, highWaterMark);
  }

  /**
   * getter for min water mark for this WorkUnit
   * @return
   */
  public long getLowWaterMark() {
    return getPropAsLong(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY);
  }

  /**
   * setter for min water mark for this WorkUnit
   * @param lowWaterMark
   */
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
