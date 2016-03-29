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

import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;

import gobblin.annotation.Alpha;


/**
 * A comparator between an existing {@link HiveRegistrationUnit} and a new {@link HiveRegistrationUnit}. It is
 * used to determine whether the existing {@link HiveRegistrationUnit} should be altered to match the new
 * {@link HiveRegistrationUnit}.
 *
 * <p>
 *   Since altering a Hive table/partition is relatively expensive, when registering a new table/partition, if the
 *   table/partition exists, it is usually beneficial to check whether the existing table/partition needs to be
 *   altered before altering it.
 * </p>
 *
 * <p>
 *   This class does <em>not</em> implement {@link java.util.Comparator} and does not conform to the contract of
 *   {@link java.util.Comparator}.
 * </p>
 *
 * <p>
 *   Sample usage:
 *
 *   <pre> {@code
 *     HiveRegistrationUnitComparator<?> comparator = new HiveRegistrationUnitComparator<>(existingTable, newTable);
 *     boolean needToUpdate = comparator.compareInputFormat().compareOutputFormat().compareNumBuckets()
 *      .compareIsCompressed().compareRawLocation().result();
 *     }}
 *   </pre>
 *
 *   Or to compare all fields:
 *
 *   <pre> {@code
 *     HiveRegistrationUnitComparator<?> comparator = new HiveRegistrationUnitComparator<>(existingTable, newTable);
 *     boolean needToUpdate = comparator.compareAll().result();
 *     }}
 *   </pre>
 * </p>
 *
 * @author ziliu
 */
@Alpha
public class HiveRegistrationUnitComparator<T extends HiveRegistrationUnitComparator<?>> {

  protected final HiveRegistrationUnit existingUnit;
  protected final HiveRegistrationUnit newUnit;

  protected boolean result = false;

  public HiveRegistrationUnitComparator(HiveRegistrationUnit existingUnit, HiveRegistrationUnit newUnit) {
    this.existingUnit = existingUnit;
    this.newUnit = newUnit;
  }

  /**
   * Compare the raw locations (without schema and authority).
   *
   * <p>
   *   This is useful since existing tables/partitions in the Hive metastore have absolute paths in the location
   *   property, but the new table/partition may have a raw path.
   * </p>
   */
  @SuppressWarnings("unchecked")
  public T compareRawLocation() {
    if (!this.result) {
      this.result |= (!new Path(existingUnit.getLocation().get()).toUri().getRawPath()
          .equals(new Path(newUnit.getLocation().get()).toUri().getRawPath()));
    }
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T compareInputFormat() {
    if (!this.result) {
      compare(this.existingUnit.getInputFormat(), this.newUnit.getInputFormat());
    }
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T compareOutputFormat() {
    if (!this.result) {
      compare(this.existingUnit.getOutputFormat(), this.newUnit.getOutputFormat());
    }
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T compareIsCompressed() {
    if (!this.result) {
      compare(this.existingUnit.getIsCompressed(), this.newUnit.getIsCompressed());
    }
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T compareNumBuckets() {
    if (!this.result) {
      compare(this.existingUnit.getNumBuckets(), this.newUnit.getNumBuckets());
    }
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T compareBucketCols() {
    if (!this.result) {
      compare(this.existingUnit.getBucketColumns(), this.newUnit.getBucketColumns());
    }
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T compareIsStoredAsSubDirs() {
    if (!this.result) {
      compare(this.existingUnit.getIsStoredAsSubDirs(), this.newUnit.getIsStoredAsSubDirs());
    }
    return (T) this;
  }

  /**
   * Compare all parameters.
   */
  @SuppressWarnings("unchecked")
  public T compareAll() {
    this.compareInputFormat().compareOutputFormat().compareIsCompressed().compareIsStoredAsSubDirs().compareNumBuckets()
        .compareBucketCols().compareRawLocation();
    return (T) this;
  }

  /**
   * Compare an existing value and a new value, and set {@link #result} accordingly.
   *
   * <p>
   *   This method returns false if newValue is absent (i.e., the existing value doesn't need to be updated).
   *   This is because when adding a table/partition to Hive, Hive automatically sets default values for
   *   some of the unspecified parameters. Therefore existingValue being present and newValue being absent
   *   doesn't mean the existing value needs to be updated.
   * </p>
   */
  protected <E> void compare(Optional<E> existingValue, Optional<E> newValue) {
    boolean different;
    if (!newValue.isPresent()) {
      different = false;
    } else {
      different = !existingValue.isPresent() || !existingValue.get().equals(newValue.get());
    }
    this.result |= different;
  }

  /**
   * Get the result of comparison.
   * @return true if the existing {@link HiveRegistrationUnit} needs to be altered, false otherwise.
   */
  public boolean result() {
    boolean resultCopy = result;
    this.result = false;
    return resultCopy;
  }

}
