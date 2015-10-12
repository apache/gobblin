/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.compaction;

import static gobblin.compaction.Dataset.DatasetState.*;

import java.util.List;

import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

import gobblin.configuration.State;


/**
 * A class that represents a dataset whose data should be compacted.
 *
 * @author ziliu
 */
public class Dataset implements Comparable<Dataset> {

  private static final double DEFAULT_PRIORITY = 1.0;
  private static final double DEFAULT_PRIORITY_REDUCTION_FACTOR = 1.0 / 3.0;

  public enum DatasetState {

    // The data completeness of this dataset has not been verified.
    UNVERIFIED,

    // The data completeness of this dataset has been verified.
    VERIFIED,

    // The data completeness of this dataset has timed out. In this case it is configurable whether the
    // compactor should or should not compact this dataset.
    GIVEN_UP,

    // Compaction of this data set has been completed (which may have either succeeded or failed).
    COMPACTION_COMPLETE
  }

  private final Path path;
  private final State jobProps;
  private final List<Throwable> throwables;
  private double priority;
  private DatasetState state;

  public Dataset(Path path, State jobProps) {
    this(path, jobProps, DEFAULT_PRIORITY);
  }

  public Dataset(Path path, State jobProps, double priority) {
    this.path = path;
    this.jobProps = jobProps;
    this.throwables = Lists.newArrayList();
    this.priority = priority;
    this.state = UNVERIFIED;
  }

  public Dataset(Dataset other) {
    this.path = other.path;
    this.jobProps = other.jobProps;
    this.throwables = other.throwables;
    this.priority = other.priority;
    this.state = other.state;
  }

  public Path path() {
    return this.path;
  }

  public double priority() {
    return this.priority;
  }

  public DatasetState state() {
    return this.state;
  }

  /**
   * Reduce the priority of the dataset by {@link #DEFAULT_PRIORITY_REDUCTION_FACTOR}.
   * @return the reduced priority
   */
  public double reducePriority() {
    return reducePriority(DEFAULT_PRIORITY_REDUCTION_FACTOR);
  }

  /**
   * Reduce the priority of the dataset.
   * @param reductionFactor the reduction factor. The priority will be reduced by reductionFactor.
   * @return the reduced priority
   */
  public double reducePriority(double reductionFactor) {
    this.priority *= 1.0 - reductionFactor;
    return this.priority;
  }

  public void setState(DatasetState state) {
    this.state = state;
  }

  public State jobProps() {
    return this.jobProps;
  }

  public List<Throwable> throwables() {
    return this.throwables;
  }

  public synchronized void addThrowable(Throwable t) {
    this.throwables.add(t);
  }

  @Override
  public int compareTo(Dataset o) {
    return Double.compare(o.priority, this.priority);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((path == null) ? 0 : path.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof Dataset)) {
      return false;
    }
    Dataset other = (Dataset) obj;
    if (path == null) {
      if (other.path != null) {
        return false;
      }
    } else if (!path.equals(other.path)) {
      return false;
    }
    return true;
  }

  /**
   * @return the {@link Path} of the {@link Dataset}.
   */
  @Override
  public String toString() {
    return this.path.toString();
  }
}
