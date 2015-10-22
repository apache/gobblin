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

import java.util.Collection;
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

  public static final double DEFAULT_PRIORITY = 1.0;
  public static final double DEFAULT_PRIORITY_REDUCTION_FACTOR = 1.0 / 3.0;

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

  public static class Builder {
    private String topic;
    private Path inputPath;
    private Path inputLatePath;
    private Path outputPath;
    private Path outputLatePath;
    private Path outputTmpPath;
    private double priority = DEFAULT_PRIORITY;

    public Builder withTopic(String topic) {
      this.topic = topic;
      return this;
    }

    public Builder withInputPath(Path inputPath) {
      this.inputPath = inputPath;
      return this;
    }

    public Builder withInputLatePath(Path inputLatePath) {
      this.inputLatePath = inputLatePath;
      return this;
    }

    public Builder withOutputPath(Path outputPath) {
      this.outputPath = outputPath;
      return this;
    }

    public Builder withOutputLatePath(Path outputLatePath) {
      this.outputLatePath = outputLatePath;
      return this;
    }

    public Builder withOutputTmpPath(Path outputTmpPath) {
      this.outputTmpPath = outputTmpPath;
      return this;
    }

    public Builder withPriority(double priority) {
      this.priority = priority;
      return this;
    }

    public Dataset build() {
      return new Dataset(this);
    }
  }

  private final String topic;
  private final Path inputPath;
  private final Path inputLatePath;
  private final Path outputPath;
  private final Path outputLatePath;
  private final Path outputTmpPath;
  private final List<Path> additionalInputPaths;
  private final List<Throwable> throwables;

  private State jobProps;
  private double priority;
  private DatasetState state;

  private Dataset(Builder builder) {
    this.topic = builder.topic;
    this.inputPath = builder.inputPath;
    this.inputLatePath = builder.inputLatePath;
    this.outputPath = builder.outputPath;
    this.outputLatePath = builder.outputLatePath;
    this.outputTmpPath = builder.outputTmpPath;
    this.additionalInputPaths = Lists.newArrayList();
    this.throwables = Lists.newArrayList();

    this.priority = builder.priority;
    this.state = DatasetState.UNVERIFIED;
  }

  /**
   * Name of the topic represented by this {@link Dataset}.
   */
  public String topic() {
    return this.topic;
  }

  /**
   * Input path that contains the data of this {@link Dataset} to be compacted.
   */
  public Path inputPath() {
    return this.inputPath;
  }

  /**
   * Path that contains the late data of this {@link Dataset} to be compacted.
   * Late input data may be generated if the input data is obtained from another compaction,
   * e.g., if we run hourly compaction and daily compaction on a topic where the compacted hourly
   * data is the input to the daily compaction.
   *
   * If this path contains any data and this {@link Dataset} is not already compacted, deduplication
   * will be applied to this {@link Dataset}.
   */
  public Path inputLatePath() {
    return this.inputLatePath;
  }

  /**
   * Output path for the compacted data.
   */
  public Path outputPath() {
    return this.outputPath;
  }

  /**
   * If {@link #outputPath()} is already compacted and new input data is found, those data can be copied
   * to this path.
   */
  public Path outputLatePath() {
    return this.outputLatePath;
  }

  /**
   * The path where the MR job writes output to. Data will be published to {@link #outputPath()} if the compaction
   * is successful.
   */
  public Path outputTmpPath() {
    return this.outputTmpPath;
  }

  /**
   * Additional paths of this {@link Dataset} besides {@link #inputPath()} that contain data to be compacted.
   */
  public List<Path> additionalInputPaths() {
    return this.additionalInputPaths;
  }

  /**
   * Add an additional input path for this {@link Dataset}.
   */
  public void addAdditionalInputPath(Path path) {
    this.additionalInputPaths.add(path);
  }

  /**
   * Add additional input paths for this {@link Dataset}.
   */
  public void addAdditionalInputPaths(Collection<Path> paths) {
    this.additionalInputPaths.addAll(paths);
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

  public void setJobProps(State jobProps) {
    this.jobProps = jobProps;
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
    result = prime * result + ((inputPath == null) ? 0 : inputPath.hashCode());
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
    if (inputPath == null) {
      if (other.inputPath != null) {
        return false;
      }
    } else if (!inputPath.equals(other.inputPath)) {
      return false;
    }
    return true;
  }

  /**
   * @return the {@link Path} of the {@link Dataset}.
   */
  @Override
  public String toString() {
    return this.inputPath.toString();
  }
}
