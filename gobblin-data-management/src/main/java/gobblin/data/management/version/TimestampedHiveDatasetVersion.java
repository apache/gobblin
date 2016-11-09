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
package gobblin.data.management.version;

import lombok.EqualsAndHashCode;

import org.apache.hadoop.hive.ql.metadata.Partition;
import org.joda.time.DateTime;

/**
 * A {@link HiveDatasetVersion} where the version is a timestamp associated with the {@link Partition}. Usually this
 * is the create time or modification time.
 */
public class TimestampedHiveDatasetVersion extends TimestampedDatasetVersion implements HiveDatasetVersion {

  private final Partition partition;

  public TimestampedHiveDatasetVersion(DateTime version, Partition partition) {
    super(version, partition.getDataLocation());
    this.partition = partition;
  }

  @Override
  public Partition getPartition() {
    return this.partition;
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj) && obj instanceof TimestampedHiveDatasetVersion && compareTo((TimestampedHiveDatasetVersion) obj) == 0;
  }

  @Override
  public int hashCode() {
    return this.partition.hashCode() + super.hashCode();
  }
}
