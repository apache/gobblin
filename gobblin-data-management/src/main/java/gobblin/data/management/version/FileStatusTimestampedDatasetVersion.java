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

import java.util.Set;

import lombok.EqualsAndHashCode;

import org.apache.hadoop.fs.FileStatus;
import org.joda.time.DateTime;

import com.google.common.collect.Sets;

/**
 * A {@link TimestampedDatasetVersion} that is also aware of the {@link FileStatus}s of all its paths.
 */
@EqualsAndHashCode(callSuper=true)
public class FileStatusTimestampedDatasetVersion extends TimestampedDatasetVersion implements FileStatusAware {

  private final FileStatus fileStatus;
  public FileStatusTimestampedDatasetVersion(DateTime version, FileStatus fileStatus) {
    super(version, fileStatus.getPath());
    this.fileStatus = fileStatus;
  }

  @Override
  public Set<FileStatus> getFileStatuses() {
    return Sets.newHashSet(this.fileStatus);
  }

}
