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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Sets;

import lombok.Data;


/**
 * Implementation of {@link gobblin.data.management.version.DatasetVersion} that uses a single path per
 * version and stores the {@link org.apache.hadoop.fs.FileStatus} of that path.
 */
@Data
public class FileStatusDatasetVersion extends StringDatasetVersion {

  protected final FileStatus fileStatus;

  public FileStatusDatasetVersion(FileStatus fileStatus) {
    super(fileStatus.getPath().getName(), fileStatus.getPath());
    this.fileStatus = fileStatus;
  }

  @Override
  public int compareTo(FileSystemDatasetVersion other) {
    FileStatusDatasetVersion otherAsFileStatus = (FileStatusDatasetVersion) other;
    return this.fileStatus.getPath().compareTo(otherAsFileStatus.getFileStatus().getPath());
  }

  @Override
  public boolean equals(Object obj) {
    return obj != null && this.getClass().equals(obj.getClass()) && compareTo((FileSystemDatasetVersion) obj) == 0;
  }

  @Override
  public int hashCode() {
    return this.fileStatus.hashCode();
  }

  @Override
  public Set<Path> getPaths() {
    return Sets.newHashSet(this.fileStatus.getPath());
  }
}

