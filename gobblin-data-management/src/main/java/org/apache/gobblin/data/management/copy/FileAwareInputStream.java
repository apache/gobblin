/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.data.management.copy;

import java.io.InputStream;

import lombok.Builder;
import lombok.NonNull;
import lombok.Getter;

import com.google.common.base.Optional;

import org.apache.gobblin.data.management.copy.splitter.DistcpFileSplitter;


/**
 * A wrapper to {@link InputStream} that represents an entity to be copied. The enclosed {@link CopyableFile} instance
 * contains file Metadata like permission, destination path etc. required by the writers and converters.
 * The enclosed {@link DistcpFileSplitter.Split} object indicates whether the {@link InputStream} to be copied is a
 * block of the {@link CopyableFile} or not. If it is present, the {@link InputStream} should already be at the start
 * position of the specified split/block.
 */
@Getter
public class FileAwareInputStream {

  private CopyableFile file;
  private InputStream inputStream;
  private Optional<DistcpFileSplitter.Split> split = Optional.absent();

  @Builder(toBuilder = true)
  public FileAwareInputStream(@NonNull CopyableFile file, @NonNull InputStream inputStream,
      Optional<DistcpFileSplitter.Split> split) {
    this.file = file;
    this.inputStream = inputStream;
    this.split = split == null ? Optional.<DistcpFileSplitter.Split>absent() : split;
  }

  @Override
  public String toString() {
    return this.file.toString();
  }
}
