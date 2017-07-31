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

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.ImmutableList;

import org.apache.gobblin.util.PathUtils;


/**
 * A {@link CopyableFileFilter} that drops a {@link CopyableFile} if another file with "filename.ready" is not found on
 * the <code>sourceFs<code>
 */
@Slf4j
public class ReadyCopyableFileFilter implements CopyableFileFilter {

  public static final String READY_EXTENSION = ".ready";

  /**
   * For every {@link CopyableFile} in <code>copyableFiles</code> checks if a {@link CopyableFile#getOrigin()#getPath()}
   * + .ready files is present on <code>sourceFs</code> {@inheritDoc}
   *
   * @see org.apache.gobblin.data.management.copy.CopyableFileFilter#filter(org.apache.hadoop.fs.FileSystem,
   *      org.apache.hadoop.fs.FileSystem, java.util.Collection)
   */
  @Override
  public Collection<CopyableFile> filter(FileSystem sourceFs, FileSystem targetFs,
      Collection<CopyableFile> copyableFiles) {
    Iterator<CopyableFile> cfIterator = copyableFiles.iterator();

    ImmutableList.Builder<CopyableFile> filtered = ImmutableList.builder();

    while (cfIterator.hasNext()) {
      CopyableFile cf = cfIterator.next();
      Path readyFilePath = PathUtils.addExtension(cf.getOrigin().getPath(), READY_EXTENSION);
      try {
        if (sourceFs.exists(readyFilePath)) {
          filtered.add(cf);
        } else {
          log.info(String.format("Removing %s as the .ready file is not found", cf.getOrigin().getPath()));
        }
      } catch (IOException e) {
        log.warn(String.format("Removing %s as the .ready file can not be read. Exception %s",
            cf.getOrigin().getPath(), e.getMessage()));
      }
    }
    return filtered.build();
  }
}
