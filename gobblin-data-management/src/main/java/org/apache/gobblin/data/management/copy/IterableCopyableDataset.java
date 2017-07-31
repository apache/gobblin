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
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;

import org.apache.gobblin.data.management.partition.FileSet;


/**
 * A {@link CopyableDatasetBase} that returns {@link CopyEntity}s as an iterator. It allows for scanning for files to
 * copy only when necessary. Reduces unnecessary work when the queue of {@link CopyEntity}s is full.
 */
public interface IterableCopyableDataset extends CopyableDatasetBase {

  /**
   * Get an iterator of {@link FileSet}s of {@link CopyEntity}, each one representing a group of files to copy and
   * associated actions.
   * @param targetFs target {@link org.apache.hadoop.fs.FileSystem} where copied files will be placed.
   * @param configuration {@link org.apache.gobblin.data.management.copy.CopyConfiguration} for this job. See {@link org.apache.gobblin.data.management.copy.CopyConfiguration}.
   * @throws IOException
   */
  public Iterator<FileSet<CopyEntity>> getFileSetIterator(FileSystem targetFs, CopyConfiguration configuration)
      throws IOException;

}
