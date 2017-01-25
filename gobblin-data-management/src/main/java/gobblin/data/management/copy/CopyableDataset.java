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

package gobblin.data.management.copy;

import gobblin.dataset.Dataset;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.fs.FileSystem;


/**
 * {@link Dataset} that supports finding {@link CopyEntity}s.
 */
public interface CopyableDataset extends CopyableDatasetBase {

  /**
   * Find all {@link CopyEntity}s in this dataset.
   *
   * <p>
   *   This method should return a collection of {@link CopyEntity}, each describing one work unit for distcp.
   *   The most common {@link CopyEntity} is the {@link gobblin.data.management.copy.CopyableDataset}, describing a file
   *   that should be copied
   *   to the target.
   *   See {@link CopyableFile} for explanation of the information contained in the {@link CopyableFile}s.
   * </p>
   *
   * @param targetFs target {@link org.apache.hadoop.fs.FileSystem} where copied files will be placed.
   * @param configuration {@link gobblin.data.management.copy.CopyConfiguration} for this job. See {@link gobblin.data.management.copy.CopyConfiguration}.
   * @return List of {@link CopyEntity}s in this dataset.
   * @throws IOException
   */
  public Collection<? extends CopyEntity> getCopyableFiles(FileSystem targetFs, CopyConfiguration configuration) throws
      IOException;

}
