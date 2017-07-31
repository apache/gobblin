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

import org.apache.gobblin.data.management.retention.profile.ConfigurableGlobDatasetFinder;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * {@link org.apache.gobblin.data.management.retention.profile.ConfigurableGlobDatasetFinder} that returns datasets of type
 * {@link org.apache.gobblin.data.management.copy.RecursiveCopyableDataset}.N
 */
public class CopyableGlobDatasetFinder extends ConfigurableGlobDatasetFinder<CopyableDataset> {

  public CopyableGlobDatasetFinder(FileSystem fs, Properties props) throws IOException {
    super(fs, props);
  }

  @Override
  public CopyableDataset datasetAtPath(Path path) throws IOException {
    return new RecursiveCopyableDataset(this.fs, path, this.props, this.datasetPattern);
  }
}
