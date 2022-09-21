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

package org.apache.gobblin.data.management.copy.iceberg;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.fs.FileSystem;

import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.partition.FileSet;


/**
 * A {@link FileSet} for Iceberg datasets containing information associated with an Iceberg table and generates {@link CopyEntity}
 */
public class IcebergTableFileSet extends FileSet<CopyEntity> {

  private final CopyConfiguration copyConfiguration;
  private final FileSystem targetFs;
  private final IcebergDataset icebergDataset;

  public IcebergTableFileSet(String name, IcebergDataset icebergDataset, FileSystem targetFs, CopyConfiguration configuration) {
    super(name, icebergDataset);
    this.copyConfiguration = configuration;
    this.targetFs = targetFs;
    this.icebergDataset = icebergDataset;
  }

  @Override
  protected Collection<CopyEntity> generateCopyEntities() throws IOException {
    return this.icebergDataset.generateCopyEntities(this.targetFs, this.copyConfiguration);
  }
}
