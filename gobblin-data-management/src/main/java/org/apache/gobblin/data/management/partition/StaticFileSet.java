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

package org.apache.gobblin.data.management.partition;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.dataset.Dataset;


/**
 * A non-lazy {@link FileSet} where the copy entities are a static, eagerly computed list of {@link CopyEntity}s.
 * @param <T>
 */
public class StaticFileSet<T extends CopyEntity> extends FileSet<T> {

  private final List<T> files;

  public StaticFileSet(String name, Dataset dataset, List<T> files) {
    super(name, dataset);
    this.files = files;
  }

  @Override
  protected Collection<T> generateCopyEntities()
      throws IOException {
    return this.files;
  }
}
