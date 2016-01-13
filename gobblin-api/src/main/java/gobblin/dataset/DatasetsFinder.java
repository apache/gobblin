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

package gobblin.dataset;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;


/**
 * Finds {@link Dataset}s in the file system.
 *
 * <p>
 *   Concrete subclasses should have a constructor with signature
 *   ({@link org.apache.hadoop.fs.FileSystem}, {@link java.util.Properties}).
 * </p>
 */
public interface DatasetsFinder<T extends Dataset> {

  /**
   * Find all {@link Dataset}s in the file system.
   * @return List of {@link Dataset}s in the file system.
   * @throws IOException
   */
  public List<T> findDatasets() throws IOException;

  /**
   * @return The deepest common root shared by all {@link Dataset}s root paths returned by this finder.
   */
  public Path commonDatasetRoot();
}
