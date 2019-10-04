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

package org.apache.gobblin.state;

import java.io.IOException;
import java.util.List;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.dataset.Dataset;


/**
 * Finds states and return them as {@link Dataset} objects.
 *
 * <p>
 *   Concrete subclasses should have a constructor with signature
 *   ({@link org.apache.hadoop.fs.FileSystem}, {@link java.util.Properties}).
 * </p>
 */
public interface StatesFinder<T extends State> {

  /**
   * Find all {@link Dataset}s in the file system.
   * @return List of {@link Dataset}s in the file system.
   * @throws IOException
   */
  List<? extends Dataset> findStates() throws IOException;
}
