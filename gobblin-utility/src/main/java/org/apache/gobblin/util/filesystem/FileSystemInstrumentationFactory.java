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

package org.apache.gobblin.util.filesystem;

import org.apache.hadoop.fs.FileSystem;

import org.apache.gobblin.broker.iface.ConfigView;
import org.apache.gobblin.broker.iface.ScopeType;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;


/**
 * A factory that instruments {@link FileSystem}. Instrumentations are usually decorators of the underlying
 * {@link FileSystem} that add some additional features to it. Implementations can extend {@link FileSystemInstrumentation}
 * for convenience.
 */
public class FileSystemInstrumentationFactory<S extends ScopeType<S>> {

  /**
   * Return an instrumented version of the input {@link FileSystem}. Generally, this will return a decorator for the
   * input {@link FileSystem}. If the instrumentation will be a no-op (due to, for example, configuration), it is
   * recommended to return the input {@link FileSystem} directly for performance.
   */
  public FileSystem instrumentFileSystem(FileSystem fs, SharedResourcesBroker<S> broker, ConfigView<S, FileSystemKey> config) {
    return fs;
  }

}
