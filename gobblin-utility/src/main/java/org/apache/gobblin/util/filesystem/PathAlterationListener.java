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

package gobblin.util.filesystem;

import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.hadoop.fs.Path;


/**
 * A listener that receives events of general file system modifications.
 * A generalized version Of FileAlterationListener interface using Path as the parameter for each method
 * @see FileAlterationListener
 */

public interface PathAlterationListener {
  void onStart(final PathAlterationObserver observer);

  void onFileCreate(final Path path);

  void onFileChange(final Path path);

  void onStop(final PathAlterationObserver observer);

  void onDirectoryCreate(final Path directory);

  void onDirectoryChange(final Path directory);

  void onDirectoryDelete(final Path directory);

  void onFileDelete(final Path path);
}
