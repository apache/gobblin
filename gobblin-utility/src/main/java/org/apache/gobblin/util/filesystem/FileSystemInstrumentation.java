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

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;


/**
 * A base class for {@link FileSystem} decorators used for {@link FileSystemInstrumentationFactory}s.
 */
public class FileSystemInstrumentation extends FileSystemDecorator {

  protected boolean closed = false;

  public FileSystemInstrumentation(FileSystem underlying) {
    super(underlying.getScheme(), underlying.getScheme());
    this.underlyingFs = underlying;
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        if (!FileSystemInstrumentation.this.closed) {
          onClose();
        }
      }
    });
  }

  @Override
  public synchronized void close() throws IOException {
    if (!this.closed) {
      onClose();
      this.closed = true;
    }
    super.close();
  }

  /**
   * A method called when the {@link FileSystem} is being closed or when the JVM is shutting down.
   * Useful for writing out information about the instrumentation.
   */
  protected void onClose() {

  }

}
