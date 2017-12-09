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
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;

import static org.apache.gobblin.util.filesystem.InstrumentedFileSystemUtils.*;


/**
 * A base {@link org.apache.hadoop.fs.FileSystem} that uses {@link FileSystemFactory} to create an underlying
 * {@link org.apache.hadoop.fs.FileSystem} with available instrumentations. (see also {@link FileSystemInstrumentation}).
 */
public class InstrumentedFileSystem extends FileSystemDecorator {

  public InstrumentedFileSystem(String scheme, FileSystem underlyingFileSystem) {
    super(scheme, underlyingFileSystem.getScheme());
    this.underlyingFs = underlyingFileSystem;
  }

  @Override
  public void initialize(URI uri, Configuration conf)
      throws IOException {
    this.replacementScheme = uri.getScheme();

    Configuration actualConfiguration = new Configuration(conf);
    String key = "fs." + this.underlyingScheme + ".impl";
    actualConfiguration.set(key, this.underlyingFs.getClass().getName());

    this.underlyingFs = FileSystemFactory.get(replaceScheme(uri, this.replacementScheme, this.underlyingScheme), actualConfiguration,
        SharedResourcesBrokerFactory.getImplicitBroker());
  }
}
