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
package gobblin.data.management.copy.extractor;

import gobblin.configuration.WorkUnitState;
import gobblin.data.management.copy.CopyableFile;
import gobblin.source.extractor.extract.sftp.SftpLightWeightFileSystem;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

import com.google.common.io.Closer;


/**
 * Used instead of {@link FileAwareInputStreamExtractor} for {@link FileSystem}s that need be closed after use E.g
 * {@link SftpLightWeightFileSystem}.
 */
public class CloseableFsFileAwareInputStreamExtractor extends FileAwareInputStreamExtractor {

  private final Closer closer = Closer.create();

  public CloseableFsFileAwareInputStreamExtractor(FileSystem fs, CopyableFile file, WorkUnitState state)
      throws IOException {
    super(fs, file, state);
    this.closer.register(fs);
  }

  public CloseableFsFileAwareInputStreamExtractor(FileSystem fs, CopyableFile file)
      throws IOException {
    super(fs, file);
    this.closer.register(fs);
  }

  @Override
  public void close()
      throws IOException {
    this.closer.close();
  }
}
