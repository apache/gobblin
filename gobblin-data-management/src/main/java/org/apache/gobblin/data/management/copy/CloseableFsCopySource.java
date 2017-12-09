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

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.copy.extractor.CloseableFsFileAwareInputStreamExtractor;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.extract.sftp.SftpLightWeightFileSystem;
import org.apache.gobblin.util.HadoopUtils;

import java.io.IOException;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;

import com.google.common.io.Closer;


/**
 * Used instead of {@link CopySource} for {@link FileSystem}s that need be closed after use E.g
 * {@link SftpLightWeightFileSystem}.
 * <p>
 * Note that all {@link FileSystem} implementations should not be closed as Hadoop's
 * {@link FileSystem#get(org.apache.hadoop.conf.Configuration)} API returns a cached copy of {@link FileSystem} by
 * default. The same {@link FileSystem} instance may be used by other classes in the same JVM. Closing a cached
 * {@link FileSystem} may cause {@link IOException} at other parts of the code using the same instance.
 * </p>
 * <p>
 * For {@link SftpLightWeightFileSystem} a new instance is returned on every
 * {@link FileSystem#get(org.apache.hadoop.conf.Configuration)} call. Closing is necessary as the file system maintains
 * a session with the remote server.
 *
 * @see HadoopUtils#newConfiguration()
 * @See SftpLightWeightFileSystem
 *      </p>
 */
@Slf4j
public class CloseableFsCopySource extends CopySource {

  private final Closer closer = Closer.create();

  @Override
  protected FileSystem getSourceFileSystem(State state)
      throws IOException {
    return this.closer.register(HadoopUtils.getSourceFileSystem(state));
  }

  @Override
  public void shutdown(SourceState state) {
    try {
      this.closer.close();
    } catch (IOException e) {
      log.warn("Failed to close all closeables", e);
    }
  }

  @Override
  protected Extractor<String, FileAwareInputStream> extractorForCopyableFile(FileSystem fs, CopyableFile cf,
      WorkUnitState state)
      throws IOException {
    return new CloseableFsFileAwareInputStreamExtractor(fs, cf, state);
  }
}
