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

package org.apache.gobblin.temporal.dynamic;

import java.io.IOException;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * A {@link ScalingDirectivesRecipient} that writes {@link ScalingDirective}s to a {@link FileSystem} directory, where each directive is the name
 * of a single file inside the directory.
 *
 * TODO: per {@link FsScalingDirectiveSource} - directives too long for one filename path component MUST (but currently do NOT!) use the
 * {@link ScalingDirectiveParser#OVERLAY_DEFINITION_PLACEHOLDER} syntax and write their {@link ProfileDerivation} overlay as the file's data/content.
 *
 * Within-length scaling directives are no-data, zero-length files.  When backed by HDFS, writing such zero-length scaling directive filenames is a
 * NameNode-only operation, with their metadata-only nature conserving NN object count/quota.
 *
 * @see FsScalingDirectiveSource
 */
@Slf4j
public class FsScalingDirectivesRecipient implements ScalingDirectivesRecipient {
  private final FileSystem fileSystem;
  private final Path dirPath;

  /** Write to `directivesDirPath` of `fileSystem` */
  public FsScalingDirectivesRecipient(FileSystem fileSystem, Path directivesDirPath) throws IOException {
    this.fileSystem = fileSystem;
    this.dirPath = directivesDirPath;
    this.fileSystem.mkdirs(this.dirPath);
  }

  /** Write to `directivesDirPath` of `fileSystem` */
  public FsScalingDirectivesRecipient(FileSystem fileSystem, String directivesDirPath) throws IOException {
    this(fileSystem, new Path(directivesDirPath));
  }

  @Override
  public void receive(List<ScalingDirective> directives) throws IOException {
    for (ScalingDirective directive : directives) {
      String directiveAsString = ScalingDirectiveParser.asString(directive);
      // TODO: handle directivePaths in excess of length limit
      Path directivePath = new Path(dirPath, directiveAsString);
      log.info("Adding ScalingDirective: {} at '{}' - {}", directiveAsString, directivePath, directive);
      fileSystem.create(directivePath, false).close();
    }
  }
}
