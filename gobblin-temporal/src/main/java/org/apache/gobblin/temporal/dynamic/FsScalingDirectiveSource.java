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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.base.Charsets;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * A {@link ScalingDirectiveSource} that reads {@link ScalingDirective}s from a {@link FileSystem} directory, where each directive is the name
 * of a single file inside the directory.  Directives too long for one filename path component MUST use the
 * {@link ScalingDirectiveParser#OVERLAY_DEFINITION_PLACEHOLDER} syntax and write their {@link ProfileDerivation} overlay as the file's data/content.
 * Within-length scaling directives are no-data, zero-length files.  When backed by HDFS, reading such zero-length scaling directive filenames is a
 * NameNode-only operation, with their metadata-only nature conserving NN object count/quota.
 */
@Slf4j
public class FsScalingDirectiveSource implements ScalingDirectiveSource {
  private final FileSystem fileSystem;
  private final Path dirPath;
  private final Optional<Path> optErrorsPath;
  private final ScalingDirectiveParser parser = new ScalingDirectiveParser();

  /** Read from `directivesDirPath` of `fileSystem`, and optionally move invalid/rejected directives to `optErrorsDirPath` */
  public FsScalingDirectiveSource(FileSystem fileSystem, String directivesDirPath, Optional<String> optErrorsDirPath) {
    this.fileSystem = fileSystem;
    this.dirPath = new Path(directivesDirPath);
    this.optErrorsPath = optErrorsDirPath.map(Path::new);
  }

  /**
   * @return all valid (parseable, in-order) scaling directives currently in the directory, ordered by ascending modtime
   *
   * Ignore invalid directives, and, when `optErrorsDirPath` was provided to the ctor, acknowledge each by moving it to a separate "errors" directory.
   * Regardless, always swallow {@link ScalingDirectiveParser.InvalidSyntaxException}.
   *
   * Like un-parseable directives, also invalid are out-of-order directives.  This blocks late/out-of-order insertion and/or edits to the directives
   * stream.  Each directive contains its own {@link ScalingDirective#getTimestampEpochMillis()} stated in its filename.  Later-modtime directives are
   * rejected when directive-timestamp-order does not match {@link FileStatus} modtime order.  In the case of a modtime tie, the directive with the
   * alphabetically-later filename is rejected.
   *
   * ATTENTION: This returns ALL known directives, even those already returned by a prior invocation.  When the underlying directory is unchanged
   * before the next invocation, the result will be equal elements in the same order.
   *
   * @throws IOException when unable to read the directory (or file data, in the case of an overlay definition placeholder)
   */
  @Override
  public List<ScalingDirective> getScalingDirectives() throws IOException {
    List<Map.Entry<ScalingDirective, FileStatus>> directiveWithFileStatus = new ArrayList<>();
    // TODO: add caching by dir modtime to avoid re-listing the same, unchanged contents, while also avoiding repetitive parsing
    // to begin, just parse w/o worrying about ordering... that comes next
    for (FileStatus fileStatus : fileSystem.listStatus(dirPath)) {
      if (!fileStatus.isFile()) {
        log.warn("Ignoring non-file object: " + fileStatus);
        optAcknowledgeError(fileStatus, "non-file (not an actual)");
      } else {
        String fileName = fileStatus.getPath().getName();
        try {
          try {
            directiveWithFileStatus.add(new ImmutablePair<>(parser.parse(fileName), fileStatus));
          } catch (ScalingDirectiveParser.OverlayPlaceholderNeedsDefinition needsDefinition) {
            // directive used placeholder syntax to indicate the overlay definition resides inside its file... so open the file to load that def
            log.info("Loading overlay definition for directive {{" + fileName + "}} from: " + fileStatus);
            String overlayDef = slurpFileAsString(fileStatus.getPath());
            directiveWithFileStatus.add(new ImmutablePair<>(needsDefinition.retryParsingWithDefinition(overlayDef), fileStatus));
          }
        } catch (ScalingDirectiveParser.InvalidSyntaxException e) {
          log.warn("Ignoring unparseable scaling directive {{" + fileName + "}}: " + fileStatus + " - " + e.getClass().getName() + ": " + e.getMessage());
          optAcknowledgeError(fileStatus, "unparseable");
        }
      }
    }

    // verify ordering: only return directives whose stated timestamp ordering (of filename prefix) matches `FileStatus` modtime order
    List<ScalingDirective> directives = new ArrayList<>();
    // NOTE: for deterministic total-ordering, sort by path, rather than by timestamp, in case of modtime tie (given only millisecs granularity)
    directiveWithFileStatus.sort(Comparator.comparing(p -> p.getValue().getPath()));
    long latestValidModTime = -1;
    for (Map.Entry<ScalingDirective, FileStatus> entry : directiveWithFileStatus) {
      long thisModTime = entry.getValue().getModificationTime();
      if (thisModTime <= latestValidModTime) {  // when equal (non-increasing) modtime: reject alphabetically-later filename (path)
        log.warn("Ignoring out-of-order scaling directive " + entry.getKey() + " since FS modTime " + thisModTime + " NOT later than last observed "
            + latestValidModTime + ": " + entry.getValue());
        optAcknowledgeError(entry.getValue(), "out-of-order");
      } else {
        directives.add(entry.getKey());
        latestValidModTime = thisModTime;
      }
    }
    return directives;
  }

  /** "acknowledge" the rejection of an invalid directive by moving it to a separate "errors" dir (when `optErrorsDirPath` was given to the ctor) */
  protected void optAcknowledgeError(FileStatus invalidDirectiveFileStatus, String desc) {
    this.optErrorsPath.ifPresent(errorsPath ->
        moveDirectiveToDir(invalidDirectiveFileStatus, errorsPath, desc)
    );
  }

  /**
   * move `invalidDirectiveFileStatus` to a designated `destDirPath`, with the reason for moving (e.g. the error) described in `desc`.
   * This is used to promote observability by acknowledging invalid, rejected directives
   */
  protected void moveDirectiveToDir(FileStatus invalidDirectiveFileStatus, Path destDirPath, String desc) {
    Path invalidDirectivePath = invalidDirectiveFileStatus.getPath();
    try {
      if (!this.fileSystem.rename(invalidDirectivePath, new Path(destDirPath, invalidDirectivePath.getName()))) {
        throw new RuntimeException(); // unclear how to obtain more info about such a failure
      }
    } catch (IOException e) {
      log.warn("Failed to move " + desc + " directive {{" + invalidDirectiveFileStatus.getPath() + "}} to '" + destDirPath + "'... leaving in place", e);
    } catch (RuntimeException e) {
      log.warn("Failed to move " + desc + " directive {{" + invalidDirectiveFileStatus.getPath() + "}} to '" + destDirPath
          + "' [unknown reason]... leaving in place", e);
    }
  }

  /** @return all contents of `path` as a single (UTF-8) `String` */
  protected String slurpFileAsString(Path path) throws IOException {
    try (InputStream is = this.fileSystem.open(path)) {
      return IOUtils.toString(is, Charsets.UTF_8);
    }
  }
}
