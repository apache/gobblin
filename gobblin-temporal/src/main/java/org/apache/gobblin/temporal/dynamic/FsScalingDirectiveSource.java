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

import com.google.common.base.Charsets;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

@Slf4j
public class FsScalingDirectiveSource implements ScalingDirectiveSource {
  private final FileSystem fileSystem;
  private final Path dirPath;
  private final Optional<Path> optErrorsPath;
  private final ScalingDirectiveParser parser = new ScalingDirectiveParser();

  public FsScalingDirectiveSource(FileSystem fileSystem, String directivesDirPath, Optional<String> optErrorDirPath) {
    this.fileSystem = fileSystem;
    this.dirPath = new Path(directivesDirPath);
    this.optErrorsPath = optErrorDirPath.map(Path::new);
  }

  // TODO: describe purpose of constraint (to preclude late insertion/edits of the directives stream) -
  // verify and only return directives whose stated (in filename) timestamp order matches `FileStatus` modtime order
  @Override
  public List<ScalingDirective> getScalingDirectives() throws IOException {
    List<Map.Entry<ScalingDirective, FileStatus>> directiveWithFileStatus = new ArrayList<>();
    for (FileStatus fileStatus : fileSystem.listStatus(dirPath)) {
      if (!fileStatus.isFile()) {
        log.warn("Ignoring non-file object: " + fileStatus);
        optAcknowledgeError(fileStatus, "non-file (not an actual)");
      } else {
        String fileName = fileStatus.getPath().getName();
        try {
          try {
            directiveWithFileStatus.add(new ImmutablePair<>(parseScalingDirective(fileName), fileStatus));
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

    // verify and only return directives whose ordering of stated (in filename) timestamp matches `FileStatus` modtime order
    List<ScalingDirective> directives = new ArrayList<>();
    // NOTE: for deterministic total-ordering, sort by path, rather than by timestamp, in case of modtime tie (given only secs granularity)
    directiveWithFileStatus.sort(Comparator.comparing(p -> p.getValue().getPath()));
    long latestValidModTime = -1;
    for (Map.Entry<ScalingDirective, FileStatus> entry : directiveWithFileStatus) {
      long thisModTime = entry.getValue().getModificationTime();
      if (thisModTime < latestValidModTime) {  // do NOT reject equal (non-increasing) modtime, given granularity of epoch seconds
        log.warn("Ignoring out-of-order scaling directive " + entry.getKey() + " since FS modTime " + thisModTime + " precedes last observed "
            + latestValidModTime + ": " + entry.getValue());
        optAcknowledgeError(entry.getValue(), "out-of-order");
      } else {
        directives.add(entry.getKey());
        latestValidModTime = thisModTime;
      }
    }
    return directives;
  }

  // ack error by moving the bad/non-directive to a separate errors dir
  protected void optAcknowledgeError(FileStatus fileStatus, String desc) {
    this.optErrorsPath.ifPresent(errorsPath ->
        moveToErrors(fileStatus, errorsPath, desc)
    );
  }

  // move broken/ignored directives into a separate directory, as an observability-enhancing ack of its rejection
  protected void moveToErrors(FileStatus badDirectiveStatus, Path errorsPath, String desc) {
    Path badDirectivePath = badDirectiveStatus.getPath();
    try {
      if (!this.fileSystem.rename(badDirectivePath, new Path(errorsPath, badDirectivePath.getName()))) {
        throw new RuntimeException(); // unclear how to obtain more info about such a failure
      }
    } catch (IOException e) {
      log.warn("Failed to move " + desc + " directive {{" + badDirectiveStatus.getPath() + "}} to '" + errorsPath + "'... leaving in place", e);
    } catch (RuntimeException e) {
      log.warn("Failed to move " + desc + " directive {{" + badDirectiveStatus.getPath() + "}} to '" + errorsPath + "' [unknown reason]... leaving in place");
    }
  }

  private ScalingDirective parseScalingDirective(String fileName)
      throws ScalingDirectiveParser.InvalidSyntaxException, ScalingDirectiveParser.OverlayPlaceholderNeedsDefinition {
    return parser.parse(fileName);
  }

  protected String slurpFileAsString(Path path) throws IOException {
    try (InputStream is = this.fileSystem.open(path)) {
      return IOUtils.toString(is, Charsets.UTF_8);
    }
  }
}
