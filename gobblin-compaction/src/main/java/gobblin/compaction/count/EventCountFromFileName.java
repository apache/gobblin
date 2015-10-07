/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.compaction.count;

import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import lombok.extern.slf4j.Slf4j;

/**
 * Implementation of {@link HdfsEventCountProvider} gets event count from a file name. File name should have the count in the past part, spearated by ".".
 * Files written using {@link gobblin.writer.AvroHdfsTimePartitionedWithRecordCountsWriter} will follow the pattern.
 * E.g., part.1.123.avro, the event count will be 123.
 */
@Slf4j
public class EventCountFromFileName implements HdfsEventCountProvider {
  @Override
  public long getEventCount(FileSystem fs, List<Path> paths) {
    long eventCount = 0;
    try {
      for (Path path : paths) {
          String[] parts = FilenameUtils.removeExtension(path.getName()).split("\\.");
          eventCount += Long.parseLong(parts[parts.length - 1]);

      }
    } catch (Exception e) {
      log.error("Failed to retrieve count from file name");
    }
    return eventCount;
  }
}
