/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util.filesystem;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.fs.FileSystem;

import com.google.common.collect.Lists;

import gobblin.configuration.State;
import gobblin.util.Decorator;
import gobblin.util.DecoratorUtils;
import gobblin.util.RateControlledFileSystem;
import gobblin.util.deprecation.DeprecationUtils;


/**
 * Utilities for {@link FileSystem}s.
 */
public class FileSystemUtils {
  public static final String MAX_FILESYSTEM_QPS = "filesystem.throttling.max.filesystem.qps";
  private static final List<String> DEPRECATED_KEYS = Lists.newArrayList("gobblin.copy.max.filesystem.qps");

  /**
   * Calls {@link #getOptionallyThrottledFileSystem(FileSystem, int)} parsing the qps from the input {@link State}
   * at key {@link #MAX_FILESYSTEM_QPS}.
   * @throws IOException
   */
  public static FileSystem getOptionallyThrottledFileSystem(FileSystem fs, State state) throws IOException {
    DeprecationUtils.handleDeprecatedOptions(state, MAX_FILESYSTEM_QPS, DEPRECATED_KEYS);

    if (state.contains(MAX_FILESYSTEM_QPS)) {
      return getOptionallyThrottledFileSystem(fs, state.getPropAsInt(MAX_FILESYSTEM_QPS));
    } else {
      return fs;
    }
  }

  /**
   * Get a throttled {@link FileSystem} that limits the number of queries per second to a {@link FileSystem}. If
   * the input qps is <= 0, no such throttling will be performed.
   * @throws IOException
   */
  public static FileSystem getOptionallyThrottledFileSystem(FileSystem fs, int qpsLimit) throws IOException {
    if (fs instanceof Decorator) {
      for (Object obj : DecoratorUtils.getDecoratorLineage(fs)) {
        if (obj instanceof RateControlledFileSystem) {
          // Already rate controlled
          return fs;
        }
      }
    }

    if (qpsLimit > 0) {
      try {
        RateControlledFileSystem newFS = new RateControlledFileSystem(fs, qpsLimit);
        newFS.startRateControl();
        return newFS;
      } catch (ExecutionException ee) {
        throw new IOException("Could not create throttled FileSystem.", ee);
      }
    }
    return fs;
  }
}
