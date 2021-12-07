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

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.UnsupportedFileSystemException;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;


/**
 * A Factory to create {@link FileContext}. It caches {@link FileContext} for the same {@link URI}.
 * This code is influenced by {@see org.apache.hadoop.fs.FileSystem.Cache}
 */
@Slf4j
public class FileContextFactory {

  static final Cache CACHE = new Cache();

  private FileContextFactory() {
  }

  public static FileContext getInstance(URI uri, Configuration conf) throws UnsupportedFileSystemException {
    String scheme = uri.getScheme();
    String disableCacheName = String.format("fs.%s.impl.disable.cache", scheme);
    if (conf.getBoolean(disableCacheName, false)) {
      log.debug("Bypassing cache to create FileContext {}", uri);
      return FileContext.getFileContext(uri, conf);
    } else {
      return CACHE.get(uri, conf);
    }
  }

  static class Cache {
    private final Map<Cache.Key, FileContext> map = new HashMap<>();

    Cache() {
    }

    FileContext get(URI uri, Configuration conf) throws UnsupportedFileSystemException {
      Cache.Key key = new Cache.Key(uri, conf);
      return getInternal(uri, conf, key);
    }

    private FileContext getInternal(URI uri, Configuration conf, Key key) throws UnsupportedFileSystemException {
      FileContext fc;

      synchronized (this) {
        fc = this.map.get(key);
      }

      if (fc != null) {
        return fc;
      } else {
        fc = FileContext.getFileContext(uri, conf);
        synchronized (this) {
          this.map.putIfAbsent(key, fc);
          return fc;
        }
      }
    }

    @AllArgsConstructor
    @EqualsAndHashCode(exclude = "conf")
    static class Key {
      URI uri;
      Configuration conf;
    }
  }
}
