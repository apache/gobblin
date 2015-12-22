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

package gobblin.data.management.copy;

import lombok.Getter;

import org.apache.hadoop.fs.Path;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;


/**
 * Context that can hold global objects required in a single copy job.
 */
public class CopyContext {

  /**
   * Cache for {@link OwnerAndPermission} for various paths in {@link org.apache.hadoop.fs.FileSystem}s. Used to reduce
   * the number of calls to {@link org.apache.hadoop.fs.FileSystem#getFileStatus} when replicating attributes. Keys
   * should be fully qualified paths in case multiple {@link org.apache.hadoop.fs.FileSystem}s are in use.
   */
  @Getter
  private final Cache<Path, OwnerAndPermission> ownerAndPermissionCache;

  public CopyContext() {
    this.ownerAndPermissionCache = CacheBuilder.newBuilder().build();
  }

}
