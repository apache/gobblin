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
package gobblin.runtime.std;

import java.net.URI;

import org.slf4j.Logger;

import com.google.common.base.Optional;

import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.MutableJobCatalog;

/**
 * Implements a write-through cache of a decorated JobCatalog
 */
public class MutableCachingJobCatalog extends CachingJobCatalog implements MutableJobCatalog {

  public MutableCachingJobCatalog(MutableJobCatalog fallback, Optional<Logger> log) {
    super(fallback, log);
  }

  /** {@inheritDoc} */
  @Override
  public void put(JobSpec jobSpec) {
    ((MutableJobCatalog)_fallback).put(jobSpec);
  }

  /** {@inheritDoc} */
  @Override
  public void remove(URI uri) {
    ((MutableJobCatalog)_fallback).remove(uri);
  }

}
