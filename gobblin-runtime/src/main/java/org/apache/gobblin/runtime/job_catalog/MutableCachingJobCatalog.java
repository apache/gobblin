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
package org.apache.gobblin.runtime.job_catalog;

import java.net.URI;

import org.slf4j.Logger;

import com.google.common.base.Optional;

import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.MutableJobCatalog;

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
