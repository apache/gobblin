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

package gobblin.util.filters;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;


/**
 * Combines multiple {@link PathFilter}s. {@link Path} is accepted only if all filters accept it.
 */
public class AndPathFilter implements PathFilter {

  public AndPathFilter(PathFilter... pathFilters) {
    this.pathFilters = pathFilters;
  }

  PathFilter[] pathFilters;

  @Override
  public boolean accept(Path path) {
    for (PathFilter filter : this.pathFilters) {
      if (!filter.accept(path)) {
        return false;
      }
    }
    return true;
  }
}
