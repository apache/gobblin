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
 * A {@link PathFilter} that filters out hidden files (those starting with '_' or '.').
 */
public class HiddenFilter implements PathFilter {

  private static final String[] HIDDEN_FILE_PREFIX = { "_", "." };

  @Override
  public boolean accept(Path path) {
    String name = path.getName();
    for (String prefix : HIDDEN_FILE_PREFIX) {
      if (name.startsWith(prefix)) {
        return false;
      }
    }
    return true;
  }

}
