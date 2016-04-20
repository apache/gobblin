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

package gobblin.util.binpacking;

import gobblin.source.workunit.WorkUnit;
import gobblin.source.workunit.WorkUnitWeighter;

import lombok.AllArgsConstructor;


/**
 * A {@link WorkUnitWeighter} implementation that parses the weight from a field in the work unit.
 */
@AllArgsConstructor
public class FieldWeighter implements WorkUnitWeighter {

  private final String field;

  @Override
  public long weight(WorkUnit workUnit) {
    return workUnit.getPropAsLong(this.field);
  }
}
