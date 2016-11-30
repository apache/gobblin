/*
 * Copyright (C) 2016-2018 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.compaction.conditions;

import java.util.LinkedList;
import java.util.List;

import gobblin.compaction.dataset.DatasetHelper;

/**
 * An implementation {@link RecompactionCondition} which contains multiple recompact conditions.
 * As long as one of those sub conditions returns true, this condition will return true
 */

public class OrRecompactionCondition implements RecompactionCondition {

  private final List<RecompactionCondition> recompactionConditions;

  public OrRecompactionCondition() {
    recompactionConditions = new LinkedList<>();
  }

  public OrRecompactionCondition addCondition (RecompactionCondition c) {
    recompactionConditions.add(c);
    return this;
  }

  public boolean isRecompactionNeeded (DatasetHelper metric) {
    for (RecompactionCondition c : recompactionConditions) {
      if (c.isRecompactionNeeded(metric)) {
        return true;
      }
    }
    return false;
  }
}
