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

package gobblin.data.management.copy.entities;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Map;

import gobblin.commit.CommitStep;
import gobblin.data.management.copy.CopyEntity;


/**
 * A {@link CopyEntity} encapsulating a {@link CommitStep}.
 */
@EqualsAndHashCode(callSuper = true)
public class CommitStepCopyEntity extends CopyEntity {

  @Getter
  private final CommitStep step;
  /** A priority value that can be used for sorting {@link CommitStepCopyEntity}s. Lower values are higher priority.*/
  @Getter
  private final int priority;

  public CommitStepCopyEntity(String fileSet, Map<String, String> additionalMetadata, CommitStep step, int priority) {
    super(fileSet, additionalMetadata);
    this.step = step;
    this.priority = priority;
  }

  @Override
  public String explain() {
    return this.step.toString();
  }
}
