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

import java.io.IOException;
import java.util.Map;

import gobblin.commit.CommitStep;


/**
 * A {@link CommitStepCopyEntity} whose step will be executed after publishing files.
 * The priority sets an order among {@link PostPublishStep} in which they will be executed.
 */
public class PostPublishStep extends CommitStepCopyEntity {

  public PostPublishStep(String fileSet, Map<String, Object> additionalMetadata, CommitStep step, int priority, String stepKey)
      throws IOException {
    super(fileSet, additionalMetadata, step, priority, stepKey);
  }

  @Override
  public String explain() throws IOException {
    return String.format("Post publish step with priority %s: %s", this.getPriority(),
        getStep().toString());
  }
}
