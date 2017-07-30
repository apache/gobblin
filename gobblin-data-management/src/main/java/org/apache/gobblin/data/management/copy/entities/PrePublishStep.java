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

package gobblin.data.management.copy.entities;

import java.util.Map;

import gobblin.commit.CommitStep;


/**
 * A {@link CommitStepCopyEntity} whose step will be executed before publishing files.
 * The priority sets an order among {@link PrePublishStep} in which they will be executed.
 */
public class PrePublishStep extends CommitStepCopyEntity {

  public PrePublishStep(String fileSet, Map<String, String> additionalMetadata, CommitStep step, int priority) {
    super(fileSet, additionalMetadata, step, priority);
  }

  @Override
  public String explain() {
    return String.format("Pre publish step with priority %s: %s", this.getPriority(), getStep().toString());
  }
}
