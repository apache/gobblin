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

package org.apache.gobblin.temporal.ddm.work;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.net.URI;
import java.util.Comparator;
import java.util.Optional;
import org.apache.gobblin.configuration.State;
import org.apache.hadoop.fs.FileStatus;


/**
 * {@link AbstractEagerFsDirBackedWorkload} for {@link WorkUnitClaimCheck} `WORK_ITEM`s, which uses {@link WorkUnitClaimCheck#getWorkUnitPath()}
 * for their total-ordering.
 */
@lombok.NoArgsConstructor // IMPORTANT: for jackson (de)serialization
@lombok.ToString(callSuper = true)
public class EagerFsDirBackedWorkUnitClaimCheckWorkload extends AbstractEagerFsDirBackedWorkload<WorkUnitClaimCheck> {

  public EagerFsDirBackedWorkUnitClaimCheckWorkload(URI nameNodeUri, String hdfsDir) {
    this(nameNodeUri, hdfsDir, Optional.empty());
  }

  public EagerFsDirBackedWorkUnitClaimCheckWorkload(URI nameNodeUri, String hdfsDir, State stateConfig) {
    this(nameNodeUri, hdfsDir, Optional.of(stateConfig));
  }

  protected EagerFsDirBackedWorkUnitClaimCheckWorkload(URI nameNodeUri, String hdfsDir, Optional<State> optStateConfig) {
    super(nameNodeUri, hdfsDir);
    optStateConfig.ifPresent(this::setStateConfig);
  }

  @Override
  protected WorkUnitClaimCheck fromFileStatus(FileStatus fileStatus) {
    // begin by setting all correlators to empty
    return new WorkUnitClaimCheck("", this.getNameNodeUri(), fileStatus.getPath().toString(), this.getStateConfig());
  }

  @Override
  @JsonIgnore // (because no-arg method resembles 'java bean property')
  protected Comparator<WorkUnitClaimCheck> getWorkItemComparator() {
    return Comparator.comparing(WorkUnitClaimCheck::getWorkUnitPath);
  }

  @Override
  protected void acknowledgeOrdering(int index, WorkUnitClaimCheck item) {
    item.setCorrelator(Integer.toString(index));
  }
}
