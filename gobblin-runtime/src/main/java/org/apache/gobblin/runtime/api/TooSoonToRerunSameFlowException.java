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

package org.apache.gobblin.runtime.api;

import lombok.Getter;


/**
 * An {@link RuntimeException} thrown when lease cannot be acquired on provided entity.
 */
public class TooSoonToRerunSameFlowException extends RuntimeException {
  @Getter
  private final FlowSpec flowSpec;

  /**
   * Account for unwrapping within @{link FlowCatalog#updateOrAddSpecHelper}`s `CallbackResult` error handling for `SpecCatalogListener`s
   * @return `TooSoonToRerunSameFlowException` wrapped in another `TooSoonToRerunSameFlowException
   */
  public static TooSoonToRerunSameFlowException wrappedOnce(FlowSpec flowSpec) {
    return new TooSoonToRerunSameFlowException(flowSpec, new TooSoonToRerunSameFlowException(flowSpec));
  }

  public TooSoonToRerunSameFlowException(FlowSpec flowSpec) {
    super("Lease already occupied by another recent execution of this flow: " + flowSpec);
    this.flowSpec = flowSpec;
  }

  /** restricted-access ctor: use {@link #wrappedOnce(FlowSpec)} instead */
  private TooSoonToRerunSameFlowException(FlowSpec flowSpec, Throwable cause) {
    super("Lease already occupied by another recent execution of this flow: " + flowSpec, cause);
    this.flowSpec = flowSpec;
  }
}
