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

package org.apache.gobblin.service.modules.orchestration;

import lombok.Getter;

@Getter
public abstract class AzkabanClientStatus<RS> {
  private boolean success = true;
  private String failMsg = "";
  private Throwable throwable = null;

  private RS response = null;

  public AzkabanClientStatus() {
  }

  public AzkabanClientStatus(RS response) {
    this.response = response;
  }

  public AzkabanClientStatus(String failMsg, Throwable throwable) {
    this.success = false;
    this.failMsg = failMsg;
    this.throwable = throwable;
  }

  /**
   * This status captures basic success.
   */
  public static class SUCCESS extends AzkabanClientStatus<Object> {
    public SUCCESS() {
      super();
    }
  }

  /**
   * This status captures basic failure (fail message and throwable).
   */
  public static class FAIL extends AzkabanClientStatus<Object> {
    public FAIL(String failMsg, Throwable throwable) {
      super(failMsg, throwable);
    }
  }
}
