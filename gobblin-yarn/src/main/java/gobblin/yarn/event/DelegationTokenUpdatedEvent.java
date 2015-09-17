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

package gobblin.yarn.event;


/**
 * A type of events fired when the delegation token has been updated by the controller.
 *
 * @author ynli
 */
public class DelegationTokenUpdatedEvent {

  private final String tokenFilePath;

  public DelegationTokenUpdatedEvent(String tokenFilePath) {
    this.tokenFilePath = tokenFilePath;
  }

  /**
   * Get the token file path.
   *
   * @return the token file path.
   */
  public String getTokenFilePath() {
    return this.tokenFilePath;
  }
}
